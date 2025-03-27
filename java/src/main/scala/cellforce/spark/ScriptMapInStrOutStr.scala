package cellforce.spark

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable


case class ScriptMapInStrOutStr(lang: Expression,
                                script: Expression,
                                func: Expression,
                                column: Expression) extends  Expression with ExpectsInputTypes {


  @transient private lazy val nativeFunctionRunnerPointers =
    new ConcurrentHashMap[String, Long]()


  override def children: Seq[Expression] = Seq(lang, script, func, column)
  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  private def computeRunnerKey(lang: String, script: String, func: String): String = {
    val langKey = DigestUtils.md5Hex(lang)
    val scriptKey = DigestUtils.md5Hex(script)
    val funcKey = DigestUtils.md5Hex(func)
    s"$langKey:$scriptKey:$funcKey"
  }

  override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = {
    val langValue = lang.eval(input).asInstanceOf[UTF8String]
    val scriptValue = script.eval(input).asInstanceOf[UTF8String]
    val funcValue = func.eval(input).asInstanceOf[UTF8String]
    val columnValue = column.eval(input).asInstanceOf[UTF8String]

    if (langValue == null || scriptValue == null ||
      funcValue == null || columnValue == null) {
      return null
    }

    try {
      val trimmedLang = langValue.trim()
      val trimmedFunc = funcValue.trim()

      val runnerKey = computeRunnerKey(
        trimmedLang.toString,
        scriptValue.toString,
        trimmedFunc.toString
      )

      val native = new NativeFunctions()
      val scriptRunnerPointer = nativeFunctionRunnerPointers.computeIfAbsent(
        runnerKey,
        _ => native.newScriptRunner(
          trimmedLang.toString,
          scriptValue.toString,
          trimmedFunc.toString
        )
      )

      val result = native.scriptMapInStrOutStr(
        scriptRunnerPointer,
        columnValue.toString
      )

      if (result == null) null
      else UTF8String.fromString(result)
    } catch {
      case e: Exception =>
        // Log error
        null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val langGen = lang.genCode(ctx)
    val scriptGen = script.genCode(ctx)
    val funcGen = func.genCode(ctx)
    val columnGen = column.genCode(ctx)

    val nativeClass = classOf[NativeFunctions].getName
    val runnerMapTerm = ctx.addMutableState(
      "java.util.concurrent.ConcurrentHashMap<String, Long>",
      "runnerMap",
      v => s"$v = new java.util.concurrent.ConcurrentHashMap<>();"
    )

    val computeKeyFuncName = ctx.freshName("computeRunnerKey")
    ctx.addNewFunction(computeKeyFuncName,
      s"""
         |private String $computeKeyFuncName(String lang, String script, String func) {
         |  String langKey = org.apache.commons.codec.digest.DigestUtils.md5Hex(lang);
         |  String scriptKey = org.apache.commons.codec.digest.DigestUtils.md5Hex(script);
         |  String funcKey = org.apache.commons.codec.digest.DigestUtils.md5Hex(func);
         |  return langKey + ":" + scriptKey + ":" + funcKey;
         |}
         |""".stripMargin)

    val native = ctx.freshName("native")
    val runnerKey = ctx.freshName("runnerKey")
    val pointer = ctx.freshName("pointer")
    val resultTerm = ctx.freshName("result")
    val existingPointer = ctx.freshName("existingPointer")

    // 添加临时变量来存储转换后的String值
    val langStrTerm = ctx.freshName("langStr")
    val scriptStrTerm = ctx.freshName("scriptStr")
    val funcStrTerm = ctx.freshName("funcStr")
    val columnStrTerm = ctx.freshName("columnStr")


    ev.copy(code =
      code"""
        ${langGen.code}
        ${scriptGen.code}
        ${funcGen.code}
        ${columnGen.code}
        boolean ${ev.isNull} = true;
        UTF8String ${ev.value} = null;

        if (!${langGen.isNull} && !${scriptGen.isNull} &&
            !${funcGen.isNull} && !${columnGen.isNull}) {

          try {
            // 先将UTF8String转换为Java String
            String $langStrTerm = ${langGen.value}.toString();
            String $scriptStrTerm = ${scriptGen.value}.toString();
            String $funcStrTerm = ${funcGen.value}.toString();
            String $columnStrTerm = ${columnGen.value}.toString();

            // 进行trim操作
            $langStrTerm = $langStrTerm.trim();
            $funcStrTerm = $funcStrTerm.trim();

            String $runnerKey = $computeKeyFuncName(
              $langStrTerm,
              $scriptStrTerm,
              $funcStrTerm
            );

            $nativeClass $native = new $nativeClass();

            Long $existingPointer = (Long)$runnerMapTerm.get($runnerKey);
            long $pointer = 0L;
            if ($existingPointer == null) {
              $pointer = $native.newScriptRunner(
                $langStrTerm,
                $scriptStrTerm,
                $funcStrTerm
              );
              $runnerMapTerm.put($runnerKey, $pointer);
            } else {
              $pointer = $existingPointer.longValue();
            }

            String $resultTerm = $native.scriptMapInStrOutStr(
              $pointer,
              $columnStrTerm
            );

            if ($resultTerm != null) {
              ${ev.value} = UTF8String.fromString($resultTerm);
              ${ev.isNull} = false;
            }
          } catch (Exception e) {
            ${ev.isNull} = true;
          }
        }
      """)
  }




  override def prettyName: String = "script_map_in_str_out_str"

//  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    ScriptMapInStrOutStr(newChildren(0), newChildren(1), newChildren(2), newChildren(3))
  }

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType, StringType)
}


object ScriptMapInStrOutStr {
  def apply(children: Seq[Expression]): Expression = {
    new ScriptMapInStrOutStr(
      children.head,
      children.apply(1),
      children.apply(2),
      children.apply(3),
    )
  }
}
