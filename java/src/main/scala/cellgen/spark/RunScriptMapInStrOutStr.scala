package cellgen.spark

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


case class RunScriptMapInStrOutStr(lang: Expression,
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

      val result = native.runScriptMapInStrOutStr(
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
    val resultTerm = ctx.freshName("result")

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
            String trimmedLang = ${langGen.value}.trim();
            String trimmedFunc = ${funcGen.value}.trim();

            String $runnerKey = $computeKeyFuncName(
              trimmedLang,
              ${scriptGen.value}.toString(),
              trimmedFunc
            );

            $nativeClass $native = new $nativeClass();

            long pointer = $runnerMapTerm.computeIfAbsent(
              $runnerKey,
              k -> $native.newScriptRunner(
                trimmedLang,
                ${scriptGen.value}.toString(),
                trimmedFunc
              )
            );

            String $resultTerm = $native.runScriptMapInStrOutStr(
              pointer,
              ${columnGen.value}.toString()
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

  override def prettyName: String = "run_script_map_in_str_out_str"

//  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    RunScriptMapInStrOutStr(newChildren(0), newChildren(1), newChildren(2), newChildren(3))
  }

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType, StringType)
}


object RunScriptMapInStrOutStr {
  def apply(children: Seq[Expression]): Expression = {
    new RunScriptMapInStrOutStr(
      children.head,
      children.apply(1),
      children.apply(2),
      children.apply(3),
    )
  }
}
