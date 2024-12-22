package cellgen.spark

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types.{BooleanType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable


case class RunScriptMapInStrOutBool(lang: Expression,
                                   script: Expression,
                                   func: Expression,
                                   column: Expression) extends  Expression with ExpectsInputTypes {

  @transient private lazy val nativeFunctionRunnerPointers =
  new ConcurrentHashMap[String, Long]()

  override def children: Seq[Expression] = Seq(lang, script, func, column)

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = true

  override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = {

    val langValue = lang.eval(input).asInstanceOf[UTF8String]
    val scriptValue = script.eval(input).asInstanceOf[UTF8String]
    val funcValue = func.eval(input).asInstanceOf[UTF8String]
    val columnValue = column.eval(input).asInstanceOf[UTF8String]

    if (langValue == null || scriptValue == null ||
      funcValue == null || columnValue == null) {
      return null
    }

    // Compute keys once
    val runnerKey = computeRunnerKey(
      langValue.trim().toString,
      scriptValue.toString,
      funcValue.trim().toString
    )

    try {
      val native = new NativeFunctions()
      val scriptRunnerPointer = nativeFunctionRunnerPointers.computeIfAbsent(
        runnerKey,
        key => native.newScriptRunner(
          langValue.trim().toString,
          scriptValue.toString,
          funcValue.trim().toString
        )
      )

      native.runScriptMapInStrOutBool(scriptRunnerPointer, columnValue.toString)
    } catch {
      case e: Exception =>
        // Log error
        null
    }
  }

  private def computeRunnerKey(lang: String, script: String, func: String): String = {
    val langKey = DigestUtils.md5Hex(lang)
    val scriptKey = DigestUtils.md5Hex(script)
    val funcKey = DigestUtils.md5Hex(func)
    s"$langKey:$scriptKey:$funcKey"
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

    ev.copy(code =
      code"""
        ${langGen.code}
        ${scriptGen.code}
        ${funcGen.code}
        ${columnGen.code}
        boolean ${ev.isNull} = true;
        boolean ${ev.value} = false;

        if (!${langGen.isNull} && !${scriptGen.isNull} &&
            !${funcGen.isNull} && !${columnGen.isNull}) {

          String $runnerKey = $computeKeyFuncName(
            ${langGen.value}.trim(),
            ${scriptGen.value}.toString(),
            ${funcGen.value}.trim()
          );

          $nativeClass $native = new $nativeClass();

          try {
            long pointer = $runnerMapTerm.computeIfAbsent(
              $runnerKey,
              k -> $native.newScriptRunner(
                ${langGen.value}.trim().toString(),
                ${scriptGen.value}.toString(),
                ${funcGen.value}.trim().toString()
              )
            );

            ${ev.value} = $native.runScriptMapInStrOutBool(
              pointer,
              ${columnGen.value}.toString()
            );
            ${ev.isNull} = false;
          } catch (Exception e) {
            ${ev.isNull} = true;
          }
        }
      """)
  }

  override def prettyName: String = "run_script_map_in_str_out_bool"

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    RunScriptMapInStrOutBool(newChildren(0), newChildren(1), newChildren(2), newChildren(3))
  }

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType, StringType)
}


object RunScriptMapInStrOutBool {
  def apply(children: Seq[Expression]): Expression = {
    new RunScriptMapInStrOutBool(
      children.head,
      children.apply(1),
      children.apply(2),
      children.apply(3),
    )
  }
}
