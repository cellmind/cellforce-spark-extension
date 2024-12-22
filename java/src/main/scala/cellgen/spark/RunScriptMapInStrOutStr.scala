package cellgen.spark

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable


case class RunScriptMapInStrOutStr(lang: Expression,
                                   script: Expression,
                                   func: Expression,
                                   column: Expression) extends  Expression with ExpectsInputTypes {

  private val nativeFunctionRunnerPointers = new mutable.HashMap[String, Long]()

  override def children: Seq[Expression] = Seq(lang, script, func, column)

  override def dataType: DataType = StringType

  override def nullable: Boolean = true

  override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = {
    var langValue = lang.eval(input).asInstanceOf[UTF8String]
    val scriptValue = script.eval(input).asInstanceOf[UTF8String]
    var funcValue = func.eval(input).asInstanceOf[UTF8String]
    val columnValue = column.eval(input).asInstanceOf[UTF8String]

    if (langValue == null || scriptValue == null ||
      funcValue == null || columnValue == null) {
      return null
    }

    langValue = langValue.trim()
    funcValue = funcValue.trim()

    val langKey = DigestUtils.md5Hex(langValue.toString)
    val scriptKey = DigestUtils.md5Hex(scriptValue.toString)
    val funcKey = DigestUtils.md5Hex(funcValue.toString)
    val runnerKey = s"${langKey}:${scriptKey}:${funcKey}"

    val native = new NativeFunctions()
    val scriptRunnerPointer: Long =
      if (nativeFunctionRunnerPointers.contains(runnerKey)) {
        nativeFunctionRunnerPointers(runnerKey)
      } else {
        val pointer = native.newScriptRunner(langValue.toString, scriptValue.toString, funcValue.toString)
        nativeFunctionRunnerPointers.put(runnerKey, pointer)
        pointer
      }
    val result = native.runScriptMapInStrOutStr(scriptRunnerPointer, columnValue.toString)
    val utf8String = UTF8String.fromString(result)
    utf8String
  }

  // Implement codegen for better performance
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ExprCode.forNonNullValue(JavaCode.literal("UnsupportedOperation", dataType))
//    val childGen = child.genCode(ctx)
//
//    ev.copy(code = code"""
//      ${childGen.code}
//      boolean ${ev.isNull} = ${childGen.isNull};
//      String ${ev.value} = null;
//      if (!${ev.isNull}) {
//        ${ev.value} = "Echo: " + ${childGen.value}.toString();
//      }
//    """)
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
