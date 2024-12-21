package cellgen.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String


case class RunScriptMapInStrOutStr(lang: Expression,
                                   script: Expression,
                                   func: Expression,
                                   column: Expression) extends  Expression with ExpectsInputTypes {

  override def children: Seq[Expression] = Seq(lang, script, func, column)

  override def dataType: DataType = StringType

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
    val native = new NativeFunctions()
    val scriptRunnerPointer = native.newScriptRunner(langValue.toString, scriptValue.toString, funcValue.toString);
    val result = native.runScriptMapInStrOutStr(scriptRunnerPointer, column.toString())
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