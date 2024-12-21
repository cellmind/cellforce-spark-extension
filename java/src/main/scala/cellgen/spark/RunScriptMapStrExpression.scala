package cellgen.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String


case class RunScriptMapStrExpression(child: Expression) extends  UnaryExpression with Serializable {

  override def dataType: DataType = StringType

  override def nullable: Boolean = child.nullable

  // Implement evaluation logic
  override protected def nullSafeEval(input: Any): Any = {
    s"Echo: ${input.toString}"
  }

  override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) null
    else {
      val native = new NativeFunctions()
      val scriptRunnerPointer = native.newScriptRunner("rhai", "fn double_str(s) { s + s } ", "double_str");
      val result = native.runScriptMapInStrOutStr(scriptRunnerPointer, value.toString);
      val utf8String = UTF8String.fromString(result)
      utf8String
    }
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

  override def prettyName: String = "run_script_map_str"

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }


}

