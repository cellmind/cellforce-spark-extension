/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cellgen.spark

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.aggregate.{RunScriptReduceInIntOutInt, UserSum}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}

class SparkUdfExtension extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectFunction(
        (new FunctionIdentifier("run_script_map_in_str_out_str"),
          new ExpressionInfo(classOf[RunScriptMapInStrOutStr].getName,
            "run_script_map_in_str_out_str"),  RunScriptMapInStrOutStr.apply)
    )

    extensions.injectFunction(
      (new FunctionIdentifier("run_script_map_in_str_out_bool"),
        new ExpressionInfo(classOf[RunScriptMapInStrOutBool].getName,
          "run_script_map_in_str_out_bool"),  RunScriptMapInStrOutBool.apply)
    )

    extensions.injectFunction(
      (new FunctionIdentifier("run_script_reduce_in_int_out_int"),
        new ExpressionInfo(
          classOf[RunScriptReduceInIntOutInt].getCanonicalName,
          "run_script_reduce_in_int_out_int"
        ),
        RunScriptReduceInIntOutInt.apply)
    )

    extensions.injectFunction(
      (new FunctionIdentifier("user_sum"),
      new ExpressionInfo(
        classOf[UserSum].getCanonicalName,
        "user_sum"
      ),
        UserSum.apply)
    )
  }
}

