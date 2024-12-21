
use std::collections::HashMap;
// Copyright 2020 astonbitecode
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::convert::TryFrom;
use std::sync::Arc;
use j4rs::InvocationArg;
use j4rs::prelude::*;
use j4rs_derive::*;
use cellgen_script_core::runner::base::ScriptFunctionRunner;
use cellgen_script_core::runner::builder::ScriptFunctionRunnerBuilder;

#[cfg(target_pointer_width = "64")]
pub unsafe fn jlong_to_pointer<T>(val: jlong) -> *mut T {
    val as *mut T
}

unsafe fn drop_pointer_in_place<T>(ptr: *mut T) {
    if !ptr.is_null() {
        std::ptr::drop_in_place(ptr);
    }
}

#[call_from_java("cellgen.spark.NativeFunctions.newscriptrunner")]
fn new_script_runner(lang: Instance, script: Instance, func: Instance) -> Result<Instance, String> {
    let jvm: Jvm = Jvm::attach_thread().unwrap();
    let lang: String = jvm.to_rust(lang).unwrap();
    let script: String = jvm.to_rust(script).unwrap();
    let func: String = jvm.to_rust(func).unwrap();

    let builder = ScriptFunctionRunnerBuilder::new();
    let runner = builder.build(lang.as_str(), script.as_str(), func.as_str()).unwrap();
    let runner = Box::new(runner);
    let runner: *mut Arc<dyn ScriptFunctionRunner> = Box::into_raw(runner);
    let pointer: i64 = runner as jlong;
    let ia = InvocationArg::try_from(pointer).map_err(|error| format!("{}", error)).unwrap();
    Instance::try_from(ia).map_err(|error| format!("{}", error))
}

#[call_from_java("cellgen.spark.NativeFunctions.dropscriptrunner")]
fn drop_script_runner(pointer: Instance) -> Result<Instance, String> {
    let jvm: Jvm = Jvm::attach_thread().unwrap();
    let pointer: i64 = jvm.to_rust(pointer).unwrap();
    let pointer: jlong = pointer;
    let runner = unsafe { jlong_to_pointer::<Arc<dyn ScriptFunctionRunner>>(pointer) };
    unsafe {drop_pointer_in_place(runner); }
    let ia = InvocationArg::try_from(0).map_err(|error| format!("{}", error)).unwrap();
    Instance::try_from(ia).map_err(|error| format!("{}", error))
}

#[call_from_java("cellgen.spark.NativeFunctions.runscriptmapinstroutstr")]
fn run_script_map_in_str_out_str(pointer: Instance, value: Instance) -> Result<Instance, String> {
    println!("call java ------------------- out str ");
    let jvm: Jvm = Jvm::attach_thread().unwrap();
    let pointer: i64 = jvm.to_rust(pointer).unwrap();
    let pointer: jlong = pointer;
    let runner: &Arc<dyn ScriptFunctionRunner> = unsafe { jlong_to_pointer::<Arc<dyn ScriptFunctionRunner>>(pointer).as_mut().unwrap() };
    let value: String = jvm.to_rust(value).unwrap();
    let result: String = runner.map_in_str_out_str(value.as_str()).unwrap();
    let ia = InvocationArg::try_from(result).map_err(|error| format!("{}", error)).unwrap();
    Instance::try_from(ia).map_err(|error| format!("{}", error))
}


#[call_from_java("cellgen.spark.NativeFunctions.runscriptmapinstroutbool")]
fn run_script_map_in_str_out_bool(pointer: Instance, value: Instance) -> Result<Instance, String> {
    println!("call java ------------------- out bool ");
    let jvm: Jvm = Jvm::attach_thread().unwrap();
    let pointer: i64 = jvm.to_rust(pointer).unwrap();
    let pointer: jlong = pointer;
    let runner: &Arc<dyn ScriptFunctionRunner> = unsafe { jlong_to_pointer::<Arc<dyn ScriptFunctionRunner>>(pointer).as_mut().unwrap() };
    let value: String = jvm.to_rust(value).unwrap();
    let result: bool = runner.map_in_str_out_bool(value.as_str()).unwrap();
    let ia = InvocationArg::try_from(result).map_err(|error| format!("{}", error)).unwrap();
    Instance::try_from(ia).map_err(|error| format!("{}", error))
}


