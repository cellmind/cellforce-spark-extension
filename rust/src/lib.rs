
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
use cellforce_script_core::runner::base::ScriptFunctionRunner;
use cellforce_script_core::runner::builder::ScriptFunctionRunnerBuilder;
use j4rs::errors::J4RsError;

#[cfg(target_pointer_width = "64")]
pub unsafe fn jlong_to_pointer<T>(val: jlong) -> *mut T {
    val as *mut T
}

unsafe fn drop_pointer_in_place<T>(ptr: *mut T) {
    if !ptr.is_null() {
        std::ptr::drop_in_place(ptr);
    }
}

fn j3rs_error_to_string(err: J4RsError) -> String {
    err.to_string()
}

#[call_from_java("cellforce.spark.NativeFunctions.newscriptrunner")]
fn new_script_runner(lang: Instance, script: Instance, func: Instance) -> Result<Instance, String> {
    let jvm: Jvm = Jvm::attach_thread().map_err(j3rs_error_to_string)?;
    let lang: String = jvm.to_rust(lang).map_err(j3rs_error_to_string)?;
    let script: String = jvm.to_rust(script).map_err(j3rs_error_to_string)?;
    let func: String = jvm.to_rust(func).map_err(j3rs_error_to_string)?;

    let builder = ScriptFunctionRunnerBuilder::new();
    let runner = builder.build(lang.as_str(), script.as_str(), func.as_str()).map_err(|e| e.to_string())?;
    let runner = Box::new(runner);
    let runner: *mut Arc<dyn ScriptFunctionRunner> = Box::into_raw(runner);
    let pointer: i64 = runner as jlong;
    let ia = InvocationArg::try_from(pointer).map_err(j3rs_error_to_string)?;
    Instance::try_from(ia).map_err(j3rs_error_to_string)
}

#[call_from_java("cellforce.spark.NativeFunctions.dropscriptrunner")]
fn drop_script_runner(pointer: Instance) -> Result<Instance, String> {
    let jvm: Jvm = Jvm::attach_thread().map_err(j3rs_error_to_string)?;
    let pointer: i64 = jvm.to_rust(pointer).map_err(j3rs_error_to_string)?;
    let pointer: jlong = pointer;
    let runner = unsafe { jlong_to_pointer::<Arc<dyn ScriptFunctionRunner>>(pointer) };
    unsafe {drop_pointer_in_place(runner); }
    let ia = InvocationArg::try_from(0).map_err(j3rs_error_to_string)?;
    Instance::try_from(ia).map_err(j3rs_error_to_string)
}

#[call_from_java("cellforce.spark.NativeFunctions.scriptmapinstroutstr")]
fn run_script_map_in_str_out_str(pointer: Instance, value: Instance) -> Result<Instance, String> {
    let jvm: Jvm = Jvm::attach_thread().map_err(j3rs_error_to_string)?;
    let pointer: i64 = jvm.to_rust(pointer).map_err(j3rs_error_to_string)?;
    let pointer: jlong = pointer;
    let runner: &Arc<dyn ScriptFunctionRunner> = unsafe { jlong_to_pointer::<Arc<dyn ScriptFunctionRunner>>(pointer).as_mut().ok_or("failed to convert long to pointer")? };
    let value: String = jvm.to_rust(value).map_err(j3rs_error_to_string)?;
    let result: String = runner.map_in_str_out_str(value.as_str()).map_err(|e| e.to_string())?;
    let ia = InvocationArg::try_from(result).map_err(j3rs_error_to_string)?;
    Instance::try_from(ia).map_err(j3rs_error_to_string)
}


#[call_from_java("cellforce.spark.NativeFunctions.scriptmapinstroutbool")]
fn run_script_map_in_str_out_bool(pointer: Instance, value: Instance) -> Result<Instance, String> {
    let jvm: Jvm = Jvm::attach_thread().map_err(j3rs_error_to_string)?;
    let pointer: i64 = jvm.to_rust(pointer).map_err(j3rs_error_to_string)?;
    let pointer: jlong = pointer;
    let runner: &Arc<dyn ScriptFunctionRunner> = unsafe { jlong_to_pointer::<Arc<dyn ScriptFunctionRunner>>(pointer).as_mut().ok_or("failed to convert long to pointer")? };
    let value: String = jvm.to_rust(value).map_err(j3rs_error_to_string)?;
    let result: bool = runner.map_in_str_out_bool(value.as_str()).map_err(|e| e.to_string())?;
    let ia = InvocationArg::try_from(result).map_err(j3rs_error_to_string)?;
    Instance::try_from(ia).map_err(j3rs_error_to_string)
}


