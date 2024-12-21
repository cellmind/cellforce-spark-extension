/*
 * Copyright 2020 astonbitecode
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cellgen.spark;

import org.astonbitecode.j4rs.api.Instance;
import org.astonbitecode.j4rs.api.java2rust.Java2RustUtils;

public class NativeFunctions {

    private static native Instance newscriptrunner(Instance<String> lang);
    private static native Instance dropscriptrunner(Instance<Long> pointer);
    private static native Instance runscriptmapstr(Instance<Long> pointer, Instance<String> value);


    static {
        System.loadLibrary("cellgen_spark_extension");
    }

    public Long newScriptRunner(String lang) {
        Instance instance = newscriptrunner(Java2RustUtils.createInstance(lang));
        Long pointer = Java2RustUtils.getObjectCasted(instance);
        return pointer;
    }

    public Integer dropScriptRunner(Long pointer) {
        Instance instance = dropscriptrunner(Java2RustUtils.createInstance(pointer));
        Integer result = Java2RustUtils.getObjectCasted(instance);
        return result;
    }

    public String runScriptMapStr(Long pointer, String value) {
        Instance instance = runscriptmapstr(Java2RustUtils.createInstance(pointer), Java2RustUtils.createInstance(value));
        String result = Java2RustUtils.getObjectCasted(instance);
        return result;
    }

}

