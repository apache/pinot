/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.function;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FunctionRegistry {

  static Map<String, List<FunctionInfo>> _functionHandleMap = new HashMap<>();

  public static FunctionInfo resolve(String functionName, Object[] argumentTypes) {
    List<FunctionInfo> list = _functionHandleMap.get(functionName.toLowerCase());
    if(list != null && list.size() > 0) {
      return list.get(0);
    }
    return null;
  }

  public static void registerStaticFunction(Method method) {

    List<FunctionInfo> list = new ArrayList<>();
    FunctionInfo functionInfo = new FunctionInfo(method, method.getDeclaringClass());
    list.add(functionInfo);
    _functionHandleMap.put(method.getName().toLowerCase(), list);
  }
}
