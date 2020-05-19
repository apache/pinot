/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function;

import java.lang.reflect.Method;


public class FunctionInfo {

  private final Method _method;
  private final Class<?> _clazz;

  public FunctionInfo(Method method, Class<?> clazz) {
    super();
    this._method = method;
    this._clazz = clazz;
  }

  public Method getMethod() {
    return _method;
  }

  public Class<?> getClazz() {
    return _clazz;
  }

  /**
   * Check if the Function is applicable to the argumentTypes.
   * We can only know the types at runtime, so we can validate if the return type is Object.
   * For e.g funcA( funcB('3.14'), columnA)
   * We can only know return type of funcB and 3.14 (String.class) but
   * we cannot know the type of columnA in advance without knowing the source schema
   * @param argumentTypes
   * @return
   */
  public boolean isApplicable(Class<?>[] argumentTypes) {

    Class<?>[] parameterTypes = _method.getParameterTypes();

    if (parameterTypes.length != argumentTypes.length) {
      return false;
    }

    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> type = parameterTypes[i];
      //
      if (!type.isAssignableFrom(argumentTypes[i]) && argumentTypes[i] != Object.class) {
        return false;
      }
    }
    return true;
  }

  /**
   * Eventually we will need to convert the input datatypes before invoking the actual method. For now, there is no conversion
   *
   * @param args
   * @return
   */
  public Object[] convertTypes(Object[] args) {
    return args;
  }
}
