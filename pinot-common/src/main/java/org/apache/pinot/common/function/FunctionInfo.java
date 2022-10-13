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
import java.util.List;


public class FunctionInfo {
  private final Method _method;
  private final Class<?> _clazz;
  private final boolean _nullableParameters;
  private final List<Class<?>> _parameterTypes;
  private final Class<?> _returnType;
  private final boolean _narrowing;
  private final boolean _implicitStrings;

  public FunctionInfo(Method method, Class<?> clazz, boolean nullableParameters, boolean narrowing,
      boolean implicitStrings) {
    _method = method;
    _clazz = clazz;
    _nullableParameters = nullableParameters;
    _parameterTypes = List.of(_method.getParameterTypes());
    _returnType = method.getReturnType();
    _narrowing = narrowing;
    _implicitStrings = implicitStrings;
  }


  public Method getMethod() {
    return _method;
  }

  public Class<?> getClazz() {
    return _clazz;
  }

  public List<Class<?>> getParameterTypes() {
    return _parameterTypes;
  }

  public boolean hasNullableParameters() {
    return _nullableParameters;
  }

  public boolean isNarrowing() {
    return _narrowing;
  }

  public boolean isImplicitStrings() {
    return _implicitStrings;
  }
}
