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
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class FunctionInfo {
  private final Method _method;
  private final Class<?> _clazz;
  private final boolean _nullableParameters;

  public FunctionInfo(Function calciteFunction) {
    if (!(calciteFunction instanceof ScalarFunctionImpl)) {
      throw new IllegalArgumentException("Can only create FunctionInfo based on ScalarFunctionImpl. Got: "
          + calciteFunction.getClass());
    }

    ScalarFunctionImpl scalarFunction = ((ScalarFunctionImpl) calciteFunction);
    _method = scalarFunction.method;
    _clazz = scalarFunction.method.getDeclaringClass();
    _nullableParameters = _method.isAnnotationPresent(ScalarFunction.class)
        && _method.getAnnotation(ScalarFunction.class).nullableParameters();
  }

  public Method getMethod() {
    return _method;
  }

  public Class<?> getClazz() {
    return _clazz;
  }

  public boolean hasNullableParameters() {
    return _nullableParameters;
  }
}
