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
import org.apache.pinot.common.utils.PinotDataType;


public class BuiltInFunctionDefinition extends BuiltInFunctionDefinitionBase {
  private final Class<?> _clazz;
  private final boolean _nullableParameters;

  public BuiltInFunctionDefinition(String name, Method method, Class<?> clazz, boolean nullableParameters) {
    super(name, method);
    _clazz = clazz;
    _nullableParameters = nullableParameters;
  }

  public Method getMethod() {
    return _method;
  }

  public PinotDataType[] getArgumentTypes() {
    return _argumentTypes;
  }

  public PinotDataType getResultType() {
    return _resultType;
  }

  public Class<?> getClazz() {
    return _clazz;
  }

  public boolean hasNullableParameters() {
    return _nullableParameters;
  }
}
