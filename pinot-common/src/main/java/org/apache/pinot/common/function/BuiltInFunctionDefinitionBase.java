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

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Built-in function definition that provides the default result/parameter type resolver.
 */
public abstract class BuiltInFunctionDefinitionBase implements FunctionDefinition {

  protected final String _name;
  protected final Method _method;
  protected final PinotDataType[] _argumentTypes;
  protected final PinotDataType _resultType;

  protected BuiltInFunctionDefinitionBase(String name, Method method) {
    _name = name;
    _method = method;
    Class<?>[] signature = method.getParameterTypes();
    _argumentTypes = new PinotDataType[signature.length];
    for (int i = 0; i < signature.length; i++) {
      _argumentTypes[i] = FunctionUtils.getParameterType(signature[i]);
    }
    _resultType = FunctionUtils.getParameterType(_method.getReturnType());
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public PinotDataType[] getArgumentTypes(Class<?>[] signature) {
    Preconditions.checkState(signature == _method.getParameterTypes());
    return _argumentTypes;
  }

  @Override
  public PinotDataType[] getArgumentTypes(FieldSpec.DataType[] argumentDataTypes) {
    // TODO: add precondition checkers.
    return _argumentTypes;
  }

  @Override
  public PinotDataType getResultType(PinotDataType[] parameterTypes) {
    return _resultType;
  }

  @Override
  public Method getFunction(PinotDataType[] argumentTypes) {
    return _method;
  }

  @Override
  public Method getFunction(PinotDataType[] argumentTypes, Object[] literalObjects) {
    return _method;
  }
}
