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
package org.apache.pinot.common.request.context;

import java.util.List;
import java.util.Objects;
import java.util.Set;


/**
 * The {@code FunctionContext} class represents the function in the expression.
 * <p>Pinot currently supports 2 types of functions: Aggregation (e.g. SUM, MAX) and Transform (e.g. ADD, SUB).
 */
public class FunctionContext {
  public enum Type {
    AGGREGATION, TRANSFORM
  }

  private final Type _type;
  private String _functionName;
  private final List<ExpressionContext> _arguments;

  public FunctionContext(Type type, String functionName, List<ExpressionContext> arguments) {
    _type = type;
    // NOTE: Standardize the function name to lower case
    _functionName = functionName.toLowerCase();
    _arguments = arguments;
  }

  public Type getType() {
    return _type;
  }

  public void setFunctionName(String functionName) {
    // NOTE: Standardize the function name to lower case
    _functionName = functionName.toLowerCase();
  }

  public String getFunctionName() {
    return _functionName;
  }

  public List<ExpressionContext> getArguments() {
    return _arguments;
  }

  /**
   * Adds the columns (IDENTIFIER expressions) in the function to the given set.
   */
  public void getColumns(Set<String> columns) {
    for (ExpressionContext argument : _arguments) {
      argument.getColumns(columns);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FunctionContext)) {
      return false;
    }
    FunctionContext that = (FunctionContext) o;
    return _type == that._type && Objects.equals(_functionName, that._functionName) && Objects
        .equals(_arguments, that._arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _functionName, _arguments);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(_functionName).append('(');
    int numArguments = _arguments.size();
    if (numArguments > 0) {
      stringBuilder.append(_arguments.get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(',').append(_arguments.get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }
}
