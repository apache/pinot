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
package org.apache.pinot.common.function.scalar;

import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(nullableParameters = true, names = {"case", "caseWhen"})
public class CaseWhenScalarFunction implements PinotScalarFunction {

  private static final FunctionInfo FUNCTION_INFO;

  static {
    try {
      FUNCTION_INFO = new FunctionInfo(CaseWhenScalarFunction.class.getMethod("caseWhen", Object[].class),
          CaseWhenScalarFunction.class, true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "case";
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(DataSchema.ColumnDataType[] argumentTypes) {
    // This function is an Object based function that can handle any type of arguments. Ideally, we would at least
    // check if the arguments are of type BOOLEAN, OBJECT, BOOLEAN, OBJECT, ... but that would break backward
    // compatibility with the v1 engine since the earlier implementation did not have such a check. In the v2 engine,
    // the standard Calcite CASE operator has the necessary type checks.
    return getFunctionInfo(argumentTypes.length);
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    if (numArguments < 2) {
      return null;
    }

    return FUNCTION_INFO;
  }

  public static Object caseWhen(Object... objs) {
    for (int i = 0; i < objs.length - 1; i += 2) {
      if (Boolean.TRUE.equals(objs[i])) {
        return objs[i + 1];
      }
    }
    // with or without else statement.
    return objs.length % 2 == 0 ? null : objs[objs.length - 1];
  }
}
