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
package org.apache.pinot.common.function.scalar.uuid;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.UuidUtils;


/**
 * Polymorphic scalar function that validates string or bytes values as UUID inputs.
 *
 * <p>This implementation is stateless and thread-safe.
 */
@ScalarFunction(names = {"IS_UUID"})
public class IsUuidScalarFunction extends AbstractStringOrBytesUuidFunction {
  private static final FunctionInfo STRING_FUNCTION_INFO;
  private static final FunctionInfo BYTES_FUNCTION_INFO;

  static {
    try {
      STRING_FUNCTION_INFO =
          new FunctionInfo(IsUuidScalarFunction.class.getMethod("isUuid", String.class), IsUuidScalarFunction.class,
              true);
      BYTES_FUNCTION_INFO =
          new FunctionInfo(IsUuidScalarFunction.class.getMethod("isUuid", byte[].class), IsUuidScalarFunction.class,
              true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FunctionInfo getStringFunctionInfo() {
    return STRING_FUNCTION_INFO;
  }

  @Override
  protected FunctionInfo getBytesFunctionInfo() {
    return BYTES_FUNCTION_INFO;
  }

  @Override
  public String getName() {
    return "IS_UUID";
  }

  @Override
  public Set<String> getNames() {
    return Set.of("IS_UUID", "ISUUID");
  }

  @Nullable
  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return new PinotSqlFunction("IS_UUID", ReturnTypes.BOOLEAN,
        OperandTypes.or(OperandTypes.family(List.of(SqlTypeFamily.CHARACTER)),
            OperandTypes.family(List.of(SqlTypeFamily.BINARY))));
  }

  public static boolean isUuid(String value) {
    return UuidUtils.isUuid(value);
  }

  public static boolean isUuid(byte[] value) {
    return UuidUtils.isUuid(value);
  }
}
