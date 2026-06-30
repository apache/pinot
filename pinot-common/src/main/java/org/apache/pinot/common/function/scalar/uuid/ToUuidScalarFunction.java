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
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.UuidUtils;


/**
 * Polymorphic scalar function that converts string or bytes inputs into Pinot's logical UUID type.
 *
 * <p>This implementation is stateless and thread-safe.
 */
@ScalarFunction(names = {"TO_UUID"})
public class ToUuidScalarFunction extends AbstractStringOrBytesUuidFunction {
  private static final FunctionInfo STRING_FUNCTION_INFO;
  private static final FunctionInfo BYTES_FUNCTION_INFO;

  static {
    try {
      STRING_FUNCTION_INFO =
          new FunctionInfo(ToUuidScalarFunction.class.getMethod("toUuid", String.class), ToUuidScalarFunction.class,
              true);
      BYTES_FUNCTION_INFO =
          new FunctionInfo(ToUuidScalarFunction.class.getMethod("toUuid", byte[].class), ToUuidScalarFunction.class,
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
    return "TO_UUID";
  }

  @Override
  public Set<String> getNames() {
    return Set.of("TO_UUID", "TOUUID");
  }

  @Nullable
  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return new PinotSqlFunction("TO_UUID", ReturnTypes.explicit(org.apache.calcite.sql.type.SqlTypeName.UUID),
        OperandTypes.or(OperandTypes.family(List.of(SqlTypeFamily.CHARACTER)),
            OperandTypes.family(List.of(SqlTypeFamily.BINARY))));
  }

  public static UUID toUuid(String value) {
    return value != null ? UuidUtils.toUUID(value) : null;
  }

  public static UUID toUuid(byte[] value) {
    return value != null ? UuidUtils.toUUID(value) : null;
  }
}
