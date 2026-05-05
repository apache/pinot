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
package org.apache.pinot.common.function.scalar.bitwise;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


/**
 * Shared utilities for bitwise scalar functions.
 *
 * <p>This helper is stateless and thread-safe.
 */
final class BitFunctionUtils {
  private BitFunctionUtils() {
  }

  static boolean isIntegral(ColumnDataType dataType) {
    return dataType == ColumnDataType.INT || dataType == ColumnDataType.LONG;
  }

  static PinotSqlFunction binaryIntegralSqlFunction(String name) {
    return new PinotSqlFunction(name, opBinding -> widerIntegralType(opBinding, 0, 1),
        OperandTypes.family(List.of(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)));
  }

  static PinotSqlFunction unaryIntegralSqlFunction(String name) {
    return new PinotSqlFunction(name, opBinding -> sameAsOperand(opBinding, 0),
        OperandTypes.family(List.of(SqlTypeFamily.INTEGER)));
  }

  static PinotSqlFunction maskSqlFunction(String name) {
    return new PinotSqlFunction(name, opBinding -> createTypeWithOperandNullability(opBinding, SqlTypeName.BIGINT),
        OperandTypes.family(List.of(SqlTypeFamily.INTEGER)));
  }

  static PinotSqlFunction shiftSqlFunction(String name) {
    return new PinotSqlFunction(name, opBinding -> sameAsOperand(opBinding, 0),
        OperandTypes.family(List.of(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)));
  }

  static PinotSqlFunction extractSqlFunction(String name) {
    return new PinotSqlFunction(name, BitFunctionUtils::integerType,
        OperandTypes.family(List.of(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)));
  }

  static boolean isBitIndexInRange(int bit, int width) {
    return bit >= 0 && bit < width;
  }

  static boolean isBitIndexInRange(long bit, int width) {
    return bit >= 0 && bit < width;
  }

  private static RelDataType widerIntegralType(SqlOperatorBinding opBinding, int firstIndex, int secondIndex) {
    SqlTypeName firstType = opBinding.getOperandType(firstIndex).getSqlTypeName();
    SqlTypeName secondType = opBinding.getOperandType(secondIndex).getSqlTypeName();
    SqlTypeName returnType =
        firstType == SqlTypeName.BIGINT || secondType == SqlTypeName.BIGINT ? SqlTypeName.BIGINT : SqlTypeName.INTEGER;
    return createTypeWithOperandNullability(opBinding, returnType);
  }

  private static RelDataType sameAsOperand(SqlOperatorBinding opBinding, int operandIndex) {
    SqlTypeName operandType = opBinding.getOperandType(operandIndex).getSqlTypeName();
    SqlTypeName returnType = operandType == SqlTypeName.BIGINT ? SqlTypeName.BIGINT : SqlTypeName.INTEGER;
    return createTypeWithOperandNullability(opBinding, returnType);
  }

  private static RelDataType integerType(SqlOperatorBinding opBinding) {
    return createTypeWithOperandNullability(opBinding, SqlTypeName.INTEGER);
  }

  private static RelDataType createTypeWithOperandNullability(SqlOperatorBinding opBinding, SqlTypeName sqlTypeName) {
    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    RelDataType type = typeFactory.createSqlType(sqlTypeName);
    boolean nullable = opBinding.collectOperandTypes().stream().anyMatch(RelDataType::isNullable);
    return typeFactory.createTypeWithNullability(type, nullable);
  }
}
