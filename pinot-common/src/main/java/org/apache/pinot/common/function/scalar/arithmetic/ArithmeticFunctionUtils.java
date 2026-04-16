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
package org.apache.pinot.common.function.scalar.arithmetic;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.pinot.common.function.sql.PinotSqlFunction;


/**
 * Shared Calcite registration utilities for arithmetic scalar functions.
 *
 * <p>This helper is stateless and thread-safe.
 */
final class ArithmeticFunctionUtils {
  private ArithmeticFunctionUtils() {
  }

  static PinotSqlFunction unaryArithmeticSqlFunction(String name) {
    return new PinotSqlFunction(name, ArithmeticFunctionUtils::inferUnaryReturnType,
        OperandTypes.family(List.of(SqlTypeFamily.ANY)));
  }

  static PinotSqlFunction binaryArithmeticSqlFunction(String name) {
    return new PinotSqlFunction(name, ArithmeticFunctionUtils::inferBinaryReturnType,
        OperandTypes.family(List.of(SqlTypeFamily.ANY, SqlTypeFamily.ANY)));
  }

  private static RelDataType inferUnaryReturnType(SqlOperatorBinding opBinding) {
    SqlTypeName returnType =
        SqlTypeUtil.isNumeric(opBinding.getOperandType(0)) ? normalizeNumericType(opBinding.getOperandType(0))
            : SqlTypeName.DOUBLE;
    return createTypeWithOperandNullability(opBinding, returnType);
  }

  private static RelDataType inferBinaryReturnType(SqlOperatorBinding opBinding) {
    RelDataType leftType = opBinding.getOperandType(0);
    RelDataType rightType = opBinding.getOperandType(1);
    SqlTypeName returnType;
    if (SqlTypeUtil.isNumeric(leftType) && SqlTypeUtil.isNumeric(rightType)) {
      SqlTypeName leftSqlType = normalizeNumericType(leftType);
      SqlTypeName rightSqlType = normalizeNumericType(rightType);
      if (leftSqlType == rightSqlType) {
        returnType = leftSqlType;
      } else if (isIntegral(leftSqlType) && isIntegral(rightSqlType)) {
        returnType = SqlTypeName.BIGINT;
      } else if (leftSqlType == SqlTypeName.DECIMAL || rightSqlType == SqlTypeName.DECIMAL) {
        returnType = SqlTypeName.DECIMAL;
      } else if (leftSqlType == SqlTypeName.DOUBLE || rightSqlType == SqlTypeName.DOUBLE) {
        returnType = SqlTypeName.DOUBLE;
      } else {
        returnType = SqlTypeName.REAL;
      }
    } else {
      // Preserve legacy coercions (for example, STRING inputs) by validating these functions as DOUBLE-returning.
      returnType = SqlTypeName.DOUBLE;
    }
    return createTypeWithOperandNullability(opBinding, returnType);
  }

  private static SqlTypeName normalizeNumericType(RelDataType relDataType) {
    switch (relDataType.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return SqlTypeName.INTEGER;
      case BIGINT:
        return SqlTypeName.BIGINT;
      case REAL:
      case FLOAT:
        return SqlTypeName.REAL;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      case DECIMAL:
        return SqlTypeName.DECIMAL;
      default:
        return SqlTypeName.DOUBLE;
    }
  }

  private static boolean isIntegral(SqlTypeName sqlTypeName) {
    return sqlTypeName == SqlTypeName.INTEGER || sqlTypeName == SqlTypeName.BIGINT;
  }

  private static RelDataType createTypeWithOperandNullability(SqlOperatorBinding opBinding, SqlTypeName sqlTypeName) {
    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    RelDataType type = typeFactory.createSqlType(sqlTypeName);
    boolean nullable = opBinding.collectOperandTypes().stream().anyMatch(RelDataType::isNullable);
    return typeFactory.createTypeWithNullability(type, nullable);
  }
}
