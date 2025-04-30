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
package org.apache.pinot.query.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;


/**
 * Custom implementation of Calcite's default type coercion implementation to add Pinot specific type coercion rules.
 * Currently, the only additional rule we add is to for TIMESTAMP / BIGINT types. For binary arithmetic and binary
 * comparison operators, we add implicit casts to convert TIMESTAMP to BIGINT. For standard operators, we add implicit
 * casts to convert TIMESTAMP to BIGINT and also vice versa.
 * <p>
 * This always works since Pinot's execution type for the TIMESTAMP SQL type is LONG. We add these implicit casts for
 * convenience since the single-stage engine already treats the two types as interchangeable and many common user query
 * patterns include things like TIMESTAMP + LONG or TIMESTAMP - LONG or TIMESTAMP > LONG. For such operations, we're
 * always adding the cast on the TIMESTAMP side, but users can choose to add explicit casts instead, for performance
 * reasons (adding cast on literal side instead of column side) and index applicability.
 */
public class PinotTypeCoercion extends TypeCoercionImpl {
  public PinotTypeCoercion(RelDataTypeFactory typeFactory,
      SqlValidator validator) {
    super(typeFactory, validator);
  }

  @Override
  public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();

    if (binding.getOperandCount() == 2 && kind.belongsTo(SqlKind.BINARY_ARITHMETIC)) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);

      if (SqlTypeUtil.isTimestamp(type1) && SqlTypeUtil.isIntType(type2)) {
        return coerceOperandType(binding.getScope(), binding.getCall(), 0, factory.createSqlType(SqlTypeName.BIGINT));
      }
      if (SqlTypeUtil.isIntType(type1) && SqlTypeUtil.isTimestamp(type2)) {
        return coerceOperandType(binding.getScope(), binding.getCall(), 1, factory.createSqlType(SqlTypeName.BIGINT));
      }
    }

    return super.binaryArithmeticCoercion(binding);
  }

  @Override
  public boolean binaryComparisonCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();

    if (binding.getOperandCount() == 2 && kind.belongsTo(SqlKind.BINARY_COMPARISON)) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);

      if (SqlTypeUtil.isTimestamp(type1) && SqlTypeUtil.isIntType(type2)) {
        return coerceOperandType(binding.getScope(), binding.getCall(), 0, factory.createSqlType(SqlTypeName.BIGINT));
      }
      if (SqlTypeUtil.isIntType(type1) && SqlTypeUtil.isTimestamp(type2)) {
        return coerceOperandType(binding.getScope(), binding.getCall(), 1, factory.createSqlType(SqlTypeName.BIGINT));
      }
    }

    return super.binaryComparisonCoercion(binding);
  }

  @Override
  public RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
    if (in.getSqlTypeName() == SqlTypeName.TIMESTAMP && expected == SqlTypeFamily.NUMERIC) {
      return factory.createSqlType(SqlTypeName.BIGINT);
    }
    if (in.getSqlTypeName() == SqlTypeName.BIGINT && expected == SqlTypeFamily.DATETIME) {
      return factory.createSqlType(SqlTypeName.TIMESTAMP);
    }
    return super.implicitCast(in, expected);
  }
}
