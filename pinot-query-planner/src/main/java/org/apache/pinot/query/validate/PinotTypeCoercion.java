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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;


/**
 * Custom implementation of Calcite's default type coercion implementation to add Pinot specific type coercion rules.
 * Currently, the only additional rule we add is to for TIMESTAMP / BIGINT types. For binary arithmetic and binary
 * comparison operators, we add implicit casts between TIMESTAMP and BIGINT. For other standard operators, we add
 * implicit casts in both directions as and when needed (TIMESTAMP -> BIGINT and BIGINT -> TIMESTAMP).
 * <p>
 * This always works since Pinot's execution type for the TIMESTAMP SQL type is LONG (i.e., BIGINT). We add these
 * implicit casts for convenience since the single-stage engine already treats the two types as interchangeable and
 * many common user query patterns include things like TIMESTAMP + LONG or TIMESTAMP - LONG or TIMESTAMP > LONG.
 * <p>
 * For binary arithmetic, the cast is always added on the TIMESTAMP side so that the result is BIGINT (long) arithmetic
 * (e.g., TIMESTAMP - LONG -> BIGINT). For binary comparisons, we prefer to add the cast on the operand that is not a
 * column reference whenever possible. For example, given a TIMESTAMP column {@code ts} and a BIGINT literal {@code L},
 * {@code ts > L} is rewritten as {@code ts > CAST(L AS TIMESTAMP)} rather than {@code CAST(ts AS BIGINT) > L}. This
 * avoids wrapping the column in a per-row CAST (which is expensive on the query path and breaks index applicability)
 * while remaining semantically equivalent. If both sides are column references (or neither is), we fall back to
 * casting the TIMESTAMP side to BIGINT to preserve the long-standing default behavior.
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

      boolean firstIsTimestamp = SqlTypeUtil.isTimestamp(type1);
      boolean secondIsTimestamp = SqlTypeUtil.isTimestamp(type2);
      boolean firstIsInt = SqlTypeUtil.isIntType(type1);
      boolean secondIsInt = SqlTypeUtil.isIntType(type2);

      if ((firstIsTimestamp && secondIsInt) || (firstIsInt && secondIsTimestamp)) {
        int timestampIdx = firstIsTimestamp ? 0 : 1;
        int intIdx = firstIsTimestamp ? 1 : 0;
        // Prefer casting the non-column-reference operand to keep the column unwrapped.
        // When the TIMESTAMP operand is a column and the BIGINT operand is not, cast the BIGINT operand to TIMESTAMP
        // rather than wrapping the column in a per-row CAST. This is semantically equivalent because Pinot's
        // execution type for TIMESTAMP is LONG.
        if (isColumnReference(binding.operand(timestampIdx))
            && !isColumnReference(binding.operand(intIdx))) {
          return coerceOperandType(binding.getScope(), binding.getCall(), intIdx,
              factory.createSqlType(SqlTypeName.TIMESTAMP));
        }
        return coerceOperandType(binding.getScope(), binding.getCall(), timestampIdx,
            factory.createSqlType(SqlTypeName.BIGINT));
      }
    }

    return super.binaryComparisonCoercion(binding);
  }

  private static boolean isColumnReference(SqlNode node) {
    return node instanceof SqlIdentifier && !((SqlIdentifier) node).isStar();
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
