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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.spi.exception.QueryErrorCode;


/// Custom implementation of Calcite's default type coercion implementation to add Pinot specific type coercion rules.
/// Currently, the only additional rule we add is to for TIMESTAMP / BIGINT types. For binary arithmetic and binary
/// comparison operators, we add implicit casts between TIMESTAMP and BIGINT. For other standard operators, we add
/// implicit casts in both directions as and when needed (TIMESTAMP -> BIGINT and BIGINT -> TIMESTAMP).
///
/// This always works since Pinot's execution type for the TIMESTAMP SQL type is LONG (i.e., BIGINT). We add these
/// implicit casts for convenience since the single-stage engine already treats the two types as interchangeable and
/// many common user query patterns include things like TIMESTAMP + LONG or TIMESTAMP - LONG or TIMESTAMP > LONG.
///
/// For binary arithmetic, the cast is always added on the TIMESTAMP side so that the result is BIGINT (long) arithmetic
/// (e.g., TIMESTAMP - LONG -> BIGINT). For binary comparisons, we prefer to add the cast on the operand that is not a
/// column reference whenever possible. For example, given a TIMESTAMP column `ts` and a BIGINT literal `L`,
/// `ts > L` is rewritten as `ts > CAST(L AS TIMESTAMP)` rather than `CAST(ts AS BIGINT) > L`. This
/// avoids wrapping the column in a per-row CAST (which is expensive on the query path and breaks index applicability)
/// while remaining semantically equivalent. If both sides are column references (or neither is), we fall back to
/// casting the TIMESTAMP side to BIGINT to preserve the long-standing default behavior.
///
/// A multi-value column (ARRAY type) compared for equality against a non-array value, i.e. `mvCol = 'a'`,
/// `mvCol <> 'a'`, `mvCol IN ('a', 'b')` or `mvCol NOT IN ('a', 'b')`, is wrapped in [PinotOperatorTable#ARRAY_TO_MV]
/// so the predicate keeps the single-stage engine semantics (`=` and `IN` match if any value matches, `<>` and
/// `NOT IN` match if no value matches). Without this, Calcite casts the value to an array, which the leaf stage cannot
/// evaluate. Like an explicit `ARRAY_TO_MV`, the predicate is only supported in a filter on the leaf stage.
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
      boolean firstIsArray = SqlTypeUtil.isArray(binding.getOperandType(0));
      boolean secondIsArray = SqlTypeUtil.isArray(binding.getOperandType(1));
      if (firstIsArray != secondIsArray) {
        int mvIndex = firstIsArray ? 0 : 1;
        if (isMultiValueColumn(binding, mvIndex)) {
          // Range comparisons are not wrapped: Calcite rewrites NOT(mv > x) to mv <= x, which is not the complement
          // under the "any value matches" semantics. Not coercing makes validation fail, as for other array types.
          return (kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS)
              && coerceMultiValueComparison(binding, mvIndex, () -> binaryComparisonCoercion(binding));
        }
      }

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

  @Override
  public boolean inOperationCoercion(SqlCallBinding binding) {
    SqlKind kind = binding.getOperator().getKind();
    if ((kind == SqlKind.IN || kind == SqlKind.NOT_IN) && binding.operand(1) instanceof SqlNodeList
        && isMultiValueColumn(binding, 0)
        && isScalarValueListRejectingNull(binding, (SqlNodeList) binding.operand(1))) {
      return coerceMultiValueComparison(binding, 0, () -> inOperationCoercion(binding));
    }
    return super.inOperationCoercion(binding);
  }

  /// Returns whether the operand at the given index is a reference to a multi-value (ARRAY type) column.
  private static boolean isMultiValueColumn(SqlCallBinding binding, int index) {
    return SqlTypeUtil.isArray(binding.getOperandType(index)) && isColumnReference(binding.operand(index));
  }

  /// Returns whether all values of the `IN` list are non-array values, and throws on a `NULL` value: Calcite types it
  /// as the array type of the column, and the leaf stage cannot evaluate it in a multi-value predicate.
  private static boolean isScalarValueListRejectingNull(SqlCallBinding binding, SqlNodeList values) {
    for (SqlNode value : values) {
      if (SqlUtil.isNullLiteral(value, false)) {
        throw QueryErrorCode.QUERY_VALIDATION.asException(
            "NULL is not supported in an IN list compared to multi-value column: " + binding.operand(0));
      }
      if (SqlTypeUtil.isArray(binding.getValidator().deriveType(binding.getScope(), value))) {
        return false;
      }
    }
    return true;
  }

  /// Wraps the multi-value column at the given index in `ARRAY_TO_MV`, then coerces the compared values against its
  /// element type with the given coercion. The coercion result is not used: wrapping the column is already a
  /// coercion. If the coercion casts the column, the wrapping is undone and false is returned, so validation fails as
  /// before: the leaf stage evaluates `ARRAY_TO_MV` only directly in a predicate, not inside a cast.
  private boolean coerceMultiValueComparison(SqlCallBinding binding, int index, BooleanSupplier coercion) {
    SqlCall call = binding.getCall();
    List<SqlNode> operands = new ArrayList<>(call.getOperandList());
    SqlNode column = operands.get(index);
    RelDataType columnType = binding.getOperandType(index);
    SqlCall arrayToMv = PinotOperatorTable.ARRAY_TO_MV.createCall(column.getParserPosition(), column);
    call.setOperand(index, arrayToMv);
    updateInferredType(arrayToMv,
        factory.createTypeWithNullability(columnType.getComponentType(), columnType.isNullable()));
    coercion.getAsBoolean();
    if (call.operand(index) == arrayToMv) {
      return true;
    }
    for (int i = 0; i < operands.size(); i++) {
      call.setOperand(i, operands.get(i));
    }
    return false;
  }

  /// Returns whether the node is a column reference, or `ARRAY_TO_MV` over a column reference.
  private static boolean isColumnReference(SqlNode node) {
    if (node instanceof SqlCall && ((SqlCall) node).getOperator() == PinotOperatorTable.ARRAY_TO_MV) {
      node = ((SqlCall) node).operand(0);
    }
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
