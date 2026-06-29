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
package org.apache.pinot.calcite.rel.rules;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;


/// Builds the {@code GROUPING(args...)} / {@code GROUPING_ID(args...)} value as a bit expression over the synthetic
/// {@code $groupingId} discriminator column. Shared by the default planner
/// ({@link PinotAggregateExchangeNodeInsertRule}) and the v2 physical planner ({@code AggregatePushdownRule}) so both
/// compute the value identically, mirroring {@link org.apache.pinot.common.request.context.GroupingSets#groupingValue}.
public class GroupingSetsRexUtils {
  private GroupingSetsRexUtils() {
  }

  /// Builds the GROUPING / GROUPING_ID value: for each argument (identified by its union-column index) extract its bit
  /// from {@code groupingIdRef}, and pack the bits with the first argument as the most significant bit. Uses Pinot's
  /// bit scalar functions, evaluated in the projection operator.
  ///
  /// @param groupingIdRef reference to the {@code $groupingId} INT column (bit i set iff union column i is rolled up)
  /// @param unionIndexes the union-column index of each GROUPING argument, in argument order
  public static RexNode buildGroupingValue(RexBuilder rexBuilder, RelDataType intType, RexNode groupingIdRef,
      List<Integer> unionIndexes) {
    SqlOperator bitAnd = pinotScalarOperator("bitAnd");
    SqlOperator bitShiftRightUnsigned = pinotScalarOperator("bitShiftRightUnsigned");
    SqlOperator bitShiftLeft = pinotScalarOperator("bitShiftLeft");
    SqlOperator bitOr = pinotScalarOperator("bitOr");
    int numArgs = unionIndexes.size();
    RexNode result = null;
    for (int j = 0; j < numArgs; j++) {
      RexNode k = rexBuilder.makeExactLiteral(BigDecimal.valueOf(unionIndexes.get(j)), intType);
      /// bit = (groupingId >>> k) & 1
      RexNode shifted = rexBuilder.makeCall(intType, bitShiftRightUnsigned, List.of(groupingIdRef, k));
      RexNode bit = rexBuilder.makeCall(intType, bitAnd,
          List.of(shifted, rexBuilder.makeExactLiteral(BigDecimal.ONE, intType)));
      /// Pack with the first argument as the MSB: shift left by (numArgs - 1 - j).
      int shiftLeftBy = numArgs - 1 - j;
      RexNode placed = shiftLeftBy == 0 ? bit : rexBuilder.makeCall(intType, bitShiftLeft,
          List.of(bit, rexBuilder.makeExactLiteral(BigDecimal.valueOf(shiftLeftBy), intType)));
      result = result == null ? placed : rexBuilder.makeCall(intType, bitOr, List.of(result, placed));
    }
    return result;
  }

  /// Looks up a Pinot scalar function as a Calcite {@link SqlOperator} by name, for building bit expressions in the
  /// grouping-set final projection.
  private static SqlOperator pinotScalarOperator(String name) {
    List<SqlOperator> operators = new ArrayList<>(1);
    PinotOperatorTable.instance(false).lookupOperatorOverloads(new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, operators,
        SqlNameMatchers.withCaseSensitive(false));
    Preconditions.checkState(!operators.isEmpty(), "Pinot scalar function not found: %s", name);
    return operators.get(0);
  }
}
