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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.pinot.common.request.context.GroupingSets;


/// Builds the {@code GROUPING(args...)} / {@code GROUPING_ID(args...)} value over the synthetic
/// {@code $groupingId} discriminator column (the grouping-set ordinal). Shared by the default planner
/// ({@link PinotAggregateExchangeNodeInsertRule}) and the v2 physical planner ({@code AggregatePushdownRule}) so both
/// compute the value identically, mirroring {@link GroupingSets#groupingValue}.
public class GroupingSetsRexUtils {
  private GroupingSetsRexUtils() {
  }

  /// Builds the GROUPING / GROUPING_ID value expression. The value is fully determined by the grouping set a
  /// row belongs to, so it is a plan-time constant per grouping-set ordinal: the expression is
  /// {@code CASE WHEN $groupingId = 0 THEN v_0 WHEN $groupingId = 1 THEN v_1 ... ELSE null END}, with each
  /// {@code v_k} packed from the argument columns' rolled-up bits in set {@code k} (first argument = most
  /// significant bit, PostgreSQL semantics, see {@link GroupingSets#groupingValuesByOrdinal}).
  ///
  /// The projection evaluates the CASE per output group row, so the per-row cost is O(number of grouping
  /// sets), bounded by {@link GroupingSets#MAX_GROUPING_SETS} (enforced where the sets are computed).
  /// TODO: replace the CASE with an O(1) per-ordinal lookup (an INT-array literal indexed by $groupingId,
  ///   mirroring the single-stage reduce-side precompute) once array literals are supported in the plan
  ///   expression conversion.
  ///
  /// @param groupingIdRef reference to the {@code $groupingId} INT column (the grouping-set ordinal)
  /// @param groupingSets per grouping set (in ordinal order), the participating union-column indexes (the
  ///                     list produced by {@code RelToPlanNodeConverter.computeGroupingSets}, so the ordinals
  ///                     here match the ones the runtime emits)
  /// @param argUnionIndexes the union-column index of each GROUPING argument, in argument order
  public static RexNode buildGroupingValue(RexBuilder rexBuilder, RelDataType intType, RexNode groupingIdRef,
      List<List<Integer>> groupingSets, List<Integer> argUnionIndexes) {
    int[] valuesByOrdinal = GroupingSets.groupingValuesByOrdinal(groupingSets, argUnionIndexes);
    int numSets = valuesByOrdinal.length;
    List<RexNode> caseOperands = new ArrayList<>(2 * numSets + 1);
    for (int ordinal = 0; ordinal < numSets; ordinal++) {
      caseOperands.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, groupingIdRef,
          rexBuilder.makeExactLiteral(BigDecimal.valueOf(ordinal), intType)));
      caseOperands.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(valuesByOrdinal[ordinal]), intType));
    }
    caseOperands.add(rexBuilder.makeNullLiteral(intType));
    return rexBuilder.makeCall(intType, SqlStdOperatorTable.CASE, caseOperands);
  }
}
