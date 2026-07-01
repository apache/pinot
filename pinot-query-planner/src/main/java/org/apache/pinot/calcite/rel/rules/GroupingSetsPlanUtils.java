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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.common.request.context.GroupingSets;


/// The shared planner-side logic for GROUP BY GROUPING SETS / ROLLUP / CUBE, used by both the default planner
/// ({@link PinotAggregateExchangeNodeInsertRule}) and the v2 physical planner ({@code AggregatePushdownRule})
/// so the two cannot drift on the encoding: the grouping sets of a query are the ordered, de-duplicated list
/// produced by {@link #computeGroupingSets}, a set's position in that list is its ordinal (the value of the
/// synthetic {@link GroupingSets#GROUPING_ID_COLUMN} discriminator), and GROUPING()/GROUPING_ID() values are
/// derived from set membership via {@link GroupingSets#groupingValuesByOrdinal}.
public class GroupingSetsPlanUtils {
  private GroupingSetsPlanUtils() {
  }

  /// Encodes each grouping set as the sorted list of its participating union group-by column indexes
  /// (positions in {@code groupSet.asList()}), in {@code getGroupSets()} order, de-duplicated (first
  /// occurrence wins, matching the single-stage parser: duplicate sets — reachable via explicit
  /// {@code GROUPING SETS ((a), (a))} — collapse to one). A set's position in the returned list is its
  /// ordinal, the value carried as the synthetic $groupingId discriminator. Mirrors the single-stage engine's
  /// {@code PinotQuery.groupingSets} so the per-set expansion can be pushed down to the single-stage (leaf)
  /// engine, and mirrors Calcite's per-set column bitset so the number of grouping columns is unlimited.
  /// Returns an empty list for a plain GROUP BY (SIMPLE). This is the single conversion point for both
  /// planners: every consumer of the ordinal (planner GROUPING projections, RepeatOperator, leaf pushdown)
  /// derives the set order from the list produced here.
  public static List<List<Integer>> computeGroupingSets(Aggregate node) {
    if (node.getGroupType() == Aggregate.Group.SIMPLE) {
      return List.of();
    }
    /// Same cap as the single-stage parser: every set is materialized in the plan, expanded per input row at
    /// runtime, and enumerated by the GROUPING()/GROUPING_ID() projection.
    if (node.getGroupSets().size() > GroupingSets.MAX_GROUPING_SETS) {
      throw new UnsupportedOperationException(
          "GROUPING SETS / ROLLUP / CUBE expands to more than " + GroupingSets.MAX_GROUPING_SETS
              + " grouping sets");
    }
    List<Integer> union = node.getGroupSet().asList();
    Map<Integer, Integer> unionIndex = new HashMap<>();
    for (int i = 0; i < union.size(); i++) {
      unionIndex.put(union.get(i), i);
    }
    Set<List<Integer>> seen = new LinkedHashSet<>();
    for (ImmutableBitSet set : node.getGroupSets()) {
      List<Integer> columnIndexes = new ArrayList<>(set.cardinality());
      for (int bit : set) {
        columnIndexes.add(unionIndex.get(bit));
      }
      Collections.sort(columnIndexes);
      seen.add(columnIndexes);
    }
    return new ArrayList<>(seen);
  }

  /// Returns the LEAF row type for a grouping-set aggregate: the synthetic
  /// {@link GroupingSets#GROUPING_ID_COLUMN} INT column inserted right after the {@code groupCount} union
  /// group-by columns, i.e. {@code [group keys..., $groupingId, aggregates...]}. Shared by
  /// {@code PinotLogicalAggregate} and {@code PhysicalAggregate} so the two planners' {@code deriveRowType()}
  /// overrides cannot drift.
  public static RelDataType appendGroupingIdColumn(RelDataTypeFactory typeFactory, RelDataType rowType,
      int groupCount) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < groupCount; i++) {
      builder.add(fields.get(i));
    }
    builder.add(GroupingSets.GROUPING_ID_COLUMN, typeFactory.createSqlType(SqlTypeName.INTEGER));
    for (int i = groupCount; i < fields.size(); i++) {
      builder.add(fields.get(i));
    }
    return builder.build();
  }

  /// Splits GROUPING() / GROUPING_ID() out of the aggregate calls: they are not real aggregations (they are
  /// functions of which grouping set a row belongs to, computed from $groupingId in the final projection).
  /// Adds the real (non-GROUPING) calls to {@code outRealAggCalls} and returns, per original call, its index
  /// among the real calls or -1 for a GROUPING / GROUPING_ID call.
  public static int[] splitOutGroupingCalls(List<AggregateCall> orgAggCalls, List<AggregateCall> outRealAggCalls) {
    int[] realAggIndex = new int[orgAggCalls.size()];
    for (int i = 0; i < orgAggCalls.size(); i++) {
      SqlKind kind = orgAggCalls.get(i).getAggregation().getKind();
      if (kind == SqlKind.GROUPING || kind == SqlKind.GROUPING_ID) {
        realAggIndex[i] = -1;
      } else {
        realAggIndex[i] = outRealAggCalls.size();
        outRealAggCalls.add(orgAggCalls.get(i));
      }
    }
    return realAggIndex;
  }

  /// Builds the projection expressions restoring the original grouping-set aggregate row type on top of the
  /// FINAL aggregate, whose output is {@code [union keys..., $groupingId, real aggregate results...]}: the
  /// group keys, then per original aggregate call either the real aggregate result reference or the
  /// GROUPING() / GROUPING_ID() value computed from $groupingId. {@code $groupingId} itself is dropped.
  ///
  /// @param finalAggRel the FINAL aggregate node the projection reads from
  /// @param aggRel the original grouping-set aggregate (provides union order, aggregate calls, and sets)
  /// @param realAggIndex per original aggregate call, its index among the real calls or -1 for GROUPING calls
  ///                     (from {@link #splitOutGroupingCalls})
  public static List<RexNode> buildGroupingSetsProjects(RexBuilder rexBuilder, RelNode finalAggRel, Aggregate aggRel,
      int[] realAggIndex) {
    int groupCount = aggRel.getGroupCount();
    int finalGroupCount = groupCount + 1;
    RelDataType intType = finalAggRel.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    /// $groupingId is the INT discriminator column immediately after the union group keys.
    RexNode groupingIdRef = rexBuilder.makeInputRef(finalAggRel, groupCount);
    List<Integer> union = aggRel.getGroupSet().asList();
    /// The ordinal order of the sets — the same list the runtime receives on the wire.
    List<List<Integer>> groupingSets = computeGroupingSets(aggRel);
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    List<RexNode> projects = new ArrayList<>(groupCount + orgAggCalls.size());
    for (int i = 0; i < groupCount; i++) {
      projects.add(rexBuilder.makeInputRef(finalAggRel, i));
    }
    for (int i = 0; i < orgAggCalls.size(); i++) {
      if (realAggIndex[i] >= 0) {
        projects.add(rexBuilder.makeInputRef(finalAggRel, finalGroupCount + realAggIndex[i]));
      } else {
        AggregateCall groupingCall = orgAggCalls.get(i);
        /// Map each GROUPING argument (an input column index) to its position in the union group key list (the
        /// index space the grouping sets are expressed in).
        List<Integer> unionIndexes = new ArrayList<>(groupingCall.getArgList().size());
        for (int arg : groupingCall.getArgList()) {
          int unionIndex = union.indexOf(arg);
          Preconditions.checkState(unionIndex >= 0, "GROUPING argument column %s is not a grouping column", arg);
          unionIndexes.add(unionIndex);
        }
        RexNode value = buildGroupingValue(rexBuilder, intType, groupingIdRef, groupingSets, unionIndexes);
        projects.add(rexBuilder.makeCast(groupingCall.getType(), value));
      }
    }
    return projects;
  }

  /// Builds the GROUPING / GROUPING_ID value expression. The value is fully determined by the grouping set a
  /// row belongs to, so it is a plan-time constant per grouping-set ordinal: the expression is
  /// {@code CASE WHEN $groupingId = 0 THEN v_0 WHEN $groupingId = 1 THEN v_1 ... ELSE null END}, with each
  /// {@code v_k} packed from the argument columns' rolled-up bits in set {@code k} (first argument = most
  /// significant bit, PostgreSQL semantics, see {@link GroupingSets#groupingValuesByOrdinal}).
  ///
  /// The projection evaluates the CASE per output group row, so the per-row cost is O(number of grouping
  /// sets), bounded by {@link GroupingSets#MAX_GROUPING_SETS} (enforced in {@link #computeGroupingSets}).
  /// TODO: replace the CASE with an O(1) per-ordinal lookup (an INT-array literal indexed by $groupingId,
  ///   mirroring the single-stage reduce-side precompute) once array literals are supported in the plan
  ///   expression conversion.
  static RexNode buildGroupingValue(RexBuilder rexBuilder, RelDataType intType, RexNode groupingIdRef,
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
