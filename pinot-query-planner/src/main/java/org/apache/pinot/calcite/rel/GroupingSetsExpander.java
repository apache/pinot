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
package org.apache.pinot.calcite.rel;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.logical.PinotLogicalGroupingSetsExpand;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * Lowers a {@code GROUP BY ROLLUP / CUBE / GROUPING SETS} query (a {@link LogicalAggregate} with more than one grouping
 * set) into the native multi-stage expansion plan, so the multi-stage engine computes every set in a single pass:
 *
 * <pre>
 *   LogicalProject  [groupCols..., aggResults...]                  -- restore row type; GROUPING()/GROUPING_ID()
 *    └─ LogicalAggregate  groupBy [unionGroupKeys..., $groupingId] -- ordinary aggregate; the standard
 *        │                                                            PinotAggregateExchangeNodeInsertRule then splits
 *        │                                                            it into LEAF / hash-exchange / FINAL
 *        └─ PinotLogicalGroupingSetsExpand                         -- one row per input row per grouping set, nulling
 *            └─ input                                                 rolled-up group keys and appending $groupingId
 * </pre>
 *
 * <p>The expand sits directly above the input (no exchange below it), so it stays in the leaf stage and the per-row
 * fan-out is reduced by the LEAF aggregate before anything crosses the network. {@code $groupingId} is part of the
 * aggregate's group key, so the inserted hash exchange is partitioned on {@code [unionGroupKeys..., $groupingId]} and
 * genuine NULL groups stay distinct from rolled-up NULL groups. The discriminator follows the
 * {@link org.apache.pinot.common.request.context.GroupingSets} convention (bit {@code p} set iff union position
 * {@code p} is rolled up), so GROUPING()/GROUPING_ID() can be derived in the top project.
 */
public class GroupingSetsExpander extends RelShuttleImpl {

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    // Expand children first (handles nested grouping-set aggregates bottom-up).
    RelNode visited = super.visit(aggregate);
    if (!(visited instanceof LogicalAggregate)) {
      return visited;
    }
    LogicalAggregate agg = (LogicalAggregate) visited;
    if (agg.getGroupSets().size() <= 1) {
      // Ordinary GROUP BY (single grouping set) - leave untouched.
      return agg;
    }
    return expand(agg);
  }

  /**
   * Returns true if any aggregate in the tree has more than one grouping set (i.e. {@code GROUP BY ROLLUP / CUBE /
   * GROUPING SETS}). Used to reject grouping sets on the v2 physical-optimizer path, which cannot convert the native
   * expand rel.
   */
  public static boolean hasGroupingSets(RelNode rel) {
    if (rel instanceof Aggregate && ((Aggregate) rel).getGroupSets().size() > 1) {
      return true;
    }
    for (RelNode input : rel.getInputs()) {
      if (hasGroupingSets(input)) {
        return true;
      }
    }
    return false;
  }

  private static RelNode expand(LogicalAggregate aggregate) {
    RelNode input = aggregate.getInput();
    RelOptCluster cluster = aggregate.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    List<Integer> unionColumns = aggregate.getGroupSet().asList();
    int numGroupColumns = unionColumns.size();
    if (numGroupColumns > CalciteSqlParser.MAX_GROUPING_SETS_COLUMNS) {
      // The per-set discriminator is a 32-bit int, so it cannot represent more than MAX_GROUPING_SETS_COLUMNS union
      // group-by columns. The single-stage path enforces the same cap in CalciteSqlParser.
      throw new IllegalStateException(String.format(
          "GROUP BY GROUPING SETS / ROLLUP / CUBE supports at most %d grouping columns, got %d",
          CalciteSqlParser.MAX_GROUPING_SETS_COLUMNS, numGroupColumns));
    }
    if (aggregate.getGroupSets().size() > CalciteSqlParser.MAX_GROUPING_SETS) {
      // Mirror the single-stage cap on the number of grouping sets so e.g. CUBE over many columns cannot expand
      // without bound; the single-stage path enforces the same cap in CalciteSqlParser.
      throw new IllegalStateException(String.format(
          "GROUP BY GROUPING SETS / ROLLUP / CUBE expands to more than %d grouping sets",
          CalciteSqlParser.MAX_GROUPING_SETS));
    }
    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    RelDataType rowType = aggregate.getRowType();
    List<String> fieldNames = rowType.getFieldNames();
    // The expand appends $groupingId after the input columns, so it sits at the input's field count.
    int groupingIdIndex = input.getRowType().getFieldCount();

    // Partition the aggregate calls into real (mergeable) aggregations and GROUPING / GROUPING_ID indicators. The
    // indicators are not computed by the aggregate; they are derived in the top project from $groupingId.
    List<AggregateCall> realAggCalls = new ArrayList<>(aggCalls.size());
    boolean[] isGroupingCall = new boolean[aggCalls.size()];
    for (int i = 0; i < aggCalls.size(); i++) {
      SqlKind kind = aggCalls.get(i).getAggregation().getKind();
      if (kind == SqlKind.GROUPING || kind == SqlKind.GROUPING_ID) {
        isGroupingCall[i] = true;
      } else {
        realAggCalls.add(aggCalls.get(i));
      }
    }

    // 1. Expand: one row per input row per grouping set; $groupingId carries the per-set rolled-up bitmask.
    List<Integer> groupingIds = computeGroupingIds(aggregate.getGroupSets(), unionColumns);
    PinotLogicalGroupingSetsExpand expand = PinotLogicalGroupingSetsExpand.create(input, unionColumns, groupingIds);

    // 2. Ordinary aggregate keyed on [unionColumns, $groupingId]. The expand keeps the input columns in place, so the
    //    aggregate-call argument / filter indexes still reference the same columns and need no remapping. Re-infer each
    //    call's result type for the single-grouping-set context (it may have been widened to nullable originally).
    ImmutableBitSet newGroupSet = aggregate.getGroupSet().rebuild().set(groupingIdIndex).build();
    int newGroupCount = newGroupSet.cardinality();
    List<AggregateCall> newAggCalls = new ArrayList<>(realAggCalls.size());
    for (AggregateCall call : realAggCalls) {
      newAggCalls.add(AggregateCall.create(call.getAggregation(), call.isDistinct(), call.isApproximate(),
          call.ignoreNulls(), call.getArgList(), call.filterArg, call.distinctKeys, call.getCollation(), newGroupCount,
          expand, null, call.getName()));
    }
    LogicalAggregate newAggregate =
        LogicalAggregate.create(expand, aggregate.getHints(), newGroupSet, null, newAggCalls);

    // 3. Top project: restore the original row type. newAggregate output is [unionColumns (numGroupColumns),
    //    $groupingId (at numGroupColumns), realAggs...]. Group columns and real aggregates reference newAggregate (cast
    //    to the original type so nullability matches); GROUPING()/GROUPING_ID() come from $groupingId.
    List<RexNode> projects = new ArrayList<>(rowType.getFieldCount());
    for (int p = 0; p < numGroupColumns; p++) {
      RelDataType columnType = rowType.getFieldList().get(p).getType();
      projects.add(rexBuilder.makeCast(columnType, rexBuilder.makeInputRef(newAggregate, p), true, false));
    }
    RexNode groupingId = rexBuilder.makeInputRef(newAggregate, numGroupColumns);
    int realAggOffset = 0;
    for (int i = 0; i < aggCalls.size(); i++) {
      RelDataType resultType = rowType.getFieldList().get(numGroupColumns + i).getType();
      if (isGroupingCall[i]) {
        projects.add(groupingValueFromId(rexBuilder, groupingId, aggCalls.get(i).getArgList(), unionColumns,
            resultType));
      } else {
        projects.add(rexBuilder.makeCast(resultType,
            rexBuilder.makeInputRef(newAggregate, numGroupColumns + 1 + realAggOffset), true, false));
        realAggOffset++;
      }
    }
    return LogicalProject.create(newAggregate, List.of(), projects, fieldNames);
  }

  /**
   * Computes the per-set {@code $groupingId} discriminator for each grouping set: bit {@code p} (p = the union-column
   * position in {@code unionColumns}) is set iff that union column is rolled up (absent) in the set, matching
   * {@link org.apache.pinot.common.request.context.GroupingSets#groupingValue}'s "1 = aggregated away" convention.
   */
  private static List<Integer> computeGroupingIds(List<ImmutableBitSet> groupingSets, List<Integer> unionColumns) {
    int numGroupColumns = unionColumns.size();
    List<Integer> groupingIds = new ArrayList<>(groupingSets.size());
    for (ImmutableBitSet set : groupingSets) {
      int groupingId = 0;
      for (int p = 0; p < numGroupColumns; p++) {
        if (!set.get(unionColumns.get(p))) {
          groupingId |= 1 << p;
        }
      }
      groupingIds.add(groupingId);
    }
    return groupingIds;
  }

  /**
   * Computes a {@code GROUPING(args) / GROUPING_ID(args)} value from the {@code $groupingId} discriminator, matching
   * {@code GroupingSets.groupingValue}: one bit per argument packed with the first argument most significant, where the
   * bit is 1 iff that argument column is rolled up. Bit {@code p} of {@code $groupingId} (p = the arg position in
   * the union group columns) is {@code MOD(FLOOR(groupingId / 2^p), 2)}; the bits are folded as
   * {@code value = value * 2 + bit}. Uses only standard arithmetic operators so no Pinot scalar resolution is needed.
   */
  private static RexNode groupingValueFromId(RexBuilder rexBuilder, RexNode groupingId, List<Integer> argList,
      List<Integer> unionColumns, RelDataType resultType) {
    RexNode two = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));
    RexNode value = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
    for (int arg : argList) {
      int bitIndex = unionColumns.indexOf(arg);
      RexNode divisor = rexBuilder.makeExactLiteral(BigDecimal.valueOf(1L << bitIndex));
      RexNode bit = rexBuilder.makeCall(SqlStdOperatorTable.MOD,
          rexBuilder.makeCall(SqlStdOperatorTable.FLOOR,
              rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, groupingId, divisor)), two);
      value = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
          rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, value, two), bit);
    }
    return rexBuilder.makeCast(resultType, value, true, false);
  }
}
