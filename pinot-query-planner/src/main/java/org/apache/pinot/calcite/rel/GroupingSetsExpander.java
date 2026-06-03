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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;


/**
 * Expands a {@code GROUP BY ROLLUP / CUBE / GROUPING SETS} query (a {@link LogicalAggregate} with more than one
 * grouping set) into a {@code UNION ALL} of one ordinary aggregate per grouping set. The multi-stage engine has no
 * native grouping-set support, so this rewrite lets it execute only standard Union / Aggregate / Project plans.
 *
 * <p>For each grouping set {@code s}, a sub-aggregate groups by exactly the columns in {@code s}, and a project re-maps
 * its output to the original aggregate's row type: columns not in {@code s} become NULL, and {@code GROUPING(...)} /
 * {@code GROUPING_ID(...)} become the constant bitmask for {@code s} (one bit per argument, set when the argument is
 * rolled up, first argument most significant). Every branch shares the original aggregate's row type, so the union is
 * type-consistent and the rewrite is transparent to the rest of the plan.
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

  private static RelNode expand(LogicalAggregate aggregate) {
    RelNode input = aggregate.getInput();
    RelOptCluster cluster = aggregate.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    List<Integer> unionColumns = aggregate.getGroupSet().asList();
    int numGroupColumns = unionColumns.size();
    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    RelDataType rowType = aggregate.getRowType();
    List<String> fieldNames = rowType.getFieldNames();

    // Partition the aggregate calls into real aggregations and GROUPING / GROUPING_ID indicators.
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

    List<RelNode> branches = new ArrayList<>(aggregate.getGroupSets().size());
    for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
      // Re-infer the aggregate-call result types for this single grouping set: the original grouping-set aggregate may
      // have widened them to nullable, but a single-set sub-aggregate has its own (possibly NOT NULL) inference.
      List<AggregateCall> setAggCalls = new ArrayList<>(realAggCalls.size());
      int groupCount = groupingSet.cardinality();
      for (AggregateCall call : realAggCalls) {
        setAggCalls.add(AggregateCall.create(call.getAggregation(), call.isDistinct(), call.isApproximate(),
            call.ignoreNulls(), call.getArgList(), call.filterArg, call.distinctKeys, call.getCollation(), groupCount,
            input, null, call.getName()));
      }
      LogicalAggregate subAggregate =
          LogicalAggregate.create(input, aggregate.getHints(), groupingSet, null, setAggCalls);
      List<Integer> setColumns = groupingSet.asList();
      List<RexNode> projects = new ArrayList<>(rowType.getFieldCount());

      // Group-by output columns: a participating column references the sub-aggregate, a rolled-up column is NULL.
      for (int p = 0; p < numGroupColumns; p++) {
        RelDataType columnType = rowType.getFieldList().get(p).getType();
        int positionInSet = setColumns.indexOf(unionColumns.get(p));
        if (positionInSet >= 0) {
          // matchNullability=true: a grouping-set aggregate makes its group columns nullable, so widen the
          // sub-aggregate's (possibly NOT NULL) column to match the original (union) row type exactly.
          projects.add(rexBuilder.makeCast(columnType, rexBuilder.makeInputRef(subAggregate, positionInSet), true,
              false));
        } else {
          projects.add(rexBuilder.makeNullLiteral(columnType));
        }
      }

      // Aggregation output columns: a real aggregation references the sub-aggregate, an indicator is a constant.
      int realAggBase = setColumns.size();
      int realAggOffset = 0;
      for (int i = 0; i < aggCalls.size(); i++) {
        RelDataType resultType = rowType.getFieldList().get(numGroupColumns + i).getType();
        if (isGroupingCall[i]) {
          long value = groupingValue(aggCalls.get(i), groupingSet);
          projects.add(rexBuilder.makeCast(resultType, rexBuilder.makeExactLiteral(BigDecimal.valueOf(value)), true,
              false));
        } else {
          projects.add(rexBuilder.makeCast(resultType,
              rexBuilder.makeInputRef(subAggregate, realAggBase + realAggOffset), true, false));
          realAggOffset++;
        }
      }
      branches.add(LogicalProject.create(subAggregate, aggregate.getHints(), projects, fieldNames));
    }
    return LogicalUnion.create(branches, true);
  }

  /**
   * Computes the {@code GROUPING} / {@code GROUPING_ID} value for a grouping set: one bit per argument, set when the
   * argument column is rolled up (not present in the set), with the first argument as the most significant bit.
   */
  private static long groupingValue(AggregateCall call, ImmutableBitSet groupingSet) {
    // One bit per argument into a long, so at most 64 arguments (far beyond any real GROUPING_ID).
    long value = 0;
    for (int argColumn : call.getArgList()) {
      value = (value << 1) | (groupingSet.get(argColumn) ? 0 : 1);
    }
    return value;
  }
}
