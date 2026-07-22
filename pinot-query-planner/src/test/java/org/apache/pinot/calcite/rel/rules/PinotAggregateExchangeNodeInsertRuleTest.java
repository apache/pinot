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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.query.type.TypeFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Unit tests for {@link PinotAggregateExchangeNodeInsertRule}, focused on the skip-leaf-stage
/// direct-aggregation path.
public class PinotAggregateExchangeNodeInsertRuleTest {
  private static final TypeFactory TYPE_FACTORY = TypeFactory.INSTANCE;
  private static final RexBuilder REX_BUILDER = RexBuilder.DEFAULT;

  private static AggregateCall count(String name) {
    return AggregateCall.create(SqlStdOperatorTable.COUNT, false, List.of(1), -1,
        TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), name);
  }

  /// Builds `Aggregate[hint=is_skip_leaf_stage_group_by]( <count()>*n ) over a two-column Values`,
  /// runs the WithoutSort rule, and returns the (unwrapped) transformed root.
  private static RelNode runSkipLeaf(int numCountCalls) {
    HepProgramBuilder program = new HepProgramBuilder();
    program.addRuleInstance(PinotAggregateExchangeNodeInsertRule.WithoutSort.INSTANCE);
    HepPlanner planner = new HepPlanner(program.build());
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
    cluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    cluster.setHintStrategies(HintStrategyTable.builder()
        .hintStrategy(PinotHintOptions.AGGREGATE_HINT_OPTIONS, HintPredicates.AGGREGATE)
        .build());
    RelDataType rowType = TYPE_FACTORY.builder()
        .add("g", SqlTypeName.INTEGER)
        .add("v", SqlTypeName.INTEGER)
        .build();
    LogicalValues input = new LogicalValues(cluster, RelTraitSet.createEmpty(), rowType, ImmutableList.of());
    List<AggregateCall> aggCalls = new ArrayList<>();
    for (int i = 0; i < numCountCalls; i++) {
      aggCalls.add(count("c" + i));
    }
    LogicalAggregate aggregate = (LogicalAggregate) LogicalAggregate.create(
            input, ImmutableBitSet.of(0), List.of(ImmutableBitSet.of(0)), aggCalls)
        .withHints(List.of(RelHint.builder(PinotHintOptions.AGGREGATE_HINT_OPTIONS)
            .hintOption(PinotHintOptions.AggregateOptions.IS_SKIP_LEAF_STAGE_GROUP_BY, "true")
            .build()));
    planner.setRoot(aggregate);
    return unwrap(planner.findBestExp());
  }

  private static RelNode unwrap(RelNode node) {
    return node instanceof HepRelVertex ? ((HepRelVertex) node).getCurrentRel() : node;
  }

  /// Regression: two identical `COUNT` calls make `generateProjectUnderAggregate` deduplicate them
  /// and return a `Project`-over-`Aggregate` (the top `Project` re-references the deduplicated column
  /// to restore the original layout). The rule used to cast that result to `Aggregate` and throw
  /// `LogicalProject cannot be cast to Aggregate`. It must now preserve the `Project` over a direct
  /// aggregate.
  @Test
  public void skipLeafWithDuplicateAggregateCallsPreservesLayoutProject() {
    RelNode result = runSkipLeaf(2);
    assertTrue(result instanceof LogicalProject,
        "expected a layout-restoring Project at the root, got " + result.getClass().getSimpleName());
    RelNode child = unwrap(result.getInput(0));
    assertTrue(child instanceof PinotLogicalAggregate,
        "expected a PinotLogicalAggregate under the Project, got " + child.getClass().getSimpleName());
    // group key + the two (deduplicated then re-expanded) count columns.
    assertEquals(result.getRowType().getFieldCount(), 3);
  }

  /// The common case (no duplicate agg calls) still produces a bare direct aggregate — no extra
  /// Project.
  @Test
  public void skipLeafWithoutDuplicatesProducesDirectAggregate() {
    RelNode result = runSkipLeaf(1);
    assertTrue(result instanceof PinotLogicalAggregate,
        "expected a PinotLogicalAggregate, got " + result.getClass().getSimpleName());
  }
}
