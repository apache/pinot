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
package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MatchNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.RowPattern;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PlanNodeToOpChain;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.INPUT_SCHEMA;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.PID_INDEX;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.VALUE_INDEX;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.concat;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.labelSymbols;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.quantifier;
import static org.apache.pinot.query.runtime.operator.match.MatchTestFixtures.symbol;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Covers the input MATCH_RECOGNIZE reads at runtime: an explicit sort over a plain mailbox receive.
///
/// `PinotMatchExchangeNodeInsertRule` orders the input with a Sort above a hash exchange rather than by sorting on the
/// receive side, so the operator below [MatchOperator] is a [SortOperator] that hands rows over in bounded blocks. A
/// partition can therefore span several input blocks, and matching must not depend on where the blocks split.
public class MatchRecognizeSortInputTest {
  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    // The ports are never used: the plan is only converted into operators, not dispatched.
    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(3, 1, 2, QueryEnvironmentTestBase.TABLE_SCHEMAS,
        QueryEnvironmentTestBase.SERVER1_SEGMENTS, QueryEnvironmentTestBase.SERVER2_SEGMENTS, null);
  }

  @Test
  public void testPlannedMatchReadsFromFullSortOverPlainMailboxReceive() {
    String sql = "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts MEASURES FIRST(DOWN.ts) AS start_ts "
        + "PATTERN (DOWN+) DEFINE DOWN AS DOWN.col3 < PREV(DOWN.col3)) AS mr";
    DispatchablePlanFragment matchStage;
    try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(sql)) {
      matchStage = findMatchStage(compiledQuery.planQuery(0).getQueryPlan());
    }
    List<MatchNode> matchNodes = collectMatchNodes(matchStage.getPlanFragment().getFragmentRoot());
    assertEquals(matchNodes.size(), 1);
    MatchNode matchNode = matchNodes.get(0);

    // Plan nodes: MATCH <- Sort without fetch or offset <- receive that is hash distributed but does not sort.
    PlanNode matchInput = matchNode.getInputs().get(0);
    assertTrue(matchInput instanceof SortNode, "Expected a SortNode below the MatchNode, got: " + matchInput);
    SortNode sortNode = (SortNode) matchInput;
    assertEquals(sortNode.getFetch(), -1);
    assertEquals(sortNode.getOffset(), -1);
    // (col1, ts): the partition key first, so that partitions arrive contiguously.
    assertEquals(sortNode.getCollations().stream().map(RelFieldCollation::getFieldIndex).collect(Collectors.toList()),
        List.of(0, 7));
    for (RelFieldCollation collation : sortNode.getCollations()) {
      assertEquals(collation.getDirection(), RelFieldCollation.Direction.ASCENDING);
    }
    PlanNode sortInput = sortNode.getInputs().get(0);
    assertTrue(sortInput instanceof MailboxReceiveNode, "Expected a MailboxReceiveNode below the Sort: " + sortInput);
    MailboxReceiveNode receiveNode = (MailboxReceiveNode) sortInput;
    assertFalse(receiveNode.isSort(), "The receive must not sort, the Sort above it does");
    assertEquals(receiveNode.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
    assertEquals(receiveNode.getKeys(), List.of(0));
    // A Sort that orders each hash partition must not turn the stage into a singleton: with PARTITION BY the matching
    // work stays spread over every worker.
    assertTrue(matchStage.getWorkerMetadataList().size() > 1,
        "Expected a distributed MATCH_RECOGNIZE stage, got workers: " + matchStage.getWorkerMetadataList());

    // Operators built from those nodes.
    OpChain opChain = PlanNodeToOpChain.convert(matchNode, OperatorTestUtil.getNoTracingContext());
    try {
      MultiStageOperator root = opChain.getRoot();
      assertTrue(root instanceof MatchOperator, "Expected a MatchOperator, got: " + root);
      MultiStageOperator sortOperator = root.getChildOperators().get(0);
      assertTrue(sortOperator instanceof FullSortOperator, "Expected a FullSortOperator, got: " + sortOperator);
      MultiStageOperator receiveOperator = sortOperator.getChildOperators().get(0);
      assertEquals(receiveOperator.getClass(), MailboxReceiveOperator.class);
    } finally {
      opChain.close();
    }
  }

  @Test
  public void testPartitionsSpanningSortOutputBlocksMatchAsWhenDeliveredInOneBlock() {
    // Two partitions, shuffled across three input blocks the way a hash exchange interleaves its senders. In ORDER BY
    // order, partition 0 is A A B A B and partition 1 is A A A B.
    List<List<Object[]>> inputBlocks = List.of(
        List.of(new Object[]{1, "A", 2}, new Object[]{0, "B", 4}, new Object[]{0, "A", 0}),
        List.of(new Object[]{1, "B", 3}, new Object[]{0, "A", 3}, new Object[]{1, "A", 0}),
        List.of(new Object[]{0, "B", 2}, new Object[]{1, "A", 1}, new Object[]{0, "A", 1}));
    // PATTERN (A+ B) with AFTER MATCH SKIP PAST LAST ROW, MEASURES COUNT(*).
    List<List<Object>> expected = List.of(List.of(0, 3L), List.of(0, 2L), List.of(1, 4L));

    // Two rows per sort output block, so each partition is handed to MATCH_RECOGNIZE in pieces, and the boundary
    // between the two partitions falls inside a block.
    assertEquals(matchOverSort(inputBlocks, 2), expected);
    assertEquals(matchOverSort(inputBlocks, Integer.MAX_VALUE), expected);
  }

  private static List<List<Object>> matchOverSort(List<List<Object[]>> inputBlocks, int maxRowsPerSortBlock) {
    OpChainExecutionContext context = OperatorTestUtil.getNoTracingContext();
    BlockListMultiStageOperator.Builder builder = new BlockListMultiStageOperator.Builder(context, INPUT_SCHEMA);
    for (List<Object[]> block : inputBlocks) {
      for (Object[] row : block) {
        builder.addRow(row);
      }
      builder.finishBlock();
    }
    // The Sort the planner places below MATCH_RECOGNIZE: (partition key, ORDER BY key), no fetch, no offset.
    SortNode sortNode = new SortNode(-1, INPUT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(),
        List.of(new RelFieldCollation(PID_INDEX), new RelFieldCollation(VALUE_INDEX)), -1, -1);
    SortOperator sortOperator =
        SortOperator.create(context, builder.buildWithEos(), sortNode, 16, Integer.MAX_VALUE, maxRowsPerSortBlock);
    assertTrue(sortOperator instanceof FullSortOperator, "Expected a FullSortOperator, got: " + sortOperator);

    DataSchema resultSchema =
        new DataSchema(new String[]{"pid", "cnt"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<MatchNode.Measure> measures = List.of(
        new MatchNode.Measure("cnt", new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of())));
    RowPattern pattern = concat(quantifier(symbol(0), 1, RowPattern.Quantifier.UNBOUNDED, true), symbol(1));
    MatchNode matchNode = new MatchNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), labelSymbols("A", "B"),
        pattern, measures, List.of(PID_INDEX), List.of(), MatchNode.AfterMatchSkipMode.PAST_LAST_ROW,
        MatchNode.NO_SKIP_TO_SYMBOL, MatchNode.RowsPerMatchMode.ONE_ROW_PER_MATCH);
    MatchOperator matchOperator = new MatchOperator(context, sortOperator, INPUT_SCHEMA, matchNode);

    List<List<Object>> output = new ArrayList<>();
    MseBlock block = matchOperator.nextBlock();
    while (!block.isEos()) {
      for (Object[] row : ((MseBlock.Data) block).asRowHeap().getRows()) {
        output.add(Arrays.asList(row));
      }
      block = matchOperator.nextBlock();
    }
    assertFalse(((MseBlock.Eos) block).isError(), "Expecting a successful end of stream, got: " + block);
    return output;
  }

  private static DispatchablePlanFragment findMatchStage(DispatchableSubPlan subPlan) {
    for (DispatchablePlanFragment fragment : subPlan.getQueryStageMap().values()) {
      if (!collectMatchNodes(fragment.getPlanFragment().getFragmentRoot()).isEmpty()) {
        return fragment;
      }
    }
    throw new AssertionError("No stage contains a MatchNode");
  }

  /// Returns the MatchNodes of the stage rooted at `root`, without crossing into the stages it reads from.
  private static List<MatchNode> collectMatchNodes(PlanNode root) {
    List<MatchNode> found = new ArrayList<>();
    root.visit(new PlanNodeVisitor.DepthFirstVisitor<Void, Void>() {
      @Override
      public Void visitMatch(MatchNode node, Void context) {
        found.add(node);
        return super.visitMatch(node, context);
      }

      @Override
      protected boolean traverseStageBoundary() {
        return false;
      }
    }, null);
    return found;
  }
}
