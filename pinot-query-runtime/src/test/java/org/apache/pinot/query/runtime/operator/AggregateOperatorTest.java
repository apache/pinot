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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BOOLEAN;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class AggregateOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _input;
  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldHandleUpstreamErrorBlocks() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    when(_input.nextBlock()).thenReturn(ErrorMseBlock.fromException(new Exception("foo!")));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    MseBlock block = operator.nextBlock();

    // Then:
    verify(_input, times(1)).nextBlock();
    assertTrue(block.isError(), "Input errors should propagate immediately");
  }

  @Test
  public void shouldHandleEndOfStreamBlockWithNoOtherInputs() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    MseBlock block = operator.nextBlock();

    // Then:
    verify(_input, times(1)).nextBlock();
    assertTrue(block.isEos(), "EOS blocks should propagate");
  }

  @Test
  public void testAggregateSingleInputBlock() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 1.0},
        "Expected two columns (group by key, agg value), agg value is final result");
    assertTrue(operator.nextBlock().isSuccess(), "Second block is EOS (done processing)");
  }

  @Test
  public void testAggregateMultipleInputBlocks() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}, new Object[]{2, 2.0}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3.0}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_input.calculateStats()).thenReturn(MultiStageQueryStats.emptyStats(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 6.0},
        "Expected two columns (group by key, agg value), agg value is final result");
    assertTrue(operator.nextBlock().isSuccess(), "Second block is EOS (done processing)");
    MultiStageQueryStats stats = operator.calculateStats();
    StatMap<AggregateOperator.StatKey> statMap = OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, stats);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 1,
        "Num groups should equal the number of distinct group keys");
  }

  @Test
  public void testAggregateWithFilter() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls =
        List.of(getSum(new RexExpression.InputRef(1)), getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1, 2);
    List<Integer> groupKeys = List.of(0);
    DataSchema inSchema =
        new DataSchema(new String[]{"group", "arg", "filterArg"}, new ColumnDataType[]{INT, DOUBLE, BOOLEAN});
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inSchema, new Object[]{2, 1.0, 0}, new Object[]{2, 2.0, 1}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3.0, 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "sum", "sumWithFilter"}, new ColumnDataType[]{INT, DOUBLE, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 6.0, 5.0},
        "Expected three columns (group by key, agg value, agg value with filter), agg value is final result");
    assertTrue(operator.nextBlock().isSuccess(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFilteredAggregateWithNullValues() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls =
        List.of(getSum(new RexExpression.InputRef(1)), getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1, 2);
    List<Integer> groupKeys = List.of(0);
    DataSchema inSchema =
        new DataSchema(new String[]{"group", "arg", "filterArg"}, new ColumnDataType[]{INT, DOUBLE, BOOLEAN});
    // null for the filterArg should be treated as false
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inSchema, new Object[]{2, 1.0, null}, new Object[]{2, 2.0, 1}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3.0, 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "sum", "sumWithFilter"}, new ColumnDataType[]{INT, DOUBLE, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 6.0, 5.0},
        "Expected three columns (group by key, agg value, agg value with filter), agg value is final result");
    assertTrue(operator.nextBlock().isSuccess(), "Second block is EOS (done processing)");
  }

  @Test
  public void testGroupByAggregateWithHashCollision() {
    _input = OperatorTestUtil.getOperator(OperatorTestUtil.OP_1);

    // Create an aggregation call with sum for first column and group by second column.
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(0)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(1);
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{STRING, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    if (resultRows.get(0)[0].equals("Aa")) {
      assertEquals(resultRows.get(0), new Object[]{"Aa", 1.0});
      assertEquals(resultRows.get(1), new Object[]{"BB", 5.0});
    } else {
      assertEquals(resultRows.get(0), new Object[]{"BB", 5.0});
      assertEquals(resultRows.get(1), new Object[]{"Aa", 1.0});
    }
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*AVERAGE.*")
  public void shouldThrowOnUnknownAggFunction() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(INT, "AVERAGE", List.of()));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema resultSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});

    // When:
    getOperator(resultSchema, aggCalls, filterArgs, groupKeys);
  }

  @Test
  public void shouldReturnErrorBlockOnUnexpectedInputType() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock())
        // TODO: it is necessary to produce two values here, the operator only throws on second
        // (see the comment in Aggregate operator)
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}, new Object[]{2, "foo"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    DataSchema resultSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    MseBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isError(), "expected ERROR block from invalid computation");
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.UNKNOWN)
            .contains("cannot be cast to class"),
        "expected it to fail with class cast exception");
  }

  @Test
  public void shouldHandleGroupLimitExceed() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "1")));
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});

    _input = new BlockListMultiStageOperator.Builder(inSchema)
        .spied()
        .addRow(2, 1.0)
        .addRow(3, 2.0)
        .finishBlock()
        .addRow(3, 3.0)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    Map<String, String> opChainMetadata = new HashMap<>();
    opChainMetadata.put(QueryOptionKey.NUM_GROUPS_WARNING_LIMIT, "1");
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys, nodeHint, opChainMetadata);

    // When:
    MseBlock block1 = operator.nextBlock();
    MseBlock block2 = operator.nextBlock();

    // Then:
    verify(_input).earlyTerminate();
    assertEquals(((MseBlock.Data) block1).getNumRows(), 1,
        "when group limit reach it should only return that many groups");
    assertTrue(block2.isEos(), "Second block is EOS (done processing)");

    MultiStageQueryStats stats = operator.calculateStats();
    StatMap<AggregateOperator.StatKey> statMap = OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, stats);
    assertTrue(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED),
        "num groups limit should be reached");
    assertTrue(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_WARNING_LIMIT_REACHED),
        "num groups warning limit should be reached");
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 1,
        "Num groups should equal the limit since only one group was accepted");
  }

  @Test
  public void testDefaultGroupTrimSize() {
    OpChainExecutionContext context = OperatorTestUtil.getTracingContext();

    assertEquals(getAggregateOperator(context, null, 0, null).getGroupTrimSize(), Integer.MAX_VALUE);
    assertEquals(getAggregateOperator(context, null, 10, null).getGroupTrimSize(), 10);

    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1));
    assertEquals(getAggregateOperator(context, null, 0, collations).getGroupTrimSize(), Integer.MAX_VALUE);
    assertEquals(getAggregateOperator(context, null, 10, collations).getGroupTrimSize(),
        Server.DEFAULT_MSE_MIN_GROUP_TRIM_SIZE);
  }

  @Test
  public void testGroupTrimSizeDependsOnContextValue() {
    OpChainExecutionContext context =
        OperatorTestUtil.getContext(Map.of(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, "100"));
    assertEquals(getAggregateOperator(context, null, 5, List.of(new RelFieldCollation(1))).getGroupTrimSize(), 100);
  }

  @Test
  public void testGroupTrimHintOverridesContextValue() {
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.MSE_MIN_GROUP_TRIM_SIZE, "30")));
    OpChainExecutionContext context =
        OperatorTestUtil.getContext(Map.of(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, "100"));
    assertEquals(getAggregateOperator(context, nodeHint, 5, List.of(new RelFieldCollation(1))).getGroupTrimSize(), 30);
  }

  @DataProvider
  public Object[][] orderedGroupTrimCases() {
    return new Object[][]{
        {"COUNT", null, AggType.LEAF},
        {"COUNT", null, AggType.INTERMEDIATE},
        {"DISTINCTCOUNT", null, AggType.LEAF},
        {"DISTINCTCOUNT", null, AggType.INTERMEDIATE},
        {"DISTINCTCOUNTHLL", null, AggType.LEAF},
        {"DISTINCTCOUNTHLL", null, AggType.INTERMEDIATE},
        {"DISTINCTCOUNTSMARTHLL", null, AggType.LEAF},
        {"DISTINCTCOUNTSMARTHLL", null, AggType.INTERMEDIATE},
        {"DISTINCTCOUNTSMARTHLL", "threshold=1", AggType.LEAF},
        {"DISTINCTCOUNTSMARTHLL", "threshold=1", AggType.INTERMEDIATE}
    };
  }

  @Test(dataProvider = "orderedGroupTrimCases")
  public void testOrderedGroupTrimPreservesIntermediateResults(String functionName, @Nullable String parameters,
      AggType trimmedStage) {
    OpChainExecutionContext context =
        OperatorTestUtil.getContext(Map.of(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, "1"));
    ColumnDataType finalType = functionName.equals("DISTINCTCOUNTHLL") || functionName.equals("COUNT")
        ? ColumnDataType.LONG : INT;
    List<RexExpression> operands = parameters == null ? List.of(new RexExpression.InputRef(1))
        : List.of(new RexExpression.InputRef(1), new RexExpression.Literal(STRING, parameters));
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(finalType, functionName, operands));
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING), new RelFieldCollation(0));
    DataSchema inputSchema = new DataSchema(new String[]{"group", "value"}, new ColumnDataType[]{INT, INT});
    DataSchema intermediateSchema = new DataSchema(new String[]{"group", "distinctCount"},
        new ColumnDataType[]{INT, functionName.equals("COUNT") ? ColumnDataType.LONG : ColumnDataType.OBJECT});
    BlockListMultiStageOperator.Builder partials = new BlockListMultiStageOperator.Builder(context, intermediateSchema);
    // Each leaf has six groups with cardinalities 1..6. Disjoint partials must merge to cardinalities 2..12.
    for (int partition = 0; partition < 2; partition++) {
      BlockListMultiStageOperator.Builder input = new BlockListMultiStageOperator.Builder(context, inputSchema);
      for (int group = 1; group <= 6; group++) {
        for (int value = 0; value < group; value++) {
          input.addRow(group, partition * 10 + value);
        }
      }
      AggregateOperator leaf = new AggregateOperator(context, input.buildWithEos(),
          new AggregateNode(-1, intermediateSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1),
              List.of(0), AggType.LEAF, false, collations, trimmedStage == AggType.LEAF ? 1 : 0));
      partials.addBlock(leaf.nextBlock());
      assertTrue(leaf.nextBlock().isSuccess());
    }
    AggregateOperator intermediate = new AggregateOperator(context, partials.buildWithEos(),
        new AggregateNode(-1, intermediateSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1), List.of(0),
            AggType.INTERMEDIATE, false, collations, trimmedStage == AggType.INTERMEDIATE ? 1 : 0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "distinctCount"}, new ColumnDataType[]{INT, finalType});
    AggregateOperator result = new AggregateOperator(context, intermediate,
        new AggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1), List.of(0),
            AggType.FINAL, false, null, 0));

    List<Object[]> rows = ((MseBlock.Data) result.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 5);
    Map<Integer, Long> counts = new HashMap<>();
    for (Object[] row : rows) {
      counts.put((Integer) row[0], ((Number) row[1]).longValue());
    }
    assertEquals(counts, Map.of(2, 4L, 3, 6L, 4, 8L, 5, 10L, 6, 12L));
    assertTrue(result.nextBlock().isSuccess());
  }

  @Test
  public void testOrderedGroupTrimPreservesFunnelEvents() {
    OpChainExecutionContext context =
        OperatorTestUtil.getContext(Map.of(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, "1"));
    DataSchema inputSchema = new DataSchema(new String[]{"group", "timestamp", "step"},
        new ColumnDataType[]{INT, ColumnDataType.LONG, BOOLEAN});
    List<RexExpression.FunctionCall> aggCalls = List.of(new RexExpression.FunctionCall(INT, "FUNNELMAXSTEP",
        List.of(new RexExpression.InputRef(1), new RexExpression.Literal(ColumnDataType.LONG, 1000L),
            new RexExpression.Literal(INT, 1), new RexExpression.InputRef(2))));
    DataSchema intermediateSchema =
        new DataSchema(new String[]{"group", "funnel"}, new ColumnDataType[]{INT, ColumnDataType.OBJECT});
    AggregateOperator leaf = new AggregateOperator(context,
        new BlockListMultiStageOperator(context,
            OperatorTestUtil.block(inputSchema, new Object[]{1, 1L, 1}, new Object[]{2, 2L, 1})),
        new AggregateNode(-1, intermediateSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1), List.of(0),
            AggType.LEAF, false, List.of(new RelFieldCollation(1)), 1));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "funnel"}, new ColumnDataType[]{INT, INT});
    AggregateOperator result = new AggregateOperator(context, leaf,
        new AggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1), List.of(0),
            AggType.FINAL, false, null, 0));

    List<Object[]> rows = ((MseBlock.Data) result.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.stream().map(row -> row[0]).sorted().toList(), List.of(1, 2));
    rows.forEach(row -> assertEquals(row[1], 1));
    assertTrue(result.nextBlock().isSuccess());
  }

  @Test
  public void testOrderedRawHllGroupTrimMatchesDirectResults() {
    OpChainExecutionContext context =
        OperatorTestUtil.getContext(Map.of(QueryOptionKey.MSE_MIN_GROUP_TRIM_SIZE, "1"));
    DataSchema inputSchema = new DataSchema(new String[]{"group", "value"}, new ColumnDataType[]{INT, INT});
    MseBlock.Data input = OperatorTestUtil.block(inputSchema, new Object[]{1, 1}, new Object[]{2, 2},
        new Object[]{3, 3}, new Object[]{4, 4}, new Object[]{5, 5}, new Object[]{6, 6});
    List<RexExpression.FunctionCall> aggCalls = List.of(new RexExpression.FunctionCall(STRING, "DISTINCTCOUNTRAWHLL",
        List.of(new RexExpression.InputRef(1), new RexExpression.Literal(INT, 4))));
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING), new RelFieldCollation(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "hll"}, new ColumnDataType[]{INT, STRING});
    AggregateOperator direct = new AggregateOperator(context, new BlockListMultiStageOperator(context, input),
        new AggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1), List.of(0),
            AggType.DIRECT, false, collations, 1));
    DataSchema intermediateSchema =
        new DataSchema(new String[]{"group", "hll"}, new ColumnDataType[]{INT, ColumnDataType.OBJECT});
    AggregateOperator leaf = new AggregateOperator(context, new BlockListMultiStageOperator(context, input),
        new AggregateNode(-1, intermediateSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1), List.of(0),
            AggType.LEAF, false, collations, 1));
    AggregateOperator result = new AggregateOperator(context, leaf,
        new AggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, List.of(-1), List.of(0),
            AggType.FINAL, false, null, 0));

    List<Object[]> expected = ((MseBlock.Data) direct.nextBlock()).asRowHeap().getRows();
    List<Object[]> actual = ((MseBlock.Data) result.nextBlock()).asRowHeap().getRows();
    assertEquals(expected.size(), 5);
    // Equal cardinalities would select groups 1..5; stored STRING ordering must retain group 6 instead of group 2.
    assertTrue(expected.stream().anyMatch(row -> row[0].equals(6)));
    assertEqualsNoOrder(actual.stream().map(row -> List.of(row)).toArray(),
        expected.stream().map(row -> List.of(row)).toArray());
    assertTrue(result.nextBlock().isSuccess());
  }

  private AggregateOperator getAggregateOperator(OpChainExecutionContext context, PlanNode.NodeHint nodeHint, int limit,
      @Nullable List<RelFieldCollation> collations) {
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    return new AggregateOperator(context, _input,
        new AggregateNode(-1, resultSchema, nodeHint, List.of(), aggCalls, filterArgs, groupKeys, AggType.DIRECT, false,
            collations, limit));
  }

  @Test
  public void shouldRecordNumGroupsBelowLimit() {
    // Given: 1 distinct group key, limit = 2 — below limit, no overflow
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "2")));
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});

    _input = new BlockListMultiStageOperator.Builder(inSchema)
        .addRow(2, 1.0)
        .addRow(2, 2.0)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys, nodeHint, Map.of());

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertTrue(operator.nextBlock().isEos());
    MultiStageQueryStats stats = operator.calculateStats();
    StatMap<AggregateOperator.StatKey> statMap = OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, stats);
    assertFalse(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED),
        "Num groups limit should not be reached when groups are below limit");
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 1,
        "Num groups should equal 1");
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.SUM.name(), List.of(arg));
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys, PlanNode.NodeHint nodeHint,
      Map<String, String> opChainMetadata) {
    return new AggregateOperator(OperatorTestUtil.getContext(opChainMetadata), _input,
        new AggregateNode(-1, resultSchema, nodeHint, List.of(), aggCalls, filterArgs, groupKeys, AggType.DIRECT,
            false, null, 0));
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys) {
    return getOperator(resultSchema, aggCalls, filterArgs, groupKeys, PlanNode.NodeHint.EMPTY, Map.of());
  }
}
