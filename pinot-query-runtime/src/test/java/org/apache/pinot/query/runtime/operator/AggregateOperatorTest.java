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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BOOLEAN;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BYTES;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.OBJECT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.UUID;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
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
  public void testGroupBySpillProducesExactResultsAndCleansUp() {
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .addRow(2, 2.0)
        .finishBlock()
        .addRow(1, 3.0)
        .addRow(3, 4.0)
        .finishBlock()
        .addRow(2, 5.0)
        .addRow(4, 6.0)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys, PlanNode.NodeHint.EMPTY,
        spillOptions(2, 2));

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    Path spillDirectory = operator.getSpillDirectory();
    assertNotNull(spillDirectory);
    assertTrue(Files.exists(spillDirectory), "Spill directory should exist while partition results are produced");
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }

    resultRows.sort((left, right) -> Integer.compare((int) left[0], (int) right[0]));
    assertEquals(resultRows.size(), 4);
    assertEquals(resultRows.get(0), new Object[]{1, 4.0});
    assertEquals(resultRows.get(1), new Object[]{2, 7.0});
    assertEquals(resultRows.get(2), new Object[]{3, 4.0});
    assertEquals(resultRows.get(3), new Object[]{4, 6.0});
    assertTrue(block.isSuccess());
    assertFalse(Files.exists(spillDirectory), "Spill directory should be deleted after the operator reaches EOS");

    StatMap<AggregateOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, operator.calculateStats());
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 4);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.SPILL_COUNT), 3);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.SPILLED_ROWS), 6);
    assertTrue(statMap.getLong(AggregateOperator.StatKey.SPILLED_BYTES) > 0);
  }

  @Test
  public void testGroupBySpillRoundTripsObjectIntermediateResults() {
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(DOUBLE, SqlKind.AVG.name(), List.of(new RexExpression.InputRef(1))));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .addRow(2, 10.0)
        .finishBlock()
        .addRow(1, 3.0)
        .addRow(2, 20.0)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"group", "avg"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys, PlanNode.NodeHint.EMPTY,
        spillOptions(1, 2));

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }

    resultRows.sort((left, right) -> Integer.compare((int) left[0], (int) right[0]));
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, 2.0});
    assertEquals(resultRows.get(1), new Object[]{2, 15.0});
    assertTrue(block.isSuccess());
  }

  @Test
  public void testGroupBySpillWritesMultipleBoundedRecords() {
    DataSchema inputSchema = new DataSchema(new String[]{"group"}, new ColumnDataType[]{INT});
    BlockListMultiStageOperator.Builder inputBuilder = new BlockListMultiStageOperator.Builder(inputSchema);
    int numGroups = 1025;
    for (int group = 0; group < numGroups; group++) {
      inputBuilder.addRow(group);
    }
    _input = inputBuilder.buildWithEos();
    AggregateOperator operator =
        getOperator(inputSchema, List.of(), List.of(), List.of(0), PlanNode.NodeHint.EMPTY,
            spillOptions(numGroups, 1));

    assertEquals(drainRows(operator).size(), numGroups);
    StatMap<AggregateOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, operator.calculateStats());
    assertEquals(statMap.getLong(AggregateOperator.StatKey.SPILL_COUNT), 1);
    assertEquals(statMap.getLong(AggregateOperator.StatKey.SPILLED_ROWS), numGroups);
  }

  @Test
  public void testDistinctGroupBySpill() {
    DataSchema inputSchema = new DataSchema(new String[]{"group"}, new ColumnDataType[]{INT});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1)
        .finishBlock()
        .addRow(2)
        .finishBlock()
        .addRow(1)
        .buildWithEos();
    AggregateOperator operator =
        getOperator(inputSchema, List.of(), List.of(), List.of(0), PlanNode.NodeHint.EMPTY,
            spillOptions(1, Server.DEFAULT_MSE_AGGREGATION_SPILL_PARTITIONS));

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }

    resultRows.sort((left, right) -> Integer.compare((int) left[0], (int) right[0]));
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertTrue(block.isSuccess());
  }

  @Test
  public void testGroupBySpillSupportsCompositeNullKeysAndFilters() {
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(2)));
    DataSchema inputSchema = new DataSchema(new String[]{"group", "subgroup", "arg", "filter"},
        new ColumnDataType[]{INT, STRING, DOUBLE, BOOLEAN});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, null, 1.0, 1)
        .addRow(1, "x", 10.0, 1)
        .finishBlock()
        .addRow(1, null, 2.0, 0)
        .addRow(1, "x", 5.0, 1)
        .buildWithEos();
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "subgroup", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    AggregateOperator operator =
        getOperator(resultSchema, aggCalls, List.of(3), List.of(0, 1), PlanNode.NodeHint.EMPTY,
            spillOptions(1, Server.MAX_MSE_AGGREGATION_SPILL_PARTITIONS));

    List<Object[]> resultRows = drainRows(operator);
    resultRows.sort((left, right) -> {
      String leftKey = (String) left[1];
      String rightKey = (String) right[1];
      if (leftKey == null) {
        return rightKey == null ? 0 : -1;
      }
      return rightKey == null ? 1 : leftKey.compareTo(rightKey);
    });

    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, null, 1.0});
    assertEquals(resultRows.get(1), new Object[]{1, "x", 15.0});
  }

  @Test
  public void testSpillPreservesIntermediateAndFinalAggregationModes() {
    RexExpression.FunctionCall avgCall =
        new RexExpression.FunctionCall(DOUBLE, SqlKind.AVG.name(), List.of(new RexExpression.InputRef(1)));
    Map<String, String> spillOptions = spillOptions(1, 2);
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .addRow(2, 10.0)
        .finishBlock()
        .addRow(1, 3.0)
        .addRow(2, 20.0)
        .buildWithEos();
    DataSchema intermediateSchema =
        new DataSchema(new String[]{"group", "avg"}, new ColumnDataType[]{INT, OBJECT});

    AggregateOperator leafOperator =
        getOperator(intermediateSchema, List.of(avgCall), List.of(-1), List.of(0), AggType.LEAF, spillOptions);
    _input = new BlockListMultiStageOperator(OperatorTestUtil.getTracingContext(), drainDataBlocks(leafOperator));
    AggregateOperator intermediateOperator =
        getOperator(intermediateSchema, List.of(avgCall), List.of(-1), List.of(0), AggType.INTERMEDIATE, spillOptions);
    _input =
        new BlockListMultiStageOperator(OperatorTestUtil.getTracingContext(), drainDataBlocks(intermediateOperator));
    DataSchema finalSchema = new DataSchema(new String[]{"group", "avg"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator finalOperator =
        getOperator(finalSchema, List.of(avgCall), List.of(-1), List.of(0), AggType.FINAL, spillOptions);

    List<Object[]> resultRows = drainRows(finalOperator);
    resultRows.sort((left, right) -> Integer.compare((int) left[0], (int) right[0]));
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, 2.0});
    assertEquals(resultRows.get(1), new Object[]{2, 15.0});
  }

  @Test
  public void testSpillEnforcesGlobalNumGroupsLimit() {
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .finishBlock()
        .addRow(2, 2.0)
        .finishBlock()
        .addRow(3, 3.0)
        .buildWithEos();
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "2")));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        getOperator(resultSchema, List.of(getSum(new RexExpression.InputRef(1))), List.of(-1), List.of(0), nodeHint,
            spillOptions(1, Server.DEFAULT_MSE_AGGREGATION_SPILL_PARTITIONS));

    assertEquals(drainRows(operator).size(), 2);
    StatMap<AggregateOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, operator.calculateStats());
    assertTrue(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED));
    assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 2);
  }

  @Test
  public void testSpillErrorsOnGlobalNumGroupsLimit() {
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .finishBlock()
        .addRow(2, 2.0)
        .buildWithEos();
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "2")));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        getOperator(resultSchema, List.of(getSum(new RexExpression.InputRef(1))), List.of(-1), List.of(0), nodeHint,
            Map.of(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED, "true",
                QueryOptionKey.MSE_AGGREGATION_SPILL_THRESHOLD, "1",
                QueryOptionKey.MSE_AGGREGATION_SPILL_PARTITIONS, "2",
                QueryOptionKey.ERROR_ON_NUM_GROUPS_LIMIT, "true"));

    MseBlock block = operator.nextBlock();
    Path spillDirectory = operator.getSpillDirectory();
    while (block.isData()) {
      block = operator.nextBlock();
    }
    assertTrue(block.isError());
    assertNotNull(spillDirectory);
    assertFalse(Files.exists(spillDirectory));
  }

  @Test
  public void testSpillErrorsWhenRestoredPartitionExceedsThreshold() {
    DataSchema inputSchema = new DataSchema(new String[]{"group"}, new ColumnDataType[]{INT});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1)
        .addRow(2)
        .addRow(3)
        .buildWithEos();
    AggregateOperator operator =
        getOperator(inputSchema, List.of(), List.of(), List.of(0), PlanNode.NodeHint.EMPTY, spillOptions(2, 1));

    MseBlock block = operator.nextBlock();
    Path spillDirectory = operator.getSpillDirectory();

    assertTrue(block.isError());
    assertNotNull(spillDirectory);
    assertFalse(Files.exists(spillDirectory));
  }

  @DataProvider(name = "spillCleanupActions")
  public Object[][] spillCleanupActions() {
    return new Object[][]{{"close"}, {"cancel"}, {"earlyTerminate"}};
  }

  @Test(dataProvider = "spillCleanupActions")
  public void testSpillCleanupOnTerminalAction(String action) {
    AggregateOperator operator = createSpillingSumOperator(false);
    assertTrue(operator.nextBlock().isData());
    Path spillDirectory = operator.getSpillDirectory();
    assertNotNull(spillDirectory);
    assertTrue(Files.exists(spillDirectory));

    switch (action) {
      case "close":
        operator.close();
        break;
      case "cancel":
        operator.cancel(new RuntimeException("cancelled"));
        break;
      case "earlyTerminate":
        operator.earlyTerminate();
        break;
      default:
        throw new IllegalArgumentException("Unsupported cleanup action: " + action);
    }

    assertFalse(Files.exists(spillDirectory));
  }

  @Test
  public void testSpillCleanupOnUpstreamError() {
    AggregateOperator operator = createSpillingSumOperator(true);

    MseBlock block = operator.nextBlock();
    Path spillDirectory = operator.getSpillDirectory();

    assertTrue(block.isError());
    assertNotNull(spillDirectory);
    assertFalse(Files.exists(spillDirectory));
  }

  @DataProvider(name = "unsupportedSpillModes")
  public Object[][] unsupportedSpillModes() {
    return new Object[][]{{true, 0}, {false, 1}};
  }

  @Test(dataProvider = "unsupportedSpillModes")
  public void testSpillDisabledForUnsupportedModes(boolean leafReturnFinalResult, int limit) {
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .addRow(2, 2.0)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateNode node = new AggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(),
        List.of(getSum(new RexExpression.InputRef(1))), List.of(-1), List.of(0), AggType.DIRECT,
        leafReturnFinalResult, null, limit);
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getContext(
            Map.of(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED, "true",
                QueryOptionKey.MSE_AGGREGATION_SPILL_THRESHOLD, "0",
                QueryOptionKey.MSE_AGGREGATION_SPILL_PARTITIONS, "0")), _input, node);

    assertTrue(operator.nextBlock().isData());
    assertNull(operator.getSpillDirectory());
  }

  @Test
  public void testSpillOptionsIgnoredForGlobalAggregation() {
    DataSchema inputSchema = new DataSchema(new String[]{"arg"}, new ColumnDataType[]{DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1.0)
        .addRow(2.0)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        getOperator(resultSchema, List.of(getSum(new RexExpression.InputRef(0))), List.of(-1), List.of(),
            PlanNode.NodeHint.EMPTY, Map.of(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED, "true",
                QueryOptionKey.MSE_AGGREGATION_SPILL_THRESHOLD, "0",
                QueryOptionKey.MSE_AGGREGATION_SPILL_PARTITIONS, "0"));

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{3.0});
    assertNull(operator.getSpillDirectory());
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void testSpillDisabledWithoutServerGate() {
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .finishBlock()
        .addRow(2, 2.0)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        getOperator(resultSchema, List.of(getSum(new RexExpression.InputRef(1))), List.of(-1), List.of(0),
            PlanNode.NodeHint.EMPTY, Map.of(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED, "false",
                QueryOptionKey.MSE_AGGREGATION_SPILL_THRESHOLD, "0",
                QueryOptionKey.MSE_AGGREGATION_SPILL_PARTITIONS, "0"));

    assertTrue(operator.nextBlock().isData());
    assertNull(operator.getSpillDirectory());
  }

  @Test
  public void testDisabledSpillStatsCanBeReadByLegacyNode()
      throws Exception {
    DataSchema inputSchema = new DataSchema(new String[]{"group"}, new ColumnDataType[]{INT});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1)
        .addRow(2)
        .buildWithEos();
    AggregateOperator operator =
        getOperator(inputSchema, List.of(), List.of(), List.of(0), PlanNode.NodeHint.EMPTY,
            Map.of(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED, "false"));
    drainRows(operator);
    StatMap<AggregateOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, operator.calculateStats());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    statMap.serialize(new DataOutputStream(bytes));

    StatMap<LegacyAggregateStatKey> legacyStatMap =
        StatMap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())),
            LegacyAggregateStatKey.class);

    assertEquals(legacyStatMap.getLong(LegacyAggregateStatKey.NUM_GROUPS), 2);
  }

  @Test
  public void testSpillDoesNotApplyNumGroupsLimitPerRun() {
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .addRow(2, 2.0)
        .addRow(3, 5.0)
        .finishBlock()
        .addRow(3, 7.0)
        .buildWithEos();
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "2")));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        getOperator(resultSchema, List.of(getSum(new RexExpression.InputRef(1))), List.of(-1), List.of(0), nodeHint,
            spillOptions(2, 2));

    Map<Integer, Double> resultByGroup = new HashMap<>();
    for (Object[] row : drainRows(operator)) {
      resultByGroup.put((int) row[0], (double) row[1]);
    }
    assertEquals(resultByGroup, Map.of(1, 1.0, 3, 12.0));
  }

  @Test
  public void testSpillRoundTripsAnyValueIntermediateResult() {
    RexExpression.FunctionCall anyValue =
        new RexExpression.FunctionCall(INT, SqlKind.ANY_VALUE.name(), List.of(new RexExpression.InputRef(1)));
    DataSchema intermediateSchema =
        new DataSchema(new String[]{"group", "anyValue"}, new ColumnDataType[]{INT, INT});
    _input = new BlockListMultiStageOperator.Builder(intermediateSchema)
        .addRow(1, 10)
        .finishBlock()
        .addRow(1, 20)
        .buildWithEos();
    AggregateOperator operator =
        getOperator(intermediateSchema, List.of(anyValue), List.of(-1), List.of(0), AggType.INTERMEDIATE,
            spillOptions(1, 2));

    List<Object[]> rows = drainRows(operator);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{1, 10});
  }

  @DataProvider(name = "binaryAnyValueModes")
  public Object[][] binaryAnyValueModes() {
    return new Object[][]{
        {BYTES, AggType.DIRECT},
        {BYTES, AggType.INTERMEDIATE},
        {UUID, AggType.DIRECT},
        {UUID, AggType.INTERMEDIATE}
    };
  }

  @Test(dataProvider = "binaryAnyValueModes")
  public void testSpillRoundTripsBinaryAnyValue(ColumnDataType dataType, AggType aggType) {
    RexExpression.FunctionCall anyValue =
        new RexExpression.FunctionCall(dataType, SqlKind.ANY_VALUE.name(), List.of(new RexExpression.InputRef(1)));
    DataSchema schema = new DataSchema(new String[]{"group", "anyValue"}, new ColumnDataType[]{INT, dataType});
    ByteArray firstValue = new ByteArray(new byte[16]);
    ByteArray secondValue = new ByteArray(new byte[]{
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
    });
    _input = new BlockListMultiStageOperator.Builder(schema)
        .addRow(1, firstValue)
        .finishBlock()
        .addRow(1, secondValue)
        .buildWithEos();
    AggregateOperator operator =
        getOperator(schema, List.of(anyValue), List.of(-1), List.of(0), aggType, spillOptions(1, 2));

    List<Object[]> rows = drainRows(operator);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{1, firstValue});
  }

  @DataProvider(name = "numGroupsLimitModes")
  public Object[][] numGroupsLimitModes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "numGroupsLimitModes")
  public void testSpillEnabledWithoutSpillStillEnforcesNumGroupsLimit(boolean errorOnLimit) {
    DataSchema inputSchema = new DataSchema(new String[]{"group"}, new ColumnDataType[]{INT});
    _input = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1)
        .addRow(2)
        .addRow(3)
        .buildWithEos();
    Map<String, String> options = new HashMap<>(spillOptions(100, 2));
    options.put(QueryOptionKey.NUM_GROUPS_LIMIT, "2");
    options.put(QueryOptionKey.ERROR_ON_NUM_GROUPS_LIMIT, Boolean.toString(errorOnLimit));
    AggregateOperator operator =
        getOperator(inputSchema, List.of(), List.of(), List.of(0), PlanNode.NodeHint.EMPTY, options);

    if (errorOnLimit) {
      assertTrue(operator.nextBlock().isError());
    } else {
      assertEquals(drainRows(operator).size(), 2);
      StatMap<AggregateOperator.StatKey> statMap =
          OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, operator.calculateStats());
      assertTrue(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED));
      assertEquals(statMap.getLong(AggregateOperator.StatKey.NUM_GROUPS), 2);
    }
    assertNull(operator.getSpillDirectory());
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

  private static List<MseBlock.Data> drainDataBlocks(AggregateOperator operator) {
    List<MseBlock.Data> blocks = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      blocks.add((MseBlock.Data) block);
      block = operator.nextBlock();
    }
    assertTrue(block.isSuccess());
    return blocks;
  }

  private static List<Object[]> drainRows(AggregateOperator operator) {
    List<Object[]> rows = new ArrayList<>();
    for (MseBlock.Data block : drainDataBlocks(operator)) {
      rows.addAll(block.asRowHeap().getRows());
    }
    return rows;
  }

  private AggregateOperator createSpillingSumOperator(boolean error) {
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    BlockListMultiStageOperator.Builder inputBuilder = new BlockListMultiStageOperator.Builder(inputSchema)
        .addRow(1, 1.0)
        .finishBlock()
        .addRow(2, 2.0);
    _input = error
        ? inputBuilder.buildWithError(ErrorMseBlock.fromException(new RuntimeException("upstream failure")))
        : inputBuilder.buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    return getOperator(resultSchema, List.of(getSum(new RexExpression.InputRef(1))), List.of(-1), List.of(0),
        PlanNode.NodeHint.EMPTY, spillOptions(1, 2));
  }

  private static Map<String, String> spillOptions(int threshold, int partitions) {
    return Map.of(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED, "true",
        QueryOptionKey.MSE_AGGREGATION_SPILL_THRESHOLD, Integer.toString(threshold),
        QueryOptionKey.MSE_AGGREGATION_SPILL_PARTITIONS, Integer.toString(partitions));
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys, PlanNode.NodeHint nodeHint,
      Map<String, String> opChainMetadata) {
    return getOperator(resultSchema, aggCalls, filterArgs, groupKeys, AggType.DIRECT, nodeHint, opChainMetadata);
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys, AggType aggType, Map<String, String> opChainMetadata) {
    return getOperator(resultSchema, aggCalls, filterArgs, groupKeys, aggType, PlanNode.NodeHint.EMPTY,
        opChainMetadata);
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys, AggType aggType, PlanNode.NodeHint nodeHint,
      Map<String, String> opChainMetadata) {
    return new AggregateOperator(OperatorTestUtil.getContext(opChainMetadata), _input,
        new AggregateNode(-1, resultSchema, nodeHint, List.of(), aggCalls, filterArgs, groupKeys, aggType, false, null,
            0));
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys) {
    return getOperator(resultSchema, aggCalls, filterArgs, groupKeys, PlanNode.NodeHint.EMPTY, Map.of());
  }

  private enum LegacyAggregateStatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG),
    EMITTED_ROWS(StatMap.Type.LONG),
    GROUPS_TRIMMED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_GROUPS_WARNING_LIMIT_REACHED(StatMap.Type.BOOLEAN),
    NUM_GROUPS(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return Math.max(value1, value2);
      }
    },
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    LegacyAggregateStatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
