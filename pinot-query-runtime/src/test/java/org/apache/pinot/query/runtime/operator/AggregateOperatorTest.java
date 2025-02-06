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

import java.util.List;
import java.util.Map;
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
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockTestUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BOOLEAN;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
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
    when(_input.nextBlock()).thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!")));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    verify(_input, times(1)).nextBlock();
    assertTrue(block.isErrorBlock(), "Input errors should propagate immediately");
  }

  @Test
  public void shouldHandleEndOfStreamBlockWithNoOtherInputs() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    when(_input.nextBlock()).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    verify(_input, times(1)).nextBlock();
    assertTrue(block.isEndOfStreamBlock(), "EOS blocks should propagate");
  }

  @Test
  public void testAggregateSingleInputBlock() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 1.0},
        "Expected two columns (group by key, agg value), agg value is final result");
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
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
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 6.0},
        "Expected two columns (group by key, agg value), agg value is final result");
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
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
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "sum", "sumWithFilter"}, new ColumnDataType[]{INT, DOUBLE, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 6.0, 5.0},
        "Expected three columns (group by key, agg value, agg value with filter), agg value is final result");
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
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

    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    if (resultRows.get(0)[0].equals("Aa")) {
      assertEquals(resultRows.get(0), new Object[]{"Aa", 1.0});
      assertEquals(resultRows.get(1), new Object[]{"BB", 5.0});
    } else {
      assertEquals(resultRows.get(0), new Object[]{"BB", 5.0});
      assertEquals(resultRows.get(1), new Object[]{"Aa", 1.0});
    }
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*AVERAGE.*")
  public void shouldThrowOnUnknownAggFunction() {
    // Given:
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, "AVERAGE", List.of()));
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
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isErrorBlock(), "expected ERROR block from invalid computation");
    assertEquals(block.getExceptions().keySet(), Sets.newHashSet(QException.INTERNAL_ERROR_CODE),
        "expected it to fail with internal error");
    assertEquals(block.getExceptions().get(QException.INTERNAL_ERROR_CODE),
        "Operator AGGREGATE_OPERATOR on stage 0 failed", "expected it to fail in AGGREGATE_OPERATOR");
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
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}, new Object[]{3, 2.0}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{3, 3.0}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator = getOperator(resultSchema, aggCalls, filterArgs, groupKeys, nodeHint);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    verify(_input).earlyTerminate();
    assertEquals(block1.getNumRows(), 1, "when group limit reach it should only return that many groups");
    assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
    StatMap<AggregateOperator.StatKey> statMap = OperatorTestUtil.getStatMap(AggregateOperator.StatKey.class, block2);
    assertTrue(statMap.getBoolean(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED),
        "num groups limit should be reached");
  }

  @Test
  public void testGroupTrimSizeIsDisabledByDefault() {
    PlanNode.NodeHint nodeHint = null;
    OpChainExecutionContext context = OperatorTestUtil.getTracingContext();

    Assert.assertEquals(getAggregateOperator(context, nodeHint, 10).getGroupTrimSize(), Integer.MAX_VALUE);
    Assert.assertEquals(getAggregateOperator(context, nodeHint, 0).getGroupTrimSize(), Integer.MAX_VALUE);
  }

  @Test
  public void testGroupTrimSizeDependsOnContextValue() {
    PlanNode.NodeHint nodeHint = null;
    OpChainExecutionContext context =
        OperatorTestUtil.getContext(Map.of(CommonConstants.Broker.Request.QueryOptionKey.GROUP_TRIM_SIZE, "100"));

    AggregateOperator operator = getAggregateOperator(context, nodeHint, 5);

    Assert.assertEquals(operator.getGroupTrimSize(), 100);
  }

  @Test
  public void testGroupTrimHintOverridesContextValue() {
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.GROUP_TRIM_SIZE, "30")));

    OpChainExecutionContext context =
        OperatorTestUtil.getContext(Map.of(CommonConstants.Broker.Request.QueryOptionKey.GROUP_TRIM_SIZE, "100"));

    AggregateOperator operator = getAggregateOperator(context, nodeHint, 5);

    Assert.assertEquals(operator.getGroupTrimSize(), 30);
  }

  private AggregateOperator getAggregateOperator(OpChainExecutionContext context, PlanNode.NodeHint nodeHint,
      int limit) {
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema resultSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    return new AggregateOperator(context, _input,
        new AggregateNode(-1, resultSchema, nodeHint, List.of(), aggCalls, filterArgs, groupKeys, AggType.DIRECT,
            false, null, limit));
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.SUM.name(), List.of(arg));
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys, PlanNode.NodeHint nodeHint) {
    return new AggregateOperator(OperatorTestUtil.getTracingContext(), _input,
        new AggregateNode(-1, resultSchema, nodeHint, List.of(), aggCalls, filterArgs, groupKeys, AggType.DIRECT,
            false, null, 0));
  }

  private AggregateOperator getOperator(DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls,
      List<Integer> filterArgs, List<Integer> groupKeys) {
    return getOperator(resultSchema, aggCalls, filterArgs, groupKeys, PlanNode.NodeHint.EMPTY);
  }
}
