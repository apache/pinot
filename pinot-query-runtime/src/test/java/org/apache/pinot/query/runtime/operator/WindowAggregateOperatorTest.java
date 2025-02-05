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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockTestUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.exception.QException;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.*;
import static org.apache.pinot.query.planner.plannode.WindowNode.WindowFrameType.RANGE;
import static org.apache.pinot.query.planner.plannode.WindowNode.WindowFrameType.ROWS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class WindowAggregateOperatorTest {
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
  public void testShouldHandleUpstreamErrorBlocks() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!")));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{
        INT, INT, DOUBLE
    });
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    verify(_input, times(1)).nextBlock();
    assertTrue(block.isErrorBlock(), "Input errors should propagate immediately");
  }

  @Test
  public void testShouldHandleEndOfStreamBlockWithNoOtherInputs() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{
        INT, INT, DOUBLE
    });
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    verify(_input, times(1)).nextBlock();
    assertTrue(block.isSuccessfulEndOfStreamBlock(), "EOS blocks should propagate");
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlock() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{
        INT, INT, DOUBLE
    });
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 1, 1.0});
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlockWithSameOrderByKeys() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{
        INT, INT, DOUBLE
    });
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 1, 1.0});
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlockWithoutPartitionByKeys() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{
        INT, INT, DOUBLE
    });
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, List.of(), List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 1, 1.0});
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlockWithLiteralInput() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, 3}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{
        INT, INT, DOUBLE
    });
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.Literal(ColumnDataType.INT, 42)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 3, 42.0});
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testPartitionByWindowAggregateWithHashCollision() {
    // Given:
    _input = OperatorTestUtil.getOperator(OperatorTestUtil.OP_1);
    DataSchema inputSchema = new DataSchema(new String[]{"arg", "group"}, new ColumnDataType[]{INT, STRING});
    DataSchema resultSchema =
        new DataSchema(new String[]{"arg", "group", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(1);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(0)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, keys, Map.of("Aa", List.<Object[]>of(new Object[]{1, "Aa", 1.0}), "BB",
        List.of(new Object[]{2, "BB", 5.0}, new Object[]{3, "BB", 5.0})));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Failed to instantiate "
      + "WindowFunction for function: AVERAGE.*")
  public void testShouldThrowOnUnknownAggFunction() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});
    DataSchema resultSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, "AVERAGE", List.of()));

    // When:
    getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
        Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Failed to instantiate "
      + "WindowFunction for function: NTILE.*")
  public void testShouldThrowOnUnknownRankAggFunction() {
    // TODO: Remove this test when support is added for NTILE function
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});
    DataSchema resultSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.NTILE.name(), List.of()));

    // When:
    getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
        Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  @Test
  public void testRankDenseRankRankingFunctions() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    // Input should be in sorted order on the order by key as SortExchange will handle pre-sorting the data
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{3, "and"}, new Object[]{2, "bar"}, new Object[]{2, "foo"},
                new Object[]{1, "foo"})).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{1, "foo"}, new Object[]{2, "foo"}, new Object[]{1, "numb"},
                new Object[]{2, "the"}, new Object[]{3, "true"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "rank", "dense_rank"},
        new ColumnDataType[]{INT, STRING, LONG, LONG});
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.RANK.name(), List.of()),
            new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.DENSE_RANK.name(), List.of()));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, 0);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, keys,
        Map.of(1, List.of(new Object[]{1, "foo", 1L, 1L}, new Object[]{1, "foo", 1L, 1L}, new Object[]{
                1, "numb", 3L, 2L
            }), 2, List.of(new Object[]{2, "bar", 1L, 1L}, new Object[]{2, "foo", 2L, 2L}, new Object[]{
                2, "foo", 2L, 2L
            }, new Object[]{2, "the", 4L, 3L}), 3,
            List.of(new Object[]{3, "and", 1L, 1L}, new Object[]{3, "true", 2L, 2L})));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testRowNumberRankingFunction() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    // Input should be in sorted order on the order by key as SortExchange will handle pre-sorting the data
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{3, "and"}, new Object[]{2, "bar"}, new Object[]{2, "foo"}))
        .thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{1, "foo"}, new Object[]{2, "foo"}, new Object[]{2, "the"},
                new Object[]{3, "true"})).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "row_number"}, new ColumnDataType[]{INT, STRING, LONG});
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.ROW_NUMBER.name(), List.of()));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, ROWS,
            Integer.MIN_VALUE, 0);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, keys, Map.of(1, List.<Object[]>of(new Object[]{1, "foo", 1L}), 2,
        List.of(new Object[]{2, "bar", 1L}, new Object[]{2, "foo", 2L}, new Object[]{2, "foo", 3L},
            new Object[]{2, "the", 4L}), 3, List.of(new Object[]{3, "and", 1L}, new Object[]{3, "true", 2L})));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testNonEmptyOrderByKeysNotMatchingPartitionByKeys() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    // Input should be in sorted order on the order by key as SortExchange will handle pre-sorting the data
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{3, "and"}, new Object[]{2, "bar"}, new Object[]{2, "foo"}))
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{1, "foo"}, new Object[]{2, "foo"},
            new Object[]{3, "true"})).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(0)));
    // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (default window frame for ORDER BY)
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, 0);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, keys, Map.of(1, List.<Object[]>of(new Object[]{1, "foo", 1.0}), 2,
        List.of(new Object[]{2, "bar", 2.0}, new Object[]{2, "foo", 6.0}, new Object[]{2, "foo", 6.0}), 3,
        List.of(new Object[]{3, "and", 3.0}, new Object[]{3, "true", 6.0})));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testNonEmptyOrderByKeysMatchingPartitionByKeys() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(1);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(0)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, 0);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, "foo", 2.0});
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testNonEmptyOrderByKeysMatchingPartitionByKeysWithDifferentDirection() {
    // Given:
    // Set ORDER BY key same as PARTITION BY key with custom direction and null direction. Should still be treated
    // like a PARTITION BY only query (since the final aggregation value won't change).
    // TODO: Test null direction handling once support for it is available
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "foo"}))
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "bar"}))
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{3, "foo"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(1);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(0)));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, keys, Map.of("bar", List.<Object[]>of(new Object[]{2, "bar", 2.0}), "foo",
        List.of(new Object[]{2, "foo", 5.0}, new Object[]{3, "foo", 5.0})));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldReturnErrorBlockOnUnexpectedInputType() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    // TODO: it is necessary to produce two values here, the operator only throws on second
    // (see the comment in WindowAggregate operator)
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "metallica"}))
        .thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "pink floyd"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> keys = List.of(0);
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isErrorBlock(), "expected ERROR block from invalid computation");
    assertEquals(block.getExceptions().keySet(), Sets.newHashSet(QException.SQL_RUNTIME_ERROR_CODE),
        "Expected only SQL runtime error but got: " + block.getExceptions());
    assertEquals(block.getExceptions().get(QException.SQL_RUNTIME_ERROR_CODE),
        "Operator WINDOW on stage 0 failed: Failed to cast value as expected in SUM");
  }

  @Test
  public void testShouldPropagateWindowLimitError() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, 1}, new Object[]{3, 4}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, INT, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.WINDOW_HINT_OPTIONS,
        Map.of(PinotHintOptions.WindowHintOptions.WINDOW_OVERFLOW_MODE, "THROW",
            PinotHintOptions.WindowHintOptions.MAX_ROWS_IN_WINDOW, "1")));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE, nodeHint);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isErrorBlock(), "expected ERROR block from window overflow");
    assertTrue(block.getExceptions().get(QException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
        .contains("reach number of rows limit"));
  }

  @Test
  public void testShouldHandleWindowWithPartialResultsWhenHitDataRowsLimit() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, 1}, new Object[]{3, 4}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, INT, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.WINDOW_HINT_OPTIONS,
        Map.of(PinotHintOptions.WindowHintOptions.WINDOW_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.WindowHintOptions.MAX_ROWS_IN_WINDOW, "1")));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE, nodeHint);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verify(_input).earlyTerminate();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2, 1, 1.0});
    TransferableBlock block2 = operator.nextBlock();
    assertTrue(block2.isSuccessfulEndOfStreamBlock());
    StatMap<WindowAggregateOperator.StatKey> windowStats =
        OperatorTestUtil.getStatMap(WindowAggregateOperator.StatKey.class, block2);
    assertTrue(windowStats.getBoolean(WindowAggregateOperator.StatKey.MAX_ROWS_IN_WINDOW_REACHED),
        "Max rows in window should be reached");
  }

  @Test
  public void testLeadLagWindowFunction() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{3, "and"}, new Object[]{2, "bar"}, new Object[]{2, "foo"},
                new Object[]{1, "foo"})).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{1, "foo"}, new Object[]{2, "foo"}, new Object[]{1, "numb"},
                new Object[]{2, "the"}, new Object[]{3, "true"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "lead", "lag"}, new ColumnDataType[]{INT, STRING, INT, INT});
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls = List.of(
        new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.LEAD.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.INT, 1))),
        new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.LAG.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.INT, 1))));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, 0);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    // Then:
    verifyResultRows(resultRows, keys, Map.of(
        1, List.of(
            new Object[]{1, "foo", 1, null},
            new Object[]{1, "foo", 1, 1},
            new Object[]{1, "numb", null, 1}),
        2, List.of(
            new Object[]{2, "bar", 2, null},
            new Object[]{2, "foo", 2, 2},
            new Object[]{2, "foo", 2, 2},
            new Object[]{2, "the", null, 2}),
        3, List.of(
            new Object[]{3, "and", 3, null},
            new Object[]{3, "true", null, 3})
    ));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLeadLagWindowFunction2() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{3, "and"}, new Object[]{2, "bar"}, new Object[]{2, "foo"},
                new Object[]{1, "foo"})).thenReturn(
            OperatorTestUtil.block(inputSchema, new Object[]{1, "foo"}, new Object[]{2, "foo"}, new Object[]{1, "numb"},
                new Object[]{2, "the"}, new Object[]{3, "true"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "lead", "lag"}, new ColumnDataType[]{INT, STRING, INT, INT});
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls = List.of(
        new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.LEAD.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.INT, 2),
                new RexExpression.Literal(ColumnDataType.INT, 100))),
        new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.LAG.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.Literal(ColumnDataType.INT, 1),
                new RexExpression.Literal(ColumnDataType.INT, 200))));
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, 0);

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    // Then:
    verifyResultRows(resultRows, keys, Map.of(
        1, List.of(
            new Object[]{1, "foo", 1, 200},
            new Object[]{1, "foo", 100, 1},
            new Object[]{1, "numb", 100, 1}),
        2, List.of(
            new Object[]{2, "bar", 2, 200},
            new Object[]{2, "foo", 2, 2},
            new Object[]{2, "foo", 100, 2},
            new Object[]{2, "the", 100, 2}),
        3, List.of(
            new Object[]{3, "and", 100, 200},
            new Object[]{3, "true", 100, 3})
    ));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testSumWithUnboundedPrecedingLowerAndUnboundedFollowingUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, frameType, Integer.MIN_VALUE, Integer.MAX_VALUE,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then (result should be the same for both window frame types):
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 59.0},
            new Object[]{"A", 10, 2002, 59.0},
            new Object[]{"A", 20, 2008, 59.0},
            new Object[]{"A", 15, 2008, 59.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testSumWithUnboundedPrecedingLowerAndCurrentRowUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, frameType, Integer.MIN_VALUE, 0,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14.0},
            new Object[]{"A", 10, 2002, 24.0},
            new Object[]{"A", 20, 2008, frameType == ROWS ? 44.0 : 59.0},
            new Object[]{"A", 15, 2008, 59.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithUnboundedPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, Integer.MIN_VALUE, 2,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 44.0},
            new Object[]{"A", 10, 2002, 59.0},
            new Object[]{"A", 20, 2008, 59.0},
            new Object[]{"A", 15, 2008, 59.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithUnboundedPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, Integer.MIN_VALUE, -2,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, null},
            new Object[]{"A", 20, 2008, 14.0},
            new Object[]{"A", 15, 2008, 24.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testSumWithCurrentRowLowerAndUnboundedFollowingUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, frameType, 0, Integer.MAX_VALUE,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 59.0},
            new Object[]{"A", 10, 2002, 45.0},
            new Object[]{"A", 20, 2008, 35.0},
            new Object[]{"A", 15, 2008, frameType == ROWS ? 15.0 : 35.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 20.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testSumWithCurrentRowLowerAndCurrentRowUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, frameType, 0, 0,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14.0},
            new Object[]{"A", 10, 2002, 10.0},
            new Object[]{"A", 20, 2008, frameType == ROWS ? 20.0 : 35.0},
            new Object[]{"A", 15, 2008, frameType == ROWS ? 15.0 : 35.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10.0},
            new Object[]{"B", 20, 2005, 20.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithCurrentRowLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, 0, 2,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 44.0},
            new Object[]{"A", 10, 2002, 45.0},
            new Object[]{"A", 20, 2008, 35.0},
            new Object[]{"A", 15, 2008, 15.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 20.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithOffsetPrecedingLowerAndUnboundedFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, -1, Integer.MAX_VALUE,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 59.0},
            new Object[]{"A", 10, 2002, 59.0},
            new Object[]{"A", 20, 2008, 45.0},
            new Object[]{"A", 15, 2008, 35.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithOffsetFollowingLowerAndUnboundedFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, 1, Integer.MAX_VALUE,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 45.0},
            new Object[]{"A", 10, 2002, 35.0},
            new Object[]{"A", 20, 2008, 15.0},
            new Object[]{"A", 15, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20.0},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithOffsetPrecedingLowerAndCurrentRowUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, -2, 0,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14.0},
            new Object[]{"A", 10, 2002, 24.0},
            new Object[]{"A", 20, 2008, 44.0},
            new Object[]{"A", 15, 2008, 45.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithOffsetPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, -1, 2,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 44.0},
            new Object[]{"A", 10, 2002, 59.0},
            new Object[]{"A", 20, 2008, 45.0},
            new Object[]{"A", 15, 2008, 35.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithVeryLargeOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        // Verify if overflows are handled correctly
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, -1, 2147483646,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 59.0},
            new Object[]{"A", 10, 2002, 59.0},
            new Object[]{"A", 20, 2008, 45.0},
            new Object[]{"A", 15, 2008, 35.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithVeryLargeOffsetFollowingLower() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        // Verify if overflows are handled correctly
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, 2147483646, 2147483647,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, null},
            new Object[]{"A", 20, 2008, null},
            new Object[]{"A", 15, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithOffsetPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, -3, -2,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, null},
            new Object[]{"A", 20, 2008, 14.0},
            new Object[]{"A", 15, 2008, 24.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithOffsetFollowingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 2, ROWS, 1, 2,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 30.0},
            new Object[]{"A", 10, 2002, 35.0},
            new Object[]{"A", 20, 2008, 15.0},
            new Object[]{"A", 15, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20.0},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testSumWithSamePartitionAndCollationKey() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, DOUBLE, List.of(0), 0, RANGE, Integer.MIN_VALUE, 0,
        getSum(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 59.0},
            new Object[]{"A", 10, 2002, 59.0},
            new Object[]{"A", 20, 2008, 59.0},
            new Object[]{"A", 15, 2008, 59.0}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 30.0},
            new Object[]{"B", 20, 2005, 30.0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testMinWithRowsWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -1, 1,
        getMin(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 10, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 10, 2008, 10},
            new Object[]{"A", 15, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testMinWithRangeWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, RANGE, 0, Integer.MAX_VALUE,
        getMin(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 12, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 12},
            new Object[]{"A", 12, 2008, 12},
            new Object[]{"A", 15, 2008, 12}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testMaxWithRowsWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 0, 2,
        getMax(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 20, 2000},
            new Object[]{"A", 15, 2002},
            new Object[]{"A", 15, 2008},
            new Object[]{"A", 10, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 20, 2000, 20},
            new Object[]{"A", 15, 2002, 15},
            new Object[]{"A", 15, 2008, 15},
            new Object[]{"A", 10, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testMaxWithRangeWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, RANGE, 0, 0,
        getMax(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 20},
            new Object[]{"A", 15, 2008, 20}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2000, 20},
            new Object[]{"B", null, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testBoolAndWithRowsWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, BOOLEAN, INT}, BOOLEAN, List.of(0), 2, ROWS, -2, -1,
        getBoolAnd(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 0, 2000},
            new Object[]{"A", 1, 2002},
            new Object[]{"A", 1, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"A", null, 2010},
            new Object[]{"B", 1, 2000},
            new Object[]{"B", 0, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 0, 2000, null},
            new Object[]{"A", 1, 2002, 0},
            new Object[]{"A", 1, 2008, 0},
            new Object[]{"A", null, 2008, 1},
            new Object[]{"A", null, 2010, null}
        ),
        "B", List.of(
            new Object[]{"B", 1, 2000, null},
            new Object[]{"B", 0, 2005, 1}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testBoolAndWithRangeWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, BOOLEAN, INT}, BOOLEAN, List.of(0), 2, RANGE, Integer.MIN_VALUE, 0,
        getBoolAnd(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 0, 2000},
            new Object[]{"A", 1, 2002},
            new Object[]{"A", 1, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"A", null, 2010},
            new Object[]{"B", 1, 2000},
            new Object[]{"B", 0, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 0, 2000, 0},
            new Object[]{"A", 1, 2002, 0},
            new Object[]{"A", 1, 2008, 0},
            new Object[]{"A", null, 2008, 0},
            new Object[]{"A", null, 2010, 0}
        ),
        "B", List.of(
            new Object[]{"B", 1, 2000, 1},
            new Object[]{"B", 0, 2005, 0}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testBoolOrWithRowsWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, BOOLEAN, INT}, BOOLEAN, List.of(0), 2, ROWS, 1, 2,
        getBoolOr(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 0, 2000},
            new Object[]{"A", 1, 2002},
            new Object[]{"A", 1, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"A", null, 2010},
            new Object[]{"B", 1, 2000},
            new Object[]{"B", 0, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 0, 2000, 1},
            new Object[]{"A", 1, 2002, 1},
            new Object[]{"A", 1, 2008, null},
            new Object[]{"A", null, 2008, null},
            new Object[]{"A", null, 2010, null}
        ),
        "B", List.of(
            new Object[]{"B", 1, 2000, 0},
            new Object[]{"B", 0, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testBoolOrWithRangeWindow() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, BOOLEAN, INT}, BOOLEAN, List.of(0), 2, RANGE, Integer.MIN_VALUE, Integer.MAX_VALUE,
        getBoolOr(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 0, 2000},
            new Object[]{"A", 1, 2002},
            new Object[]{"A", 1, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"A", null, 2010},
            new Object[]{"B", 1, 2000},
            new Object[]{"B", 0, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 0, 2000, 1},
            new Object[]{"A", 1, 2002, 1},
            new Object[]{"A", 1, 2008, 1},
            new Object[]{"A", null, 2008, 1},
            new Object[]{"A", null, 2010, 1}
        ),
        "B", List.of(
            new Object[]{"B", 1, 2000, 1},
            new Object[]{"B", 0, 2005, 1}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueWithUnboundedPrecedingLowerAndCurrentRowUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, 0,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", 20, 2008, 14},
            new Object[]{"A", 15, 2008, 14}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueWithUnboundedPrecedingLowerAndUnboundedFollowingUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, Integer.MAX_VALUE,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", 20, 2008, 14},
            new Object[]{"A", 15, 2008, 14}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueWithUnboundedPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, -2,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, null},
            new Object[]{"A", 20, 2008, 14},
            new Object[]{"A", 15, 2008, 14}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueWithUnboundedPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, 2,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", 20, 2008, 14},
            new Object[]{"A", 15, 2008, 14}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueWithCurrentRowLowerAndUnboundedFollowingUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, Integer.MAX_VALUE,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", null, 2008, null},
            new Object[]{"A", 15, 2008, frameType == ROWS ? 15 : null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueWithCurrentRowLowerAndCurrentRowUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, 0,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 20},
            new Object[]{"A", 15, 2008, frameType == ROWS ? 15 : 20}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueWithCurrentRowLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 0, 2,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 20},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueWithOffsetPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -1, 2,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", 15, 2008, 20}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueWithOffsetPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -2, -1,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", 20, 2008, 14},
            new Object[]{"A", 15, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueWithOffsetFollowingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 2, 3,
        getFirstValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 20},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 20, 2008, null},
            new Object[]{"A", 15, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueWithUnboundedPrecedingLowerAndCurrentRowUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, 0,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, frameType == ROWS ? 20 : 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueWithUnboundedPrecedingLowerAndUnboundedFollowingUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, Integer.MAX_VALUE,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 15},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 20, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueWithUnboundedPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, -2,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, null},
            new Object[]{"A", 20, 2008, 14},
            new Object[]{"A", 15, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueWithUnboundedPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, 2,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 20},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 20, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueWithCurrentRowLowerAndUnboundedFollowingUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, Integer.MAX_VALUE,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 15},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 20, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueWithCurrentRowLowerAndCurrentRowUpper(WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, 0,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, frameType == ROWS ? 20 : null},
            new Object[]{"A", null, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueWithCurrentRowLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 0, 2,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 20},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 20, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueWithOffsetPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -1, 2,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 20},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 20, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2005, 20}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueWithOffsetPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -2, -1,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", 15, 2008, 20}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueWithOffsetFollowingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 1, 3,
        getLastValue(new RexExpression.InputRef(1)),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 15},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 20, 2008, 15},
            new Object[]{"A", 15, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 20},
            new Object[]{"B", 20, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueIgnoreNullsWithUnboundedPrecedingLowerAndCurrentRowUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, 0,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, null},
            new Object[]{"A", null, 2002, frameType == ROWS ? null : 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", null, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueIgnoreNullsWithUnboundedPrecedingLowerAndUnboundedFollowingUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, Integer.MAX_VALUE,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", 20, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, 10},
            new Object[]{"A", null, 2002, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", null, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", 20, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueIgnoreNullsWithUnboundedPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, -1,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, null},
            new Object[]{"A", null, 2002, null},
            new Object[]{"A", 10, 2002, null},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", null, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueIgnoreNullsWithUnboundedPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, 1,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, null},
            new Object[]{"A", null, 2002, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", null, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueIgnoreNullsWithCurrentRowLowerAndUnboundedFollowingUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, Integer.MAX_VALUE,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, 10},
            new Object[]{"A", null, 2002, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 20},
            new Object[]{"A", null, 2008, frameType == ROWS ? null : 20}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testFirstValueIgnoreNullsWithCurrentRowLowerAndCurrentRowUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, 0,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, null},
            new Object[]{"A", null, 2002, frameType == ROWS ? null : 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 20},
            new Object[]{"A", null, 2008, frameType == ROWS ? null : 20}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueIgnoreNullsWithCurrentRowLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 0, 1,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, null},
            new Object[]{"A", null, 2002, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 20},
            new Object[]{"A", null, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueIgnoreNullsWithOffsetPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -1, 1,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, null},
            new Object[]{"A", null, 2002, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", null, 2008, 20}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueIgnoreNullsWithOffsetPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -2, -1,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, null},
            new Object[]{"A", null, 2002, null},
            new Object[]{"A", 10, 2002, null},
            new Object[]{"A", 20, 2008, 10},
            new Object[]{"A", null, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testFirstValueIgnoreNullsWithOffsetFollowingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 1, 3,
        getFirstValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", null, 2000},
            new Object[]{"A", null, 2002},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 20, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", null, 2000, 10},
            new Object[]{"A", null, 2002, 10},
            new Object[]{"A", 10, 2002, 20},
            new Object[]{"A", 20, 2008, null},
            new Object[]{"A", null, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", null, 2005, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueIgnoreNullsWithUnboundedPrecedingLowerAndCurrentRowUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, 0,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", null, 2008, frameType == ROWS ? 10 : 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueIgnoreNullsWithUnboundedPrecedingLowerAndUnboundedFollowingUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, Integer.MIN_VALUE, Integer.MAX_VALUE,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 15, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 15},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 15, 2008, 15},
            new Object[]{"A", null, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueIgnoreNullsWithUnboundedPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, -1,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", null, 2008, 10},
            new Object[]{"A", 15, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueIgnoreNullsWithUnboundedPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, Integer.MIN_VALUE, 2,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 10},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", null, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueIgnoreNullsWithCurrentRowLowerAndUnboundedFollowingUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, Integer.MAX_VALUE,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", 15, 2008},
            new Object[]{"A", null, 2008},
            new Object[]{"A", null, 2010},
            new Object[]{"B", null, 2000},
            new Object[]{"B", 10, 2005}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 15},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", 15, 2008, 15},
            new Object[]{"A", null, 2008, frameType == ROWS ? null : 15},
            new Object[]{"A", null, 2010, null}
        ),
        "B", List.of(
            new Object[]{"B", null, 2000, 10},
            new Object[]{"B", 10, 2005, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(dataProvider = "windowFrameTypes")
  public void testLastValueIgnoreNullsWithCurrentRowLowerAndCurrentRowUpper(
      WindowNode.WindowFrameType frameType) {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, frameType, 0, 0,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2000},
            new Object[]{"B", null, 2008}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 14},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", null, 2008, frameType == ROWS ? null : 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2000, frameType == ROWS ? null : 10},
            new Object[]{"B", null, 2008, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueIgnoreNullsWithCurrentRowLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 0, 1,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2008}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", null, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2008, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueIgnoreNullsWithOffsetPrecedingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -1, 1,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2008}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 10},
            new Object[]{"A", 10, 2002, 10},
            new Object[]{"A", null, 2008, 15},
            new Object[]{"A", 15, 2008, 15}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, 10},
            new Object[]{"B", null, 2008, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueIgnoreNullsWithOffsetPrecedingLowerAndOffsetPrecedingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, -2, -1,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2008}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, null},
            new Object[]{"A", 10, 2002, 14},
            new Object[]{"A", null, 2008, 10},
            new Object[]{"A", 15, 2008, 10}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", null, 2008, 10}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testLastValueIgnoreNullsWithOffsetFollowingLowerAndOffsetFollowingUpper() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value", "year"},
        new ColumnDataType[]{STRING, INT, INT}, INT, List.of(0), 2, ROWS, 1, 2,
        getLastValue(new RexExpression.InputRef(1), true),
        new Object[][]{
            new Object[]{"A", 14, 2000},
            new Object[]{"A", 10, 2002},
            new Object[]{"A", null, 2008},
            new Object[]{"A", 15, 2008},
            new Object[]{"B", 10, 2000},
            new Object[]{"B", null, 2008}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 14, 2000, 10},
            new Object[]{"A", 10, 2002, 15},
            new Object[]{"A", null, 2008, 15},
            new Object[]{"A", 15, 2008, null}
        ),
        "B", List.of(
            new Object[]{"B", 10, 2000, null},
            new Object[]{"B", null, 2008, null}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testNtile() {
    // Given:
    WindowAggregateOperator operator = prepareDataForWindowFunction(new String[]{"name", "value"},
        new ColumnDataType[]{STRING, INT}, INT, List.of(0), 1, ROWS, 0, 0,
        new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.NTILE.name(),
            List.of(new RexExpression.Literal(INT, 3)), false, false),
        new Object[][]{
            new Object[]{"A", 1},
            new Object[]{"A", 2},
            new Object[]{"A", 3},
            new Object[]{"A", 4},
            new Object[]{"A", 5},
            new Object[]{"A", 6},
            new Object[]{"A", 7},
            new Object[]{"A", 8},
            new Object[]{"A", 9},
            new Object[]{"A", 10},
            new Object[]{"A", 11},
            new Object[]{"B", 1},
            new Object[]{"B", 2},
            new Object[]{"B", 3},
            new Object[]{"B", 4},
            new Object[]{"B", 5},
            new Object[]{"B", 6},
            new Object[]{"C", 1},
            new Object[]{"C", 2}
        });

    // When:
    List<Object[]> resultRows = operator.nextBlock().getContainer();

    // Then:
    verifyResultRows(resultRows, List.of(0), Map.of(
        "A", List.of(
            new Object[]{"A", 1, 1},
            new Object[]{"A", 2, 1},
            new Object[]{"A", 3, 1},
            new Object[]{"A", 4, 1},
            new Object[]{"A", 5, 2},
            new Object[]{"A", 6, 2},
            new Object[]{"A", 7, 2},
            new Object[]{"A", 8, 2},
            new Object[]{"A", 9, 3},
            new Object[]{"A", 10, 3},
            new Object[]{"A", 11, 3}
        ),
        "B", List.of(
            new Object[]{"B", 1, 1},
            new Object[]{"B", 2, 1},
            new Object[]{"B", 3, 2},
            new Object[]{"B", 4, 2},
            new Object[]{"B", 5, 3},
            new Object[]{"B", 6, 3}
        ),
        "C", List.of(
            new Object[]{"C", 1, 1},
            new Object[]{"C", 2, 2}
        )));
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  private WindowAggregateOperator prepareDataForWindowFunction(String[] inputSchemaCols,
      ColumnDataType[] inputSchemaColTypes, ColumnDataType outputType, List<Integer> partitionKeys,
      int collationFieldIndex, WindowNode.WindowFrameType frameType, int windowFrameLowerBound,
      int windowFrameUpperBound, RexExpression.FunctionCall functionCall, Object[][] rows) {
    DataSchema inputSchema = new DataSchema(inputSchemaCols, inputSchemaColTypes);
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, rows))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));

    String[] outputSchemaCols = new String[inputSchemaCols.length + 1];
    System.arraycopy(inputSchemaCols, 0, outputSchemaCols, 0, inputSchemaCols.length);
    outputSchemaCols[inputSchemaCols.length] = functionCall.getFunctionName().toLowerCase();

    ColumnDataType[] outputSchemaColTypes = new ColumnDataType[inputSchemaColTypes.length + 1];
    System.arraycopy(inputSchemaColTypes, 0, outputSchemaColTypes, 0, inputSchemaColTypes.length);
    outputSchemaColTypes[inputSchemaCols.length] = outputType;

    DataSchema resultSchema = new DataSchema(outputSchemaCols, outputSchemaColTypes);
    List<RexExpression.FunctionCall> aggCalls = List.of(functionCall);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(collationFieldIndex));
    return getOperator(inputSchema, resultSchema, partitionKeys, collations, aggCalls, frameType, windowFrameLowerBound,
        windowFrameUpperBound);
  }

  @Test
  public void testShouldThrowOnWindowFrameWithInvalidOffsetBounds() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));

    // Then:
    IllegalStateException e = Assert.expectThrows(IllegalStateException.class,
        () -> getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, ROWS, 5, 2));
    assertEquals(e.getMessage(), "Window frame lower bound can't be greater than upper bound");

    e = Assert.expectThrows(IllegalStateException.class,
        () -> getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, ROWS, -2, -3));
    assertEquals(e.getMessage(), "Window frame lower bound can't be greater than upper bound");
  }

  @Test
  public void testShouldThrowOnWindowFrameWithOffsetBoundsForRange() {
    // TODO: Remove this test when support for RANGE window frames with offset PRECEDING / FOLLOWING is added
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));

    // Then:
    IllegalStateException e = Assert.expectThrows(IllegalStateException.class,
        () -> getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE, 5,
            Integer.MAX_VALUE));
    assertEquals(e.getMessage(), "RANGE window frame with offset PRECEDING / FOLLOWING is not supported");

    e = Assert.expectThrows(IllegalStateException.class,
        () -> getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MAX_VALUE, 5));
    assertEquals(e.getMessage(), "RANGE window frame with offset PRECEDING / FOLLOWING is not supported");
  }

  private WindowAggregateOperator getOperator(DataSchema inputSchema, DataSchema resultSchema, List<Integer> keys,
      List<RelFieldCollation> collations, List<RexExpression.FunctionCall> aggCalls,
      WindowNode.WindowFrameType windowFrameType, int lowerBound, int upperBound, PlanNode.NodeHint nodeHint) {
    return new WindowAggregateOperator(OperatorTestUtil.getTracingContext(), _input, inputSchema,
        new WindowNode(-1, resultSchema, nodeHint, List.of(), keys, collations, aggCalls, windowFrameType, lowerBound,
            upperBound, List.of()));
  }

  private WindowAggregateOperator getOperator(DataSchema inputSchema, DataSchema resultSchema, List<Integer> keys,
      List<RelFieldCollation> collations, List<RexExpression.FunctionCall> aggCalls,
      WindowNode.WindowFrameType windowFrameType, int lowerBound, int upperBound) {
    return getOperator(inputSchema, resultSchema, keys, collations, aggCalls, windowFrameType, lowerBound, upperBound,
        PlanNode.NodeHint.EMPTY);
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.SUM.name(), List.of(arg));
  }

  private static RexExpression.FunctionCall getMin(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.MIN.name(), List.of(arg));
  }

  private static RexExpression.FunctionCall getMax(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.MAX.name(), List.of(arg));
  }

  private static RexExpression.FunctionCall getBoolAnd(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, "BOOLAND", List.of(arg));
  }

  private static RexExpression.FunctionCall getBoolOr(RexExpression arg) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, "BOOLOR", List.of(arg));
  }

  private static RexExpression.FunctionCall getFirstValue(RexExpression arg) {
    return getFirstValue(arg, false);
  }

  private static RexExpression.FunctionCall getFirstValue(RexExpression arg, boolean ignoreNulls) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.FIRST_VALUE.name(), List.of(arg), false,
        ignoreNulls);
  }

  private static RexExpression.FunctionCall getLastValue(RexExpression arg) {
    return getLastValue(arg, false);
  }

  private static RexExpression.FunctionCall getLastValue(RexExpression arg, boolean ignoreNulls) {
    return new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.LAST_VALUE.name(), List.of(arg), false,
        ignoreNulls);
  }

  private static void verifyResultRows(List<Object[]> resultRows, List<Integer> keys,
      Map<Object, List<Object[]>> expectedKeyedRows) {
    int numKeys = keys.size();
    Map<Object, List<Object[]>> keyedResultRows = new HashMap<>();
    for (Object[] row : resultRows) {
      Object key;
      if (numKeys == 1) {
        key = row[keys.get(0)];
      } else {
        Object[] values = new Object[numKeys];
        for (int i = 0; i < numKeys; i++) {
          values[i] = row[keys.get(i)];
        }
        key = new Key(values);
      }
      keyedResultRows.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
    }
    assertEquals(keyedResultRows.size(), expectedKeyedRows.size());
    for (Map.Entry<Object, List<Object[]>> entry : keyedResultRows.entrySet()) {
      List<Object[]> expectedRows = expectedKeyedRows.get(entry.getKey());
      assertNotNull(expectedRows);
      verifyResultRows(entry.getValue(), expectedRows);
    }
  }

  private static void verifyResultRows(List<Object[]> resultRows, List<Object[]> expectedRows) {
    int numRows = resultRows.size();
    assertEquals(numRows, expectedRows.size());
    for (int i = 0; i < numRows; i++) {
      assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }

  @DataProvider(name = "windowFrameTypes")
  public Object[][] getWindowFrameTypes() {
    return new Object[][]{
        {ROWS},
        {WindowNode.WindowFrameType.RANGE}
    };
  }
}
