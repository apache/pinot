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
import org.apache.pinot.common.exception.QueryException;
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
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
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
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.ROWS,
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
    WindowAggregateOperator operator =
        getOperator(inputSchema, resultSchema, keys, collations, aggCalls, WindowNode.WindowFrameType.RANGE,
            Integer.MIN_VALUE, Integer.MAX_VALUE);

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

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Only RANGE type frames "
      + "are supported at present.*")
  public void testShouldThrowOnInvalidRowsFunction() {
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));

    // Then:
    getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.ROWS,
        Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Only default frame is "
      + "supported, lowerBound must be UNBOUNDED PRECEDING")
  public void testShouldThrowOnCustomFramesCustomPreceding() {
    // TODO: Remove this test once custom frame support is added
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));

    // Then:
    getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE, 5,
        Integer.MAX_VALUE);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Only default frame is "
      + "supported, upperBound must be UNBOUNDED FOLLOWING or CURRENT ROW")
  public void testShouldThrowOnCustomFramesCustomFollowing() {
    // TODO: Remove this test once custom frame support is added
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "arg", "sum"}, new ColumnDataType[]{INT, STRING, DOUBLE});
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));

    // Then:
    getOperator(inputSchema, resultSchema, keys, List.of(), aggCalls, WindowNode.WindowFrameType.RANGE,
        Integer.MIN_VALUE, 5);
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
    assertTrue(block.getExceptions().get(1000).contains("String cannot be cast to class"),
        "expected it to fail with class cast exception");
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
    assertTrue(block.getExceptions().get(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
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
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "lead", "lag"},
        new ColumnDataType[]{INT, STRING, INT, INT});
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.LEAD.name(),
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
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "lead", "lag"},
        new ColumnDataType[]{INT, STRING, INT, INT});
    List<Integer> keys = List.of(0);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    List<RexExpression.FunctionCall> aggCalls =
        List.of(new RexExpression.FunctionCall(ColumnDataType.INT, SqlKind.LEAD.name(),
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
}
