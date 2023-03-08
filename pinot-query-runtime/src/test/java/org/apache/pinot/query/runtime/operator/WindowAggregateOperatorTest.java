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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.stage.WindowNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;


public class WindowAggregateOperatorTest {

  private AutoCloseable _mocks;

  @Mock
  private MultiStageOperator _input;

  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testShouldHandleUpstreamErrorBlocks() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    Mockito.when(_input.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!")));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build

    // Then:
    Mockito.verify(_input, Mockito.times(1)).nextBlock();
    Assert.assertTrue(block1.isErrorBlock(), "Input errors should propagate immediately");
  }

  @Test
  public void testShouldHandleEndOfStreamBlockWithNoOtherInputs() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    Mockito.when(_input.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(1)).nextBlock();
    Assert.assertTrue(block.isEndOfStreamBlock(), "EOS blocks should propagate");
  }

  @Test
  public void testShouldHandleUpstreamNoOpBlocksWhileConstructing() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 1}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build when reading NoOp block
    TransferableBlock block2 = operator.nextBlock(); // return when reading EOS block

    // Then:
    Mockito.verify(_input, Mockito.times(3)).nextBlock();
    Assert.assertTrue(block1.isNoOpBlock());
    Assert.assertEquals(block2.getContainer().size(), 1);
  }

  @Test
  public void testShouldHandleUpstreamNoOpBlocksWhileConstructingMultipleRows() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 1}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 2}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 2}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build when reading NoOp block
    TransferableBlock block2 = operator.nextBlock(); // build when reading NoOp block
    TransferableBlock block3 = operator.nextBlock(); // return when reading EOS block
    TransferableBlock block4 = operator.nextBlock(); // EOS block

    // Then:
    Mockito.verify(_input, Mockito.times(6)).nextBlock();
    Assert.assertTrue(block1.isNoOpBlock());
    Assert.assertTrue(block2.isNoOpBlock());
    Assert.assertEquals(block3.getContainer().size(), 3);
    Assert.assertEquals(block3.getContainer().get(0), new Object[]{1, 1, 3.0});
    Assert.assertEquals(block3.getContainer().get(1), new Object[]{1, 2, 3.0});
    Assert.assertEquals(block3.getContainer().get(2), new Object[]{2, 2, 2});
    Assert.assertTrue(block4.isEndOfStreamBlock());
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlock() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1, 1},
        "Expected three columns (original two columns, agg value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlockWithSameOrderByKeys() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    List<RexExpression> order = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, order,
        Arrays.asList(RelFieldCollation.Direction.ASCENDING), Arrays.asList(RelFieldCollation.NullDirection.LAST),
        calls, Integer.MIN_VALUE, Integer.MAX_VALUE, WindowNode.WindowFrameType.RANGE, Collections.emptyList(),
        outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1, 1},
        "Expected three columns (original two columns, agg value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlockWithoutPartitionByKeys() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE,
        Integer.MAX_VALUE, WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2,
        _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1, 1},
        "Expected three columns (original two columns, agg value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldWindowAggregateOverSingleInputBlockWithLiteralInput() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.Literal(FieldSpec.DataType.INT, 42)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    // second value is 1 (the literal) instead of 3 (the col val)
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 3, 42},
        "Expected three columns (original two columns, agg literal value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testShouldCallMergerWhenWindowAggregatingMultipleRows() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 1}, new Object[]{1, 2}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    AggregationUtils.Merger merger = Mockito.mock(AggregationUtils.Merger.class);
    Mockito.when(merger.merge(Mockito.any(), Mockito.any())).thenReturn(12d);
    Mockito.when(merger.initialize(Mockito.any(), Mockito.any())).thenReturn(1d);
    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema,
        ImmutableMap.of("SUM", cdt -> merger), 1, 2, _serverAddress);

    // When:
    TransferableBlock resultBlock = operator.nextBlock(); // (output result)

    // Then:
    // should call merger twice, one from second row in first block and two from the first row
    // in second block
    Mockito.verify(merger, Mockito.times(1)).initialize(Mockito.any(), Mockito.any());
    Mockito.verify(merger, Mockito.times(2)).merge(Mockito.any(), Mockito.any());
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{1, 1, 12d},
        "Expected three columns (original two columns, agg literal value)");
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{1, 2, 12d},
        "Expected three columns (original two columns, agg literal value)");
    Assert.assertEquals(resultBlock.getContainer().get(2), new Object[]{1, 3, 12d},
        "Expected three columns (original two columns, agg literal value)");
  }


  @Test
  public void testPartitionByWindowAggregateWithHashCollision() {
    MultiStageOperator upstreamOperator = OperatorTestUtil.getOperator(OperatorTestUtil.OP_1);
    // Create an aggregation call with sum for first column and group by second column.
    RexExpression.FunctionCall agg = getSum(new RexExpression.InputRef(0));
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, INT, DOUBLE});
    WindowAggregateOperator sum0PartitionBy1 =
        new WindowAggregateOperator(upstreamOperator, Arrays.asList(new RexExpression.InputRef(1)),
            Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Arrays.asList(agg),
            Integer.MIN_VALUE, Integer.MAX_VALUE, WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema,
            inSchema, 1, 2, _serverAddress);

    TransferableBlock result = sum0PartitionBy1.getNextBlock();
    while (result.isNoOpBlock()) {
      result = sum0PartitionBy1.getNextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{2, "BB", 5.0}, new Object[]{3, "BB", 5.0},
        new Object[]{1, "Aa", 1});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    Assert.assertEquals(resultRows.get(2), expectedRows.get(2));
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Unexpected aggregation "
      + "function name: AVERAGE.*")
  public void testShouldThrowOnUnknownAggFunction() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(
        new RexExpression.FunctionCall(SqlKind.AVG, FieldSpec.DataType.INT, "AVERAGE", ImmutableList.of()));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    DataSchema outSchema = new DataSchema(new String[]{"unknown"}, new DataSchema.ColumnDataType[]{DOUBLE});
    DataSchema inSchema = new DataSchema(new String[]{"unknown"}, new DataSchema.ColumnDataType[]{DOUBLE});

    // When:
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Unexpected aggregation "
      + "function name: ROW_NUMBER.*")
  public void testShouldThrowOnUnknownRankAggFunction() {
    // TODO: Remove this test when support is added for RANK functions
    // Given:
    List<RexExpression> calls = ImmutableList.of(
        new RexExpression.FunctionCall(SqlKind.ROW_NUMBER, FieldSpec.DataType.INT, "ROW_NUMBER", ImmutableList.of()));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    DataSchema outSchema = new DataSchema(new String[]{"unknown"}, new DataSchema.ColumnDataType[]{DOUBLE});
    DataSchema inSchema = new DataSchema(new String[]{"unknown"}, new DataSchema.ColumnDataType[]{DOUBLE});

    // When:
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Order by is not yet "
      + "supported in window functions")
  public void testShouldThrowOnNonEmptyOrderByKeysNotMatchingPartitionByKeys() {
    // TODO: Remove this test once order by support is added
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    List<RexExpression> order = ImmutableList.of(new RexExpression.InputRef(1));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, STRING, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, order,
        Arrays.asList(RelFieldCollation.Direction.ASCENDING), Arrays.asList(RelFieldCollation.NullDirection.LAST),
        calls, Integer.MIN_VALUE, Integer.MAX_VALUE, WindowNode.WindowFrameType.RANGE, Collections.emptyList(),
        outSchema, inSchema, 1, 2, _serverAddress);
  }

  @Test
  public void testShouldThrowOnNonEmptyOrderByKeysMatchingPartitionByKeysWithDifferentDirection() {
    // Given:
    // Set ORDER BY key same as PARTITION BY key with custom direction and null direction. Should still be treated
    // like a PARTITION BY only query (since the final aggregation value won't change).
    // TODO: Test null direction handling once support for it is available
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(0)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(1));
    List<RexExpression> order = ImmutableList.of(new RexExpression.InputRef(1));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "bar"}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{3, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, STRING, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, order,
        Arrays.asList(RelFieldCollation.Direction.DESCENDING), Arrays.asList(RelFieldCollation.NullDirection.LAST),
        calls, Integer.MIN_VALUE, Integer.MAX_VALUE, WindowNode.WindowFrameType.RANGE, Collections.emptyList(),
        outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock resultBlock = operator.nextBlock(); // (output result)

    // Then:
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{2, "bar", 2},
        "Expected three columns (original two columns, agg literal value)");
    Assert.assertEquals(resultBlock.getContainer().get(1), new Object[]{2, "foo", 5.0},
        "Expected three columns (original two columns, agg literal value)");
    Assert.assertEquals(resultBlock.getContainer().get(2), new Object[]{3, "foo", 5.0},
        "Expected three columns (original two columns, agg literal value)");
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Only RANGE type frames "
      + "are supported at present")
  public void testShouldThrowOnCustomFramesRows() {
    // TODO: Remove this test once custom frame support is added
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, STRING, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.ROW, Collections.emptyList(), outSchema, inSchema, 1, 2,
        _serverAddress);
  }

  @Test
  public void testShouldNotThrowCurrentRowPartitionByOrderByOnSameKey() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(0)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(1));
    List<RexExpression> order = ImmutableList.of(new RexExpression.InputRef(1));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, STRING, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, order,
        Arrays.asList(RelFieldCollation.Direction.ASCENDING), Arrays.asList(RelFieldCollation.NullDirection.LAST),
        calls, Integer.MIN_VALUE, 0, WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema,
        inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, "foo", 2},
        "Expected three columns (original two columns, agg value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Only default frame is "
      + "supported, lowerBound must be UNBOUNDED PRECEDING")
  public void testShouldThrowOnCustomFramesCustomPreceding() {
    // TODO: Remove this test once custom frame support is added
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, STRING, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, 5, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2,
        _serverAddress);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Only default frame is "
      + "supported, upperBound must be UNBOUNDED FOLLOWING or CURRENT ROW")
  public void testShouldThrowOnCustomFramesCustomFollowing() {
    // TODO: Remove this test once custom frame support is added
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, STRING, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, 5,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);
  }

  @Test
  public void testShouldReturnErrorBlockOnUnexpectedInputType() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, STRING});
    // TODO: it is necessary to produce two values here, the operator only throws on second
    // (see the comment in WindowAggregate operator)
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "metallica"}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "pink floyd"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "arg", "sum"},
        new DataSchema.ColumnDataType[]{INT, STRING, DOUBLE});
    WindowAggregateOperator operator = new WindowAggregateOperator(_input, group, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), calls, Integer.MIN_VALUE, Integer.MAX_VALUE,
        WindowNode.WindowFrameType.RANGE, Collections.emptyList(), outSchema, inSchema, 1, 2, _serverAddress);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Assert.assertTrue(block.isErrorBlock(), "expected ERROR block from invalid computation");
    Assert.assertTrue(block.getDataBlock().getExceptions().get(1000).contains("String cannot be cast to class"),
        "expected it to fail with class cast exception");
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(SqlKind.SUM, FieldSpec.DataType.INT, "SUM", ImmutableList.of(arg));
  }
}
