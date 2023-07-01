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
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
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

// TODO(Sonam): Ensure test passes when the switch to AggregateOperator is made.
public class AggregateOperatorTest {

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
  public void shouldHandleUpstreamErrorBlocks() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    Mockito.when(_input.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!")));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build

    // Then:
    Mockito.verify(_input, Mockito.times(1)).nextBlock();
    Assert.assertTrue(block1.isErrorBlock(), "Input errors should propagate immediately");
  }

  @Test
  public void shouldHandleEndOfStreamBlockWithNoOtherInputs() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    Mockito.when(_input.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(1)).nextBlock();
    Assert.assertTrue(block.isEndOfStreamBlock(), "EOS blocks should propagate");
  }

  @Test
  public void shouldHandleUpstreamNoOpBlocksWhileConstructing() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 1}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build when reading NoOp block
    TransferableBlock block2 = operator.nextBlock(); // return when reading EOS block

    // Then:
    Mockito.verify(_input, Mockito.times(3)).nextBlock();
    Assert.assertTrue(block1.isNoOpBlock());
    Assert.assertEquals(block2.getContainer().size(), 1);
  }

  @Test
  public void shouldAggregateSingleInputBlock() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1},
        "Expected two columns (group by key, agg value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void shouldAggregateSingleInputBlockWithLiteralInput() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.Literal(FieldSpec.DataType.INT, 1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    // second value is 1 (the literal) instead of 3 (the col val)
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1},
        "Expected two columns (group by key, agg value)");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void shouldCallMergerWhenAggregatingMultipleRows() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 1}, new Object[]{1, 1}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{1, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    AggregationUtils.Merger merger = Mockito.mock(AggregationUtils.Merger.class);
    Mockito.when(merger.merge(Mockito.any(), Mockito.any())).thenReturn(12d);
    Mockito.when(merger.init(Mockito.any(), Mockito.any())).thenReturn(1d);
    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema,
            ImmutableMap.of("SUM", cdt -> merger));

    // When:
    TransferableBlock resultBlock = operator.nextBlock(); // (output result)

    // Then:
    // should call merger twice, one from second row in first block and two from the first row
    // in second block
    Mockito.verify(merger, Mockito.times(1)).init(Mockito.any(), Mockito.any());
    Mockito.verify(merger, Mockito.times(2)).merge(Mockito.any(), Mockito.any());
    Assert.assertEquals(resultBlock.getContainer().get(0), new Object[]{1, 12d},
        "Expected two columns (group by key, agg value)");
  }

  @Test
  public void testGroupByAggregateWithHashCollision() {
    MultiStageOperator upstreamOperator = OperatorTestUtil.getOperator(OperatorTestUtil.OP_1);
    // Create an aggregation call with sum for first column and group by second column.
    RexExpression.FunctionCall agg = getSum(new RexExpression.InputRef(0));
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    AggregateOperator sum0GroupBy1 = new AggregateOperator(OperatorTestUtil.getDefaultContext(), upstreamOperator,
        OperatorTestUtil.getDataSchema(OperatorTestUtil.OP_1), Collections.singletonList(agg),
        Collections.singletonList(new RexExpression.InputRef(1)), inSchema);
    TransferableBlock result = sum0GroupBy1.getNextBlock();
    while (result.isNoOpBlock()) {
      result = sum0GroupBy1.getNextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{"Aa", 1}, new Object[]{"BB", 5.0});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Unexpected value: "
      + "AVERAGE.*")
  public void shouldThrowOnUnknownAggFunction() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(
        new RexExpression.FunctionCall(SqlKind.AVG, FieldSpec.DataType.INT, "AVERAGE", ImmutableList.of()));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    DataSchema outSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});
    DataSchema inSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});

    // When:
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema);
  }

  @Test
  public void shouldReturnErrorBlockOnUnexpectedInputType() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, STRING});
    Mockito.when(_input.nextBlock())
        // TODO: it is necessary to produce two values here, the operator only throws on second
        // (see the comment in Aggregate operator)
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, "foo"}, new Object[]{2, "foo"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, inSchema);

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
