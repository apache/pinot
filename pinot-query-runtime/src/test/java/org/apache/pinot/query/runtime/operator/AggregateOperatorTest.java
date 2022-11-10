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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
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


public class AggregateOperatorTest {

  private AutoCloseable _mocks;

  @Mock
  private Operator<TransferableBlock> _input;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
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

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = new AggregateOperator(_input, outSchema, calls, group);

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

    Mockito.when(_input.nextBlock())
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = new AggregateOperator(_input, outSchema, calls, group);

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
    Mockito.when(_input.nextBlock())
        .thenReturn(block(inSchema, new Object[]{1, 1}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = new AggregateOperator(_input, outSchema, calls, group);

    // When:
    TransferableBlock block1 = operator.nextBlock(); // build
    TransferableBlock block2 = operator.nextBlock(); // get no-op

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.isNoOpBlock(), "First block should be no-op (not yet constructed)");
    Assert.assertTrue(block2.isNoOpBlock(), "Second block should be no-op (done construct)");
  }

  @Test
  public void shouldAggregateSingleInputBlock() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock())
        .thenReturn(block(inSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = new AggregateOperator(_input, outSchema, calls, group);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();
    TransferableBlock block3 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.isNoOpBlock(), "First block should be no-op (not yet constructed)");
    Assert.assertTrue(block2.getNumRows() > 0, "Second block is the result");
    Assert.assertEquals(block2.getContainer().iterator().next(), new Object[]{2, 1},
        "Expected two columns (group by key, agg value)");
    Assert.assertTrue(block3.isEndOfStreamBlock(), "Third block is EOS (done processing)");
  }

  @Test
  public void shouldAggregateSingleInputBlockWithLiteralInput() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.Literal(FieldSpec.DataType.INT, 1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock())
        .thenReturn(block(inSchema, new Object[]{2, 3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = new AggregateOperator(_input, outSchema, calls, group);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();
    TransferableBlock block3 = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(2)).nextBlock();
    Assert.assertTrue(block1.isNoOpBlock(), "First block should be no-op (not yet constructed)");
    Assert.assertTrue(block2.getNumRows() > 0, "Second block is the result");
    // second value is 1 (the literal) instead of 3 (the col val)
    Assert.assertEquals(block2.getContainer().iterator().next(), new Object[]{2, 1},
        "Expected two columns (group by key, agg value)");
    Assert.assertTrue(block3.isEndOfStreamBlock(), "Third block is EOS (done processing)");
  }

  @Test
  public void shouldCallMergerWhenAggregatingMultipleRows() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, INT});
    Mockito.when(_input.nextBlock())
        .thenReturn(block(inSchema, new Object[]{1, 1}, new Object[]{1, 1}))
        .thenReturn(block(inSchema, new Object[]{1, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    AggregateOperator.Merger merger = Mockito.mock(AggregateOperator.Merger.class);
    Mockito.when(merger.apply(Mockito.any(), Mockito.any())).thenReturn(12d);
    DataSchema outSchema = new DataSchema(new String[]{"sum"}, new ColumnDataType[]{DOUBLE});
    AggregateOperator operator = new AggregateOperator(_input, outSchema, calls, group, ImmutableMap.of(
        "SUM", merger
    ));

    // When:
    operator.nextBlock(); // first block consume
    operator.nextBlock(); // second block consume (done build)
    TransferableBlock resultBlock = operator.nextBlock(); // (output result)

    // Then:
    // should call merger twice, one from second row in first block and two from the first row
    // in second block
    Mockito.verify(merger, Mockito.times(2)).apply(Mockito.any(), Mockito.any());
    Assert.assertEquals(resultBlock.getContainer().iterator().next(), new Object[]{1, 12d},
        "Expected two columns (group by key, agg value)");
  }

  @Test
  public void testGroupByAggregateWithHashCollision() {
    BaseOperator<TransferableBlock> upstreamOperator = OperatorTestUtil.getOperator(OperatorTestUtil.OP_1);
    // Create an aggregation call with sum for first column and group by second column.
    RexExpression.FunctionCall agg = getSum(new RexExpression.InputRef(0));
    AggregateOperator sum0GroupBy1 =
        new AggregateOperator(upstreamOperator, OperatorTestUtil.getDataSchema(OperatorTestUtil.OP_1),
            Arrays.asList(agg), Arrays.asList(new RexExpression.InputRef(1)));
    TransferableBlock result = sum0GroupBy1.getNextBlock();
    while (result.isNoOpBlock()) {
      result = sum0GroupBy1.getNextBlock();
    }
    Collection<Object[]> resultRows = result.getContainer();
    Iterator<Object[]> resultIt = resultRows.iterator();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{"Aa", 1}, new Object[]{"BB", 5.0});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultIt.next(), expectedRows.get(0));
    Assert.assertEquals(resultIt.next(), expectedRows.get(1));
  }

  @Test(
      expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*Unexpected value: AVERAGE.*")
  public void shouldThrowOnUnknownAggFunction() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(
        new RexExpression.FunctionCall(SqlKind.AVG, FieldSpec.DataType.INT, "AVERAGE", ImmutableList.of())
    );
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    DataSchema outSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});

    // When:
    AggregateOperator operator = new AggregateOperator(_input, outSchema, calls, group);
  }

  private static TransferableBlock block(DataSchema schema, Object[]... rows) {
    return new TransferableBlock(Arrays.asList(rows), schema, DataBlock.Type.ROW);
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(SqlKind.SUM, FieldSpec.DataType.INT, "SUM", ImmutableList.of(arg));
  }
}
