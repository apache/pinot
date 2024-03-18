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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BOOLEAN;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;


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

  private static AbstractPlanNode.NodeHint getAggHints(Map<String, String> hintsMap) {
    RelHint.Builder relHintBuilder = RelHint.builder(PinotHintOptions.AGGREGATE_HINT_OPTIONS);
    hintsMap.forEach(relHintBuilder::hintOption);
    return new AbstractPlanNode.NodeHint(ImmutableList.of(relHintBuilder.build()));
  }

  @Test
  public void shouldHandleUpstreamErrorBlocks() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    Mockito.when(_input.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!")));

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, AggType.DIRECT,
            Collections.singletonList(-1), null);

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

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, AggType.DIRECT,
            Collections.singletonList(-1), null);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Mockito.verify(_input, Mockito.times(1)).nextBlock();
    Assert.assertTrue(block.isEndOfStreamBlock(), "EOS blocks should propagate");
  }

  @Test
  public void testAggregateSingleInputBlock() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    Mockito.when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, AggType.DIRECT,
            Collections.singletonList(-1), null);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 1.0},
        "Expected two columns (group by key, agg value), agg value is final result");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testAggregateMultipleInputBlocks() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}, new Object[]{2, 2.0}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3.0}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, AggType.DIRECT,
            Collections.singletonList(-1), null);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 6.0},
        "Expected two columns (group by key, agg value), agg value is final result");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testAggregateWithFilter() {
    // Given:
    List<RexExpression> calls =
        ImmutableList.of(getSum(new RexExpression.InputRef(1)), getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    List<Integer> filterArgIds = ImmutableList.of(-1, 2);

    DataSchema inSchema =
        new DataSchema(new String[]{"group", "arg", "filterArg"}, new ColumnDataType[]{INT, DOUBLE, BOOLEAN});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0, 0}, new Object[]{2, 2.0, 1}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 3.0, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema =
        new DataSchema(new String[]{"group", "sum", "sumWithFilter"}, new ColumnDataType[]{INT, DOUBLE, DOUBLE});
    AggregateOperator operator =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, AggType.DIRECT,
            filterArgIds, null);

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then:
    Assert.assertTrue(block1.getNumRows() > 0, "First block is the result");
    Assert.assertEquals(block1.getContainer().get(0), new Object[]{2, 6.0, 5.0},
        "Expected three columns (group by key, agg value, agg value with filter), agg value is final result");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @Test
  public void testGroupByAggregateWithHashCollision() {
    MultiStageOperator upstreamOperator = OperatorTestUtil.getOperator(OperatorTestUtil.OP_1);
    // Create an aggregation call with sum for first column and group by second column.
    RexExpression.FunctionCall agg = getSum(new RexExpression.InputRef(0));
    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{STRING, DOUBLE});
    AggregateOperator sum0GroupBy1 =
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), upstreamOperator, outSchema,
            Collections.singletonList(agg), Collections.singletonList(new RexExpression.InputRef(1)), AggType.DIRECT,
            Collections.singletonList(-1), null);
    TransferableBlock result = sum0GroupBy1.getNextBlock();
    List<Object[]> resultRows = result.getContainer();
    Assert.assertEquals(resultRows.size(), 2);
    if (resultRows.get(0)[0].equals("Aa")) {
      Assert.assertEquals(resultRows.get(0), new Object[]{"Aa", 1.0});
      Assert.assertEquals(resultRows.get(1), new Object[]{"BB", 5.0});
    } else {
      Assert.assertEquals(resultRows.get(0), new Object[]{"BB", 5.0});
      Assert.assertEquals(resultRows.get(1), new Object[]{"Aa", 1.0});
    }
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*AVERAGE.*")
  public void shouldThrowOnUnknownAggFunction() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(
        new RexExpression.FunctionCall(SqlKind.AVG, ColumnDataType.INT, "AVERAGE", ImmutableList.of()));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));
    DataSchema outSchema = new DataSchema(new String[]{"unknown"}, new ColumnDataType[]{DOUBLE});

    // When:
    new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group, AggType.DIRECT,
        Collections.singletonList(-1), null);
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
        new AggregateOperator(OperatorTestUtil.getDefaultContext(), _input, outSchema, calls, group,
            AggType.INTERMEDIATE, Collections.singletonList(-1), null);

    // When:
    TransferableBlock block = operator.nextBlock();

    // Then:
    Assert.assertTrue(block.isErrorBlock(), "expected ERROR block from invalid computation");
    Assert.assertTrue(block.getExceptions().get(1000).contains("cannot be cast to class"),
        "expected it to fail with class cast exception");
  }

  @Test
  public void shouldHandleGroupLimitExceed() {
    // Given:
    List<RexExpression> calls = ImmutableList.of(getSum(new RexExpression.InputRef(1)));
    List<RexExpression> group = ImmutableList.of(new RexExpression.InputRef(0));

    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new ColumnDataType[]{INT, DOUBLE});
    Mockito.when(_input.nextBlock())
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}, new Object[]{3, 2.0}))
        .thenReturn(OperatorTestUtil.block(inSchema, new Object[]{3, 3.0}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema outSchema = new DataSchema(new String[]{"group", "sum"}, new ColumnDataType[]{INT, DOUBLE});
    OpChainExecutionContext context = OperatorTestUtil.getDefaultContext();
    Map<String, String> hintsMap = ImmutableMap.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "1");
    AggregateOperator operator =
        new AggregateOperator(context, _input, outSchema, calls, group, AggType.DIRECT, Collections.singletonList(-1),
            getAggHints(hintsMap));

    // When:
    TransferableBlock block1 = operator.nextBlock();
    TransferableBlock block2 = operator.nextBlock();

    // Then
    Mockito.verify(_input).earlyTerminate();

    // Then:
    Assert.assertTrue(block1.getNumRows() == 1, "when group limit reach it should only return that many groups");
    Assert.assertTrue(block2.isEndOfStreamBlock(), "Second block is EOS (done processing)");
    String operatorId =
        Joiner.on("_").join(AggregateOperator.class.getSimpleName(), context.getStageId(), context.getServer());
    OperatorStats operatorStats = context.getStats().getOperatorStats(context, operatorId);
    Assert.assertEquals(operatorStats.getExecutionStats().get(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName()),
        "true");
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(SqlKind.SUM, ColumnDataType.INT, "SUM", ImmutableList.of(arg));
  }
}
