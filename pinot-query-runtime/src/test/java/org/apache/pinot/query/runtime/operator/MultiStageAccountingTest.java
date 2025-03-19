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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockTestUtils;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.ITest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.DOUBLE;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MultiStageAccountingTest implements ITest {
  private AutoCloseable _mocks;
  @Mock
  private VirtualServerAddress _serverAddress;

  protected String _testName;
  protected MultiStageOperator _operator;

  @Factory(dataProvider = "operatorProvider")
  public MultiStageAccountingTest(String testName, MultiStageOperator operator) {
    _testName = testName;
    _operator = operator;
  }

  @BeforeClass
  public static void setUpClass() {
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    configs.put(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    // init accountant and start watcher task
    Tracing.ThreadAccountantOps.initializeThreadAccountant(new PinotConfiguration(configs), "testGroupBy");

    // Setup Thread Context
    Tracing.ThreadAccountantOps.setupRunner("MultiStageAccountingTest", ThreadExecutionContext.TaskType.MSE);
    ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
    Tracing.ThreadAccountantOps.setupWorker(1, ThreadExecutionContext.TaskType.MSE, threadExecutionContext);
  }

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
  public void testOperatorAccounting() {
    _operator.nextBlock().getContainer();

    ThreadResourceUsageAccountant threadAccountant = Tracing.getThreadAccountant();
    Collection<? extends QueryResourceTracker> queryResourceTrackers = threadAccountant.getQueryResources().values();
    Collection<? extends ThreadResourceTracker> threadResourceTrackers = threadAccountant.getThreadResources();

    // Then:
    assertEquals(queryResourceTrackers.size(), 1);
    assertEquals(threadResourceTrackers.size(), 1);
    assertTrue(queryResourceTrackers.iterator().next().getAllocatedBytes() > 0);
    assertTrue(threadResourceTrackers.iterator().next().getAllocatedBytes() > 0);
    assertTrue(_operator.nextBlock().isSuccessfulEndOfStreamBlock(), "Second block is EOS (done processing)");
  }

  @DataProvider(name = "operatorProvider")
  public static Object[][] getOperators() {
    return new Object[][]{
        {"AggregateOperator", getAggregateOperator()},
        {"SortOperator", getSortOperator()},
        {"HashJoinOperator", getHashJoinOperator()},
        {"WindowAggregateOperator", getWindowAggregateOperator()},
        {"SetOperator", getIntersectOperator()}
    };
  }

  private static MultiStageOperator getAggregateOperator() {
    MultiStageOperator input = Mockito.mock();
    // Given:
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    List<Integer> filterArgs = List.of(-1);
    List<Integer> groupKeys = List.of(0);
    DataSchema inSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, DOUBLE});
    when(input.nextBlock()).thenReturn(OperatorTestUtil.block(inSchema, new Object[]{2, 1.0}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"group", "sum"}, new DataSchema.ColumnDataType[]{INT, DOUBLE});
    return new AggregateOperator(OperatorTestUtil.getTracingContext(), input,
        new AggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), aggCalls, filterArgs, groupKeys,
            AggregateNode.AggType.DIRECT, false, null, 0));
  }

  private static MultiStageOperator getHashJoinOperator() {
    MultiStageOperator leftInput = Mockito.mock();
    MultiStageOperator rightInput = Mockito.mock();

    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    when(leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_co2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    return new HashJoinOperator(OperatorTestUtil.getTracingContext(), leftInput, leftSchema, rightInput,
        new JoinNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.INNER, List.of(0), List.of(0),
            List.of(), JoinNode.JoinStrategy.HASH));
  }

  private static MultiStageOperator getSortOperator() {
    MultiStageOperator input = Mockito.mock();
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(input.nextBlock()).thenReturn(
            new TransferableBlock(List.of(new Object[]{2}, new Object[]{1}), schema, DataBlock.Type.ROW))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));
    return new SortOperator(OperatorTestUtil.getTracingContext(), input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, 10, 0));
  }

  private static MultiStageOperator getWindowAggregateOperator() {
    MultiStageOperator input = Mockito.mock();
    // Given:
    DataSchema inputSchema = new DataSchema(new String[]{"group", "arg"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, new Object[]{2, 1}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"group", "arg", "sum"}, new DataSchema.ColumnDataType[]{
        INT, INT, DOUBLE
    });
    List<Integer> keys = List.of(0);
    List<RexExpression.FunctionCall> aggCalls = List.of(getSum(new RexExpression.InputRef(1)));
    return new WindowAggregateOperator(OperatorTestUtil.getTracingContext(), input, inputSchema,
        new WindowNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), keys, List.of(), aggCalls,
            WindowNode.WindowFrameType.RANGE, Integer.MIN_VALUE, Integer.MAX_VALUE, List.of()));
  }

  private static MultiStageOperator getIntersectOperator() {
    MultiStageOperator leftOperator = Mockito.mock();
    MultiStageOperator rightOperator = Mockito.mock();

    DataSchema schema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(schema, new Object[]{1, "AA"}, new Object[]{2, "BB"}, new Object[]{3, "CC"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    Mockito.when(rightOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(schema, new Object[]{1, "AA"}, new Object[]{2, "BB"}, new Object[]{4, "DD"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));

    return new IntersectOperator(OperatorTestUtil.getTracingContext(), ImmutableList.of(leftOperator, rightOperator),
        schema);
  }

  private static RexExpression.FunctionCall getSum(RexExpression arg) {
    return new RexExpression.FunctionCall(DataSchema.ColumnDataType.INT, SqlKind.SUM.name(), List.of(arg));
  }

  @Override
  public String getTestName() {
    return _testName;
  }
}
