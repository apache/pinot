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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMaxStepAggregationFunction;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.ReceiverSpec;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// A spool whose receiver stages read the same block in different ways, as their [ReceiverSpec]s say.
public class MailboxSendOperatorReceiverSpecTest {
  private static final int SENDER_STAGE_ID = 5;
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"k", "v"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private MultiStageOperator _input;
  @Mock
  private SendingMailbox _mailboxOfStage2;
  @Mock
  private SendingMailbox _mailboxOfStage3;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(1234);
    when(_mailboxService.getSendingMailbox(eq("host2"), eq(2), anyLong(), anyString(), anyLong(), any()))
        .thenReturn(_mailboxOfStage2);
    when(_mailboxService.getSendingMailbox(eq("host3"), eq(3), anyLong(), anyString(), anyLong(), any()))
        .thenReturn(_mailboxOfStage3);
    when(_input.calculateStats()).thenReturn(MultiStageQueryStats.emptyStats(SENDER_STAGE_ID));
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void eachReceiverStageGetsTheColumnsOfItsSpec() {
    MailboxSendNode node = new MailboxSendNode(SENDER_STAGE_ID, SCHEMA, List.of(), List.of(2, 3),
        PinotRelExchangeType.STREAMING, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), false, null, false,
        KeySelector.DEFAULT_HASH_ALGORITHM);
    node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), List.of(1)));
    RowHeapDataBlock block = new RowHeapDataBlock(List.of(new Object[]{1, "a"}, new Object[]{2, "b"}), SCHEMA);
    when(_input.nextBlock()).thenReturn(block, SuccessMseBlock.INSTANCE);

    MailboxSendOperator operator = new MailboxSendOperator(context(), _input, node);
    operator.nextBlock();

    ArgumentCaptor<MseBlock.Data> stage2 = ArgumentCaptor.forClass(MseBlock.Data.class);
    verify(_mailboxOfStage2).send(stage2.capture());
    assertEquals(stage2.getValue().getDataSchema(), SCHEMA);
    assertRows(stage2.getValue().asRowHeap().getRows(), List.of(new Object[]{1, "a"}, new Object[]{2, "b"}));

    ArgumentCaptor<MseBlock.Data> stage3 = ArgumentCaptor.forClass(MseBlock.Data.class);
    verify(_mailboxOfStage3).send(stage3.capture());
    assertEquals(stage3.getValue().getDataSchema(),
        new DataSchema(new String[]{"v"}, new ColumnDataType[]{ColumnDataType.STRING}));
    assertRows(stage3.getValue().asRowHeap().getRows(), List.of(new Object[]{"a"}, new Object[]{"b"}));
  }

  /// A block with aggregation intermediate results (an OBJECT column) reaches a local receiver by reference and a
  /// remote receiver through a column mask. The remote receiver reads the masked view, with the aggregation function
  /// of each column, before the local receiver gets the original objects and can change them.
  @Test
  public void objectColumnsReachMaskedReceivers() {
    DataSchema schema = new DataSchema(new String[]{"key", "other", "funnel"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.OBJECT});
    AggregationFunction aggFunction = new FunnelMaxStepAggregationFunction(List.of(
        ExpressionContext.forIdentifier("ts"), ExpressionContext.forLiteral(Literal.longValue(1000L)),
        ExpressionContext.forLiteral(Literal.intValue(2)), ExpressionContext.forIdentifier("step0"),
        ExpressionContext.forIdentifier("step1")), false);
    PriorityQueue<FunnelStepEvent> stepEvents = new PriorityQueue<>(List.of(new FunnelStepEvent(1000L, 0)));
    RowHeapDataBlock block = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"user0", 7, stepEvents}), schema,
        new AggregationFunction[]{null, null, aggFunction});
    when(_input.nextBlock()).thenReturn(block, SuccessMseBlock.INSTANCE);
    when(_mailboxOfStage2.isLocal()).thenReturn(true);
    when(_mailboxOfStage2.deliversByReference()).thenReturn(true);

    MailboxSendNode node = new MailboxSendNode(SENDER_STAGE_ID, schema, List.of(), List.of(2, 3),
        PinotRelExchangeType.STREAMING, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), false, null, false,
        KeySelector.DEFAULT_HASH_ALGORITHM);
    node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), List.of(2, 0)));
    MailboxSendOperator operator = new MailboxSendOperator(context(), _input, node);
    operator.nextBlock();

    ArgumentCaptor<MseBlock.Data> stage2 = ArgumentCaptor.forClass(MseBlock.Data.class);
    ArgumentCaptor<MseBlock.Data> stage3 = ArgumentCaptor.forClass(MseBlock.Data.class);
    InOrder inOrder = inOrder(_mailboxOfStage3, _mailboxOfStage2);
    inOrder.verify(_mailboxOfStage3).send(stage3.capture());
    inOrder.verify(_mailboxOfStage2).send(stage2.capture());
    assertEquals(stage2.getValue().getDataSchema(), schema);
    RowHeapDataBlock remote = stage3.getValue().asRowHeap();
    assertEquals(remote.getDataSchema(), new DataSchema(new String[]{"funnel", "key"},
        new ColumnDataType[]{ColumnDataType.OBJECT, ColumnDataType.STRING}));
    assertSame(remote.getAggFunctions()[0], aggFunction);
    assertNull(remote.getAggFunctions()[1]);
    // The remote mailbox serializes the masked block with the function of its OBJECT column.
    DataBlock serialized = remote.asSerialized().getDataBlock();
    assertEquals(serialized.getNumberOfRows(), 1);
    assertEquals(serialized.getNumberOfColumns(), 2);
    assertEquals(serialized.getString(0, 1), "user0");
  }

  private OpChainExecutionContext context() {
    Map<Integer, MailboxInfos> mailboxInfos = Map.of(
        2, new MailboxInfos(new MailboxInfo("host2", 2, List.of(0))),
        3, new MailboxInfos(new MailboxInfo("host3", 3, List.of(0))));
    WorkerMetadata workerMetadata = new WorkerMetadata(0, mailboxInfos, Map.of());
    StageMetadata stageMetadata = new StageMetadata(SENDER_STAGE_ID, List.of(workerMetadata), Map.of());
    return OpChainExecutionContext.fromQueryContext(_mailboxService, Map.of(), stageMetadata, workerMetadata, null,
        true, true, QueryExecutionContext.forMseTest());
  }

  private static void assertRows(List<Object[]> actual, List<Object[]> expected) {
    assertTrue(Arrays.deepEquals(actual.toArray(), expected.toArray()),
        Arrays.deepToString(actual.toArray()) + " != " + Arrays.deepToString(expected.toArray()));
  }
}
