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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.HashFunctionSelector;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryExecutionContext.QueryType;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests materialized mailbox send and receive operator behavior.
public class MaterializedMailboxOperatorTest {
  private static final long REQUEST_ID = 0L;
  private static final int PRODUCER_STAGE_ID = 1;
  private static final int CONSUMER_STAGE_ID = 2;
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("consumer");
    when(_mailboxService.getPort()).thenReturn(1234);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testMaterializedSendCreatesOneLocalPartitionPerConsumerWorker() {
    SendingMailbox partition0 = mock(SendingMailbox.class);
    SendingMailbox partition1 = mock(SendingMailbox.class);
    when(partition0.isLocal()).thenReturn(true);
    when(partition1.isLocal()).thenReturn(true);
    when(_mailboxService.getMaterializedSendingMailbox(eq(REQUEST_ID), eq(PRODUCER_STAGE_ID), eq(0), eq(0),
        any(), any())).thenReturn(partition0);
    when(_mailboxService.getMaterializedSendingMailbox(eq(REQUEST_ID), eq(PRODUCER_STAGE_ID), eq(0), eq(1),
        any(), any())).thenReturn(partition1);

    WorkerMetadata worker = new WorkerMetadata(0,
        Map.of(CONSUMER_STAGE_ID,
            new SharedMailboxInfos(new MailboxInfo("consumer", 1234, List.of(0, 1)))),
        Map.of());
    StageMetadata stage = new StageMetadata(PRODUCER_STAGE_ID, List.of(worker), Map.of());
    OpChainExecutionContext context = context(stage, worker);
    MultiStageOperator input = mock(MultiStageOperator.class);
    when(input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    when(input.calculateStats()).thenReturn(MultiStageQueryStats.emptyStats(PRODUCER_STAGE_ID));
    MailboxSendNode node = new MailboxSendNode(PRODUCER_STAGE_ID, DATA_SCHEMA, List.of(), CONSUMER_STAGE_ID,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), false, List.of(), false,
        HashFunctionSelector.MURMUR2);
    node.setMaterialized(true);

    MseBlock result = new MailboxSendOperator(context, input, node).nextBlock();

    assertTrue(result.isSuccess());
    verify(partition0).send(any(MseBlock.Eos.class), eq(List.of()));
    verify(partition1).send(any(MseBlock.Eos.class), eq(List.of()));
  }

  @Test
  public void testMaterializedReceiveReadsItsLogicalPartitionFromEveryProducer() {
    MseBlock.Data block0 = OperatorTestUtil.block(DATA_SCHEMA, new Object[]{10});
    MseBlock.Data block1 = OperatorTestUtil.block(DATA_SCHEMA, new Object[]{11});
    MseBlock.Data block2 = OperatorTestUtil.block(DATA_SCHEMA, new Object[]{12});
    when(_mailboxService.readMaterializedPartition("producer-a", 1111, REQUEST_ID, PRODUCER_STAGE_ID, 0, 1,
        Long.MAX_VALUE)).thenReturn(List.of(block0).iterator());
    when(_mailboxService.readMaterializedPartition("producer-a", 1111, REQUEST_ID, PRODUCER_STAGE_ID, 2, 1,
        Long.MAX_VALUE)).thenReturn(List.of(block1).iterator());
    when(_mailboxService.readMaterializedPartition("producer-b", 2222, REQUEST_ID, PRODUCER_STAGE_ID, 1, 1,
        Long.MAX_VALUE)).thenReturn(List.of(block2).iterator());

    MaterializedMailboxReceiveOperator operator = receiveOperator(1, Long.MAX_VALUE);

    assertSame(operator.nextBlock(), block0);
    assertSame(operator.nextBlock(), block1);
    assertSame(operator.nextBlock(), block2);
    assertTrue(operator.nextBlock().isSuccess());
    StatMap<BaseMailboxReceiveOperator.StatKey> stats = operator.copyStatMaps();
    assertEquals(stats.getInt(BaseMailboxReceiveOperator.StatKey.FAN_IN), 3);
    assertEquals(stats.getLong(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS), 3L);
  }

  @Test
  public void testMaterializedReceiveUsesAssignedHandles() {
    MseBlock.Data block0 = OperatorTestUtil.block(DATA_SCHEMA, new Object[]{10});
    MseBlock.Data block1 = OperatorTestUtil.block(DATA_SCHEMA, new Object[]{11});
    when(_mailboxService.readMaterializedPartition("assigned-a", 3333, REQUEST_ID, PRODUCER_STAGE_ID, 2, 7,
        Long.MAX_VALUE)).thenReturn(List.of(block0).iterator());
    when(_mailboxService.readMaterializedPartition("assigned-b", 4444, REQUEST_ID, PRODUCER_STAGE_ID, 5, 8,
        Long.MAX_VALUE)).thenReturn(List.of(block1).iterator());
    List<Worker.MaterializedPartitionHandle> handles = List.of(
        materializedHandle(2, 7, "assigned-a", 3333),
        materializedHandle(5, 8, "assigned-b", 4444));
    WorkerMetadata worker = new WorkerMetadata(0, Map.of(PRODUCER_STAGE_ID,
        new SharedMailboxInfos(new MailboxInfo("fallback", 9999, List.of(9)))), Map.of(), handles);
    StageMetadata stage = new StageMetadata(CONSUMER_STAGE_ID, List.of(worker), Map.of());

    MaterializedMailboxReceiveOperator operator = new MaterializedMailboxReceiveOperator(
        context(stage, worker, Long.MAX_VALUE), receiveNode(RelDistribution.Type.HASH_DISTRIBUTED));

    assertSame(operator.nextBlock(), block0);
    assertSame(operator.nextBlock(), block1);
    assertTrue(operator.nextBlock().isSuccess());
    verify(_mailboxService, times(0)).readMaterializedPartition(eq("fallback"), eq(9999), eq(REQUEST_ID),
        eq(PRODUCER_STAGE_ID), eq(9), eq(0), eq(Long.MAX_VALUE));
  }

  @Test
  public void testEarlyTerminationDrainsUnreadPartitions() {
    @SuppressWarnings("unchecked")
    Iterator<MseBlock.Data> first = mock(Iterator.class);
    @SuppressWarnings("unchecked")
    Iterator<MseBlock.Data> second = mock(Iterator.class);
    MseBlock.Data firstBlock = OperatorTestUtil.block(DATA_SCHEMA, new Object[]{10});
    MseBlock.Data droppedBlock = OperatorTestUtil.block(DATA_SCHEMA, new Object[]{11});
    when(first.hasNext()).thenReturn(true, true, false);
    when(first.next()).thenReturn(firstBlock, droppedBlock);
    when(second.hasNext()).thenReturn(false);
    when(_mailboxService.readMaterializedPartition("producer-a", 1111, REQUEST_ID, PRODUCER_STAGE_ID, 0, 0,
        Long.MAX_VALUE)).thenReturn(first);
    when(_mailboxService.readMaterializedPartition("producer-a", 1111, REQUEST_ID, PRODUCER_STAGE_ID, 2, 0,
        Long.MAX_VALUE)).thenReturn(second);
    when(_mailboxService.readMaterializedPartition("producer-b", 2222, REQUEST_ID, PRODUCER_STAGE_ID, 1, 0,
        Long.MAX_VALUE)).thenReturn(List.<MseBlock.Data>of().iterator());

    MaterializedMailboxReceiveOperator operator = receiveOperator(0, Long.MAX_VALUE);
    assertSame(operator.nextBlock(), firstBlock);
    operator.earlyTerminate();

    assertTrue(operator.nextBlock().isSuccess());
    verify(first, times(2)).next();
    verify(_mailboxService).readMaterializedPartition("producer-b", 2222, REQUEST_ID, PRODUCER_STAGE_ID, 1, 0,
        Long.MAX_VALUE);
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*only supports HASH_DISTRIBUTED.*")
  public void testMaterializedReceiveRejectsNonHashDistribution() {
    WorkerMetadata worker = new WorkerMetadata(0, Map.of(), Map.of());
    StageMetadata stage = new StageMetadata(CONSUMER_STAGE_ID, List.of(worker), Map.of());
    MailboxReceiveNode node = receiveNode(RelDistribution.Type.SINGLETON);
    new MaterializedMailboxReceiveOperator(context(stage, worker), node);
  }

  private MaterializedMailboxReceiveOperator receiveOperator(int workerId, long deadlineMs) {
    MailboxInfos producers = new SharedMailboxInfos(List.of(
        new MailboxInfo("producer-a", 1111, List.of(0, 2)),
        new MailboxInfo("producer-b", 2222, List.of(1))));
    WorkerMetadata worker = new WorkerMetadata(workerId, Map.of(PRODUCER_STAGE_ID, producers), Map.of());
    StageMetadata stage = new StageMetadata(CONSUMER_STAGE_ID, List.of(worker), Map.of());
    return new MaterializedMailboxReceiveOperator(context(stage, worker, deadlineMs),
        receiveNode(RelDistribution.Type.HASH_DISTRIBUTED));
  }

  private static MailboxReceiveNode receiveNode(RelDistribution.Type distributionType) {
    return new MailboxReceiveNode(CONSUMER_STAGE_ID, DATA_SCHEMA, PRODUCER_STAGE_ID,
        PinotRelExchangeType.STREAMING, distributionType, List.of(0), List.of(), false, false, null, true);
  }

  private static Worker.MaterializedPartitionHandle materializedHandle(
      int producerWorkerId, int logicalPartitionId, String host, int port) {
    return Worker.MaterializedPartitionHandle.newBuilder()
        .setRequestId(REQUEST_ID)
        .setProducerStageId(PRODUCER_STAGE_ID)
        .setProducerWorkerId(producerWorkerId)
        .setLogicalPartitionId(logicalPartitionId)
        .setHost(host)
        .setTransferPort(port)
        .build();
  }

  private OpChainExecutionContext context(StageMetadata stage, WorkerMetadata worker) {
    return context(stage, worker, Long.MAX_VALUE);
  }

  private OpChainExecutionContext context(StageMetadata stage, WorkerMetadata worker, long deadlineMs) {
    QueryExecutionContext queryContext = new QueryExecutionContext(QueryType.MSE, REQUEST_ID, "cid", "default",
        System.currentTimeMillis(), deadlineMs, deadlineMs, "broker", "instance", "");
    return OpChainExecutionContext.fromQueryContext(_mailboxService, Map.of(), stage, worker, null, false, false,
        queryContext);
  }
}
