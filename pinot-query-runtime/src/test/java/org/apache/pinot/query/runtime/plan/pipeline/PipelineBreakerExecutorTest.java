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
package org.apache.pinot.query.runtime.plan.pipeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxIdUtils;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.mockito.Mockito.when;


public class PipelineBreakerExecutorTest {
  private static final VirtualServerAddress RECEIVER_ADDRESS = new VirtualServerAddress("localhost", 123, 0);
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
  private static final String MAILBOX_ID_1 = MailboxIdUtils.toMailboxId(0, 1, 0, 0, 0);
  private static final String MAILBOX_ID_2 = MailboxIdUtils.toMailboxId(0, 2, 0, 0, 0);

  private AutoCloseable _mocks;
  @Mock
  private MailboxService _mailboxService;
  @Mock
  private ReceivingMailbox _mailbox1;
  @Mock
  private ReceivingMailbox _mailbox2;

  private VirtualServerAddress _server = new VirtualServerAddress("localhost", 123, 0);
  private OpChainSchedulerService _scheduler = new OpChainSchedulerService(Executors.newCachedThreadPool());
  private StageMetadata _stageMetadata1 = new StageMetadata.Builder().setWorkerMetadataList(Stream.of(_server).map(
      s -> new WorkerMetadata.Builder().setVirtualServerAddress(s)
          .addMailBoxInfoMap(0, new MailboxMetadata(
              ImmutableList.of(org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 0, 0, 0),
                  org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(2, 0, 0, 0)),
              ImmutableList.of(_server), ImmutableMap.of()))
          .addMailBoxInfoMap(1, new MailboxMetadata(
              ImmutableList.of(org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(1, 0, 0, 0)),
              ImmutableList.of(_server), ImmutableMap.of()))
          .addMailBoxInfoMap(2, new MailboxMetadata(
              ImmutableList.of(org.apache.pinot.query.planner.physical.MailboxIdUtils.toPlanMailboxId(2, 0, 0, 0)),
              ImmutableList.of(_server), ImmutableMap.of()))
          .build()).collect(Collectors.toList())).build();

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_mailboxService.getHostname()).thenReturn("localhost");
    when(_mailboxService.getPort()).thenReturn(123);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test(enabled = false) // TODO: Enable once pipeline breaker is adapted
  public void shouldReturnBlocksUponNormalOperation() {
    MailboxReceiveNode mailboxReceiveNode =
        new MailboxReceiveNode(0, DATA_SCHEMA, 1, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    DistributedStagePlan distributedStagePlan =
        new DistributedStagePlan(0, RECEIVER_ADDRESS, mailboxReceiveNode, _stageMetadata1);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockUtils.getEndOfStreamTransferableBlock(OperatorTestUtil.getDummyStats(0, 1, _server)));

    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_scheduler, _mailboxService, distributedStagePlan,
            System.currentTimeMillis() + 10_000L, 0, false);

    // then
    // should have single PB result, receive 2 data blocks, EOS block shouldn't be included
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 1);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().values().iterator().next().size(), 2);

    // should collect stats from previous stage here
    Assert.assertNotNull(pipelineBreakerResult.getOpChainStats());
    Assert.assertEquals(pipelineBreakerResult.getOpChainStats().getOperatorStatsMap().size(), 1);
  }

  @Test(enabled = false) // TODO: Enable once pipeline breaker is adapted
  public void shouldWorkWithMultiplePBNodeUponNormalOperation() {
    MailboxReceiveNode mailboxReceiveNode1 =
        new MailboxReceiveNode(0, DATA_SCHEMA, 1, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    MailboxReceiveNode mailboxReceiveNode2 =
        new MailboxReceiveNode(0, DATA_SCHEMA, 2, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    JoinNode joinNode = new JoinNode(0, DATA_SCHEMA, DATA_SCHEMA, DATA_SCHEMA, JoinRelType.INNER, null, null, false);
    joinNode.addInput(mailboxReceiveNode1);
    joinNode.addInput(mailboxReceiveNode2);
    DistributedStagePlan distributedStagePlan =
        new DistributedStagePlan(0, RECEIVER_ADDRESS, joinNode, _stageMetadata1);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        TransferableBlockUtils.getEndOfStreamTransferableBlock(OperatorTestUtil.getDummyStats(0, 1, _server)));
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockUtils.getEndOfStreamTransferableBlock(OperatorTestUtil.getDummyStats(0, 2, _server)));

    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_scheduler, _mailboxService, distributedStagePlan,
            System.currentTimeMillis() + 10_000L, 0, false);

    // then
    // should have two PB result, receive 2 data blocks, one each, EOS block shouldn't be included
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 2);
    Iterator<List<TransferableBlock>> it = pipelineBreakerResult.getResultMap().values().iterator();
    Assert.assertEquals(it.next().size(), 1);
    Assert.assertEquals(it.next().size(), 1);
    Assert.assertFalse(it.hasNext());

    // should collect stats from previous stage here
    Assert.assertNotNull(pipelineBreakerResult.getOpChainStats());
    Assert.assertEquals(pipelineBreakerResult.getOpChainStats().getOperatorStatsMap().size(), 2);
  }

  @Test(enabled = false) // TODO: Enable once pipeline breaker is adapted
  public void shouldReturnErrorBlocksFailureWhenPBExecute() {
    MailboxReceiveNode incorrectlyConfiguredMailboxNode =
        new MailboxReceiveNode(0, DATA_SCHEMA, 3, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    DistributedStagePlan distributedStagePlan =
        new DistributedStagePlan(0, RECEIVER_ADDRESS, incorrectlyConfiguredMailboxNode, _stageMetadata1);

    // when
    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_scheduler, _mailboxService, distributedStagePlan,
            System.currentTimeMillis() + 10_000L, 0, false);

    // then
    // should contain only failure error blocks
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 1);
    List<TransferableBlock> resultBlocks = pipelineBreakerResult.getResultMap().values().iterator().next();
    Assert.assertEquals(resultBlocks.size(), 1);
    Assert.assertTrue(resultBlocks.get(0).isEndOfStreamBlock());
    Assert.assertFalse(resultBlocks.get(0).isSuccessfulEndOfStreamBlock());

    // should have null stats from previous stage here
    Assert.assertNull(pipelineBreakerResult.getOpChainStats());
  }

  @Test(enabled = false) // TODO: Enable once pipeline breaker is adapted
  public void shouldReturnErrorBlocksFailureWhenPBTimeout() {
    MailboxReceiveNode incorrectlyConfiguredMailboxNode =
        new MailboxReceiveNode(0, DATA_SCHEMA, 1, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    DistributedStagePlan distributedStagePlan =
        new DistributedStagePlan(0, RECEIVER_ADDRESS, incorrectlyConfiguredMailboxNode, _stageMetadata1);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_scheduler, _mailboxService, distributedStagePlan,
            System.currentTimeMillis() - 10_000L, 0, false);

    // then
    // should contain only failure error blocks
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 1);
    List<TransferableBlock> resultBlocks = pipelineBreakerResult.getResultMap().values().iterator().next();
    Assert.assertEquals(resultBlocks.size(), 1);
    Assert.assertTrue(resultBlocks.get(0).isEndOfStreamBlock());
    Assert.assertFalse(resultBlocks.get(0).isSuccessfulEndOfStreamBlock());
  }

  @Test(enabled = false) // TODO: Enable once pipeline breaker is adapted
  public void shouldReturnErrorBlocksWhenAnyPBFailure() {
    MailboxReceiveNode mailboxReceiveNode1 =
        new MailboxReceiveNode(0, DATA_SCHEMA, 1, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    MailboxReceiveNode incorrectlyConfiguredMailboxNode =
        new MailboxReceiveNode(0, DATA_SCHEMA, 3, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    JoinNode joinNode = new JoinNode(0, DATA_SCHEMA, DATA_SCHEMA, DATA_SCHEMA, JoinRelType.INNER, null, null, false);
    joinNode.addInput(mailboxReceiveNode1);
    joinNode.addInput(incorrectlyConfiguredMailboxNode);
    DistributedStagePlan distributedStagePlan =
        new DistributedStagePlan(0, RECEIVER_ADDRESS, joinNode, _stageMetadata1);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_scheduler, _mailboxService, distributedStagePlan,
            System.currentTimeMillis() + 10_000L, 0, false);

    // then
    // should fail even if one of the 2 PB returns correct results.
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 2);
    for (List<TransferableBlock> resultBlocks : pipelineBreakerResult.getResultMap().values()) {
      Assert.assertEquals(resultBlocks.size(), 1);
      Assert.assertTrue(resultBlocks.get(0).isEndOfStreamBlock());
      Assert.assertFalse(resultBlocks.get(0).isSuccessfulEndOfStreamBlock());
    }

    // should have null stats from previous stage here
    Assert.assertNull(pipelineBreakerResult.getOpChainStats());
  }

  @Test(enabled = false) // TODO: Enable once pipeline breaker is adapted
  public void shouldReturnErrorBlocksWhenReceivedErrorFromSender() {
    MailboxReceiveNode mailboxReceiveNode1 =
        new MailboxReceiveNode(0, DATA_SCHEMA, 1, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    MailboxReceiveNode incorrectlyConfiguredMailboxNode =
        new MailboxReceiveNode(0, DATA_SCHEMA, 2, RelDistribution.Type.SINGLETON, PinotRelExchangeType.PIPELINE_BREAKER,
            null, null, false, false, null);
    JoinNode joinNode = new JoinNode(0, DATA_SCHEMA, DATA_SCHEMA, DATA_SCHEMA, JoinRelType.INNER, null, null, false);
    joinNode.addInput(mailboxReceiveNode1);
    joinNode.addInput(incorrectlyConfiguredMailboxNode);
    DistributedStagePlan distributedStagePlan =
        new DistributedStagePlan(0, RECEIVER_ADDRESS, joinNode, _stageMetadata1);

    // when
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_1)).thenReturn(_mailbox1);
    when(_mailboxService.getReceivingMailbox(MAILBOX_ID_2)).thenReturn(_mailbox2);
    Object[] row1 = new Object[]{1, 1};
    Object[] row2 = new Object[]{2, 3};
    when(_mailbox1.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row1),
        TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException("ERROR ON 1")));
    when(_mailbox2.poll()).thenReturn(OperatorTestUtil.block(DATA_SCHEMA, row2),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    PipelineBreakerResult pipelineBreakerResult =
        PipelineBreakerExecutor.executePipelineBreakers(_scheduler, _mailboxService, distributedStagePlan,
            System.currentTimeMillis() + 10_000L, 0, false);

    // then
    // should fail even if one of the 2 PB doesn't contain error block from sender.
    Assert.assertNotNull(pipelineBreakerResult);
    Assert.assertEquals(pipelineBreakerResult.getResultMap().size(), 2);
    for (List<TransferableBlock> resultBlocks : pipelineBreakerResult.getResultMap().values()) {
      Assert.assertEquals(resultBlocks.size(), 1);
      Assert.assertTrue(resultBlocks.get(0).isEndOfStreamBlock());
      Assert.assertFalse(resultBlocks.get(0).isSuccessfulEndOfStreamBlock());
    }
  }
}
