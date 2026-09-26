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
package org.apache.pinot.query.runtime.plan;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.SortedMailboxMergeReceiveOperator;
import org.apache.pinot.query.runtime.operator.SortedMailboxReceiveOperator;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class OpChainConverterDispatcherTest {
  private static final DataSchema DATA_SCHEMA = new DataSchema(
      new String[]{"c1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
  private static final List<RelFieldCollation> COLLATIONS = List.of(new RelFieldCollation(0));

  @AfterMethod(alwaysRun = true)
  public void resetOverride() {
    OpChainConverterDispatcher.setActiveConverterIdOverride(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUnknownOverrideRejected() {
    OpChainConverterDispatcher.setActiveConverterIdOverride("no-such-converter-id");
  }

  @Test
  public void testSendEarlyErrorReturnsMailboxSendOpChain() {
    OpChainConverterDispatcher.setActiveConverterIdOverride("default");
    int stageId = 1;
    int receiverStageId = 0;
    int workerId = 0;

    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(8000);
    when(mailboxService.getSendingMailbox(anyString(), anyInt(), anyString(), anyLong(), any()))
        .thenReturn(mock(SendingMailbox.class));

    MailboxInfo receiverMailboxInfo = new MailboxInfo("localhost", 18080, List.of(0));
    WorkerMetadata workerMetadata =
        new WorkerMetadata(workerId, Map.of(receiverStageId, new MailboxInfos(receiverMailboxInfo)));
    StageMetadata stageMetadata = new StageMetadata(stageId, List.of(workerMetadata), Map.of());
    OpChainExecutionContext context =
        new OpChainExecutionContext(mailboxService, 123L, "cid-123", System.currentTimeMillis() + 60_000,
            System.currentTimeMillis() + 60_000, "broker", Map.of(), stageMetadata, workerMetadata, null, false, false);
    StagePlan stagePlan = new StagePlan(createMailboxSendNode(DATA_SCHEMA, stageId, receiverStageId), stageMetadata);
    ErrorMseBlock errorBlock = ErrorMseBlock.fromError(QueryErrorCode.QUERY_EXECUTION, "simulated pipeline failure");

    OpChain opChain = OpChainConverterDispatcher.sendEarlyError(context, stagePlan, errorBlock);
    try {
      Assert.assertNotNull(opChain);
      Assert.assertTrue(opChain.getRoot() instanceof MailboxSendOperator,
          "Default converter should build a mailbox-send opchain for early errors");
      MseBlock block = opChain.getRoot().nextBlock();
      Assert.assertTrue(block.isError(), "Early-error opchain should immediately emit an error block");
      Assert.assertTrue(block instanceof ErrorMseBlock);
      ErrorMseBlock emittedError = (ErrorMseBlock) block;
      QueryErrorCode mainErrorCode = emittedError.getMainErrorCode();
      Assert.assertEquals(mainErrorCode, QueryErrorCode.QUERY_EXECUTION);
      String message = emittedError.getErrorMessages().get(mainErrorCode);
      Assert.assertTrue(message.contains("simulated pipeline failure"),
          "Unexpected error message from early-error opchain: " + message);
    } finally {
      opChain.close();
    }
  }

  @Test
  public void testExplicitSortInputUsesSortedSendingMailbox() {
    int stageId = 1;
    int receiverStageId = 0;
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(8000);
    when(mailboxService.getSendingMailbox(anyString(), anyInt(), anyString(), anyLong(), any(), eq(true)))
        .thenReturn(mock(SendingMailbox.class));

    MailboxInfo receiverMailboxInfo = new MailboxInfo("localhost", 18080, List.of(0));
    WorkerMetadata workerMetadata =
        new WorkerMetadata(0, Map.of(receiverStageId, new MailboxInfos(receiverMailboxInfo)));
    StageMetadata stageMetadata = new StageMetadata(stageId, List.of(workerMetadata), Map.of());
    OpChainExecutionContext context = createContext(mailboxService, stageMetadata, workerMetadata);
    PlanNode valueNode = new ValueNode(stageId, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    PlanNode sortNode = new SortNode(stageId, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(valueNode), COLLATIONS,
        Integer.MAX_VALUE, 0);
    MailboxSendNode sendNode = new MailboxSendNode(stageId, DATA_SCHEMA, List.of(sortNode), receiverStageId,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.RANDOM_DISTRIBUTED, List.of(), false, COLLATIONS, true,
        "MURMUR3");

    try (OpChain opChain = PlanNodeToOpChain.convert(sendNode, context)) {
      Assert.assertTrue(opChain.getRoot() instanceof MailboxSendOperator);
      verify(mailboxService).getSendingMailbox(anyString(), anyInt(), anyString(), anyLong(), any(), eq(true));
    }
  }

  @Test
  public void testSortedReceiveOperatorSelection() {
    int stageId = 0;
    int senderStageId = 1;
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(8000);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of());
    StageMetadata stageMetadata = new StageMetadata(stageId, List.of(workerMetadata), Map.of());
    OpChainExecutionContext context = createContext(mailboxService, stageMetadata, workerMetadata);

    MailboxReceiveNode mergeReceiveNode = new MailboxReceiveNode(stageId, DATA_SCHEMA, senderStageId,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON, List.of(), COLLATIONS, true, true, null);
    try (OpChain opChain = PlanNodeToOpChain.convert(mergeReceiveNode, context)) {
      Assert.assertTrue(opChain.getRoot() instanceof SortedMailboxMergeReceiveOperator);
    }

    MailboxReceiveNode legacyReceiveNode = new MailboxReceiveNode(stageId, DATA_SCHEMA, senderStageId,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON, List.of(), COLLATIONS, true, false, null);
    try (OpChain opChain = PlanNodeToOpChain.convert(legacyReceiveNode, context)) {
      Assert.assertTrue(opChain.getRoot() instanceof SortedMailboxReceiveOperator);
    }
  }

  private static OpChainExecutionContext createContext(MailboxService mailboxService, StageMetadata stageMetadata,
      WorkerMetadata workerMetadata) {
    return new OpChainExecutionContext(mailboxService, 123L, "cid-123", System.currentTimeMillis() + 60_000,
        System.currentTimeMillis() + 60_000, "broker", Map.of(), stageMetadata, workerMetadata, null, false, false);
  }

  private static MailboxSendNode createMailboxSendNode(DataSchema schema, int stageId, int receiverStageId) {
    PlanNode inputNode = new ValueNode(stageId, schema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    return new MailboxSendNode(
        stageId,
        schema,
        List.of(inputNode),
        receiverStageId,
        PinotRelExchangeType.STREAMING,
        RelDistribution.Type.RANDOM_DISTRIBUTED,
        List.of(),
        false,
        List.of(),
        false,
        "MURMUR3");
  }
}
