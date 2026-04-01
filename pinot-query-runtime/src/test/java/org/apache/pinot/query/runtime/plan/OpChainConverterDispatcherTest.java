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
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
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
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class OpChainConverterDispatcherTest {

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
    DataSchema schema = new DataSchema(
        new String[]{"c1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
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
    StagePlan stagePlan = new StagePlan(createMailboxSendNode(schema, stageId, receiverStageId), stageMetadata);
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
