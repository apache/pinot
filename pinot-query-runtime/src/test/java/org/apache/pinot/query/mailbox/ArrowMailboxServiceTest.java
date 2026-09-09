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
package org.apache.pinot.query.mailbox;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/** Verifies negotiated Arrow IPC and legacy fallback across real gRPC mailbox endpoints. */
public class ArrowMailboxServiceTest {
  @DataProvider
  public Object[][] capabilities() {
    return new Object[][]{
        {true, true, true}, {true, false, false}, {false, true, true}, {false, false, false}, {true, true, false}
    };
  }

  @Test(dataProvider = "capabilities")
  public void testCapabilityNegotiationAndEos(boolean senderEnabled, boolean receiverEnabled, boolean nativeConsumer)
      throws Exception {
    MailboxService senderService = newService(senderEnabled);
    MailboxService receiverService = newService(receiverEnabled);
    WorkerMetadata worker = new WorkerMetadata(0, Map.of(), Map.of());
    OpChainExecutionContext receiverContext = OperatorTestUtil.getOpChainContext(receiverService,
        System.currentTimeMillis() + 30_000, new StageMetadata(0, List.of(worker), Map.of()));
    senderService.start();
    try {
      receiverService.start();
      try (ArrowBuffers sourceBuffers = ArrowMailboxTest.newBuffers();
          ArrowQueryContext sourceContext = ArrowMailboxTest.newContext(sourceBuffers, 0);
          QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
        String mailboxId = "arrow-grpc-" + senderEnabled + "-" + receiverEnabled;
        ReceivingMailbox receiving = receiverService.getReceivingMailbox(mailboxId);
        receiving.registeredReader(() -> { });
        if (nativeConsumer) {
          receiving.enableArrow(receiverContext.getOrCreateArrowContext());
        }
        StatMap<MailboxSendOperator.StatKey> stats = new StatMap<>(MailboxSendOperator.StatKey.class);
        GrpcSendingMailbox sending = (GrpcSendingMailbox) senderService.getSendingMailbox(
            "localhost", receiverService.getPort(), mailboxId, System.currentTimeMillis() + 30_000, stats);
        ArrowBlock source = ArrowMailboxTest.newBlock(sourceContext);
        try {
          sending.send(source);
          MseBlock.Data first = (MseBlock.Data) read(receiving).getBlock();
          assertTrue(first.isSerialized(), "Unknown peers must receive legacy bytes");
          ArrowMailboxTest.assertRows(first);
          if (senderEnabled && nativeConsumer) {
            TestUtils.waitForCondition(unused -> sending.isArrowIpcSupported(), 5000L,
                "Receiver did not advertise IPC after its first block");
          }
          sending.send(source);
        } finally {
          source.release();
        }
        sourceContext.close();
        MseBlock.Data second = (MseBlock.Data) read(receiving).getBlock();
        try {
          assertEquals(second.isArrow(), senderEnabled && receiverEnabled && nativeConsumer);
          ArrowMailboxTest.assertRows(second);
        } finally {
          if (second instanceof ArrowBlock) {
            ((ArrowBlock) second).release();
          }
        }
        List<DataBuffer> serializedStats = MultiStageQueryStats.emptyStats(1).serialize();
        sending.send(SuccessMseBlock.INSTANCE, serializedStats);
        ReceivingMailbox.MseBlockWithStats eos = read(receiving);
        assertTrue(eos.getBlock().isSuccess());
        assertEquals(eos.getSerializedStats().size(), serializedStats.size());
        assertEquals(stats.getInt(MailboxSendOperator.StatKey.RAW_MESSAGES), 3);
        receiving.closeArrow();
        receiverContext.closeArrowResources();
        assertEquals(sourceBuffers.getAllocatedMemory(), 0L);
      } finally {
        receiverContext.closeArrowResources();
        receiverService.shutdown();
      }
    } finally {
      senderService.shutdown();
    }
  }

  private static MailboxService newService(boolean arrowEnabled) {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW, arrowEnabled,
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, 1024));
    return new MailboxService("localhost", QueryTestUtils.getAvailablePort(), InstanceType.SERVER, config);
  }

  private static ReceivingMailbox.MseBlockWithStats read(ReceivingMailbox mailbox) {
    AtomicReference<ReceivingMailbox.MseBlockWithStats> result = new AtomicReference<>();
    TestUtils.waitForCondition(ignored -> {
      result.set(mailbox.poll());
      return result.get() != null;
    }, 5000L, "No block received over gRPC");
    return result.get();
  }
}
