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
package org.apache.pinot.query.runtime.operator.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.service.dispatch.AdaptiveRoutingUpstreamTimings;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests per-sender elapsed-time tracking in {@link BlockingMultiStreamConsumer.OfMseBlock}.
 */
public class BlockingMultiStreamConsumerUpstreamTimingTest {

  @SuppressWarnings("unchecked")
  private AsyncStream<ReceivingMailbox.MseBlockWithStats> mockStream(String id) {
    AsyncStream<ReceivingMailbox.MseBlockWithStats> stream = mock(AsyncStream.class);
    when(stream.getId()).thenReturn(id);
    doNothing().when(stream).addOnNewDataListener(any());
    return stream;
  }

  private static ReceivingMailbox.MseBlockWithStats eos() {
    return new ReceivingMailbox.MseBlockWithStats(SuccessMseBlock.INSTANCE, List.of());
  }

  private OpChainExecutionContext createContext() {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    StageMetadata stageMetadata = new StageMetadata(0,
        List.of(new WorkerMetadata(0, Map.of(), Map.of())), Map.of());
    return OperatorTestUtil.getOpChainContext(mailboxService, Long.MAX_VALUE, stageMetadata);
  }

  /**
   * Covers: per-sender tracking, max-dedup for duplicate sender keys, and no-op for unmapped streams.
   *
   * <p>Three streams: A (fast), A-worker1 (slow, same sender key as A), and C (no sender key mapping).
   * Verifies: A gets max of both workers (700ms), C produces no entry.
   */
  @Test
  public void testPerSenderElapsedTimeWithMaxDedupAndUnmappedStream() {
    AtomicLong clock = new AtomicLong(0L);

    AsyncStream<ReceivingMailbox.MseBlockWithStats> streamA0 = mockStream("mailbox-A-w0");
    AsyncStream<ReceivingMailbox.MseBlockWithStats> streamA1 = mockStream("mailbox-A-w1");
    AsyncStream<ReceivingMailbox.MseBlockWithStats> streamC = mockStream("mailbox-C");

    String keyA = AdaptiveRoutingUpstreamTimings.senderKey("host-a", 8442);

    when(streamA0.poll()).thenAnswer(inv -> {
      clock.set(100L);
      return eos();
    });
    when(streamA1.poll()).thenAnswer(inv -> {
      clock.set(700L);
      return eos();
    });
    when(streamC.poll()).thenAnswer(inv -> {
      clock.set(500L);
      return eos();
    });

    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      Map<Object, String> streamIdToSenderKey = new HashMap<>();
      streamIdToSenderKey.put("mailbox-A-w0", keyA);
      streamIdToSenderKey.put("mailbox-A-w1", keyA);
      // mailbox-C deliberately NOT mapped -> tests null-guard path

      BlockingMultiStreamConsumer.OfMseBlock consumer = new BlockingMultiStreamConsumer.OfMseBlock(
          createContext(),
          new ArrayList<>(List.of(streamA0, streamA1, streamC)),
          /* senderStageId= */ 2,
          streamIdToSenderKey,
          clock::get);

      consumer.readMseBlockBlocking();

      Map<String, Long> timings = consumer.getSenderElapsedMs();
      assertEquals(timings.size(), 1, "Only keyA should be recorded (keyC has no mapping)");
      assertEquals((long) timings.get(keyA), 700L, "Should keep max across workers (700 > 100)");
      assertTrue(!timings.containsKey(AdaptiveRoutingUpstreamTimings.senderKey("host-c", 8442)),
          "Unmapped stream must not produce a timing entry");
    }
  }
}
