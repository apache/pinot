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

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Reproducer for the missing sender-side gRPC backpressure described in
/// `grpc-oom-analysis.md`.
///
/// A "fast sender" pushes the same small data block over and over on the test
/// thread while a "slow reader" thread polls the receiving mailbox at roughly
/// 50 blocks per second. Because [GrpcSendingMailbox] does not use gRPC's
/// `isReady()` / `setOnReadyHandler()` hooks and ignores the receiver-side
/// buffer-size feedback (see the TODO in
/// [org.apache.pinot.query.mailbox.channel.MailboxStatusObserver]), every
/// `send()` returns immediately — the proto bytes pile up in Netty's outbound
/// direct memory regardless of how slowly the consumer drains.
///
/// The test asserts that within a few seconds the sender pushes
/// significantly more blocks than the receiver could ever consume. The direct
/// memory delta is logged but not asserted because Netty's pooled allocator
/// reuses freed chunks and the measurement is sensitive to other JVM activity.
///
/// When sender-side flow control is added, this test should be rewritten to
/// assert the opposite: that `send()` blocks (or yields cooperatively) once the
/// receiver falls behind, so that `sendCount` stays close to `polledCount`.
public class GrpcSenderBackpressureReproTest {
  private static final DataSchema SCHEMA = new DataSchema(
      new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.STRING});
  // Small but not empty — enough that each MailboxContent has a real payload
  // but small enough that 100k of them comfortably fit in JVM direct memory on
  // a CI machine.
  private static final String PAYLOAD = "x".repeat(128);
  private static final long SEND_BUDGET_NS = TimeUnit.SECONDS.toNanos(3);
  private static final long READER_POLL_INTERVAL_MS = 20;

  private MailboxService _senderService;
  private MailboxService _receiverService;

  @BeforeClass
  public void setUp() {
    PinotConfiguration config = new PinotConfiguration(Map.of());
    _senderService = new MailboxService("localhost", QueryTestUtils.getAvailablePort(),
        InstanceType.SERVER, config);
    _senderService.start();
    _receiverService = new MailboxService("localhost", QueryTestUtils.getAvailablePort(),
        InstanceType.SERVER, config);
    _receiverService.start();
  }

  @AfterClass
  public void tearDown() {
    _senderService.shutdown();
    _receiverService.shutdown();
  }

  @Test
  public void fastSenderOutpacesSlowReceiver()
      throws Exception {
    String mailboxId = MailboxIdUtils.toMailboxId(1, 1, 0, 0, 0);
    long deadlineMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
    StatMap<MailboxSendOperator.StatKey> stats =
        new StatMap<>(MailboxSendOperator.StatKey.class);

    SendingMailbox sender = _senderService.getSendingMailbox(
        "localhost", _receiverService.getPort(), mailboxId, deadlineMs, stats);
    ReceivingMailbox receiver = _receiverService.getReceivingMailbox(mailboxId);
    receiver.registeredReader(() -> { });

    AtomicBoolean stop = new AtomicBoolean(false);
    AtomicLong polled = new AtomicLong();
    Thread slowReader = new Thread(() -> {
      while (!stop.get()) {
        ReceivingMailbox.MseBlockWithStats msg = receiver.poll();
        if (msg != null && !msg.getBlock().isEos()) {
          polled.incrementAndGet();
        }
        sleepQuiet(READER_POLL_INTERVAL_MS);
      }
    }, "slow-reader");
    slowReader.setDaemon(true);
    slowReader.start();

    // Same instance, sent over and over.
    RowHeapDataBlock block = OperatorTestUtil.block(SCHEMA, new Object[]{PAYLOAD});

    // We read memory through the `MailboxService` gauge accessors instead of
    // `PlatformDependent.usedDirectMemory()`. The gauges:
    //  * are scoped per `MailboxService`, so the numbers don't leak in from
    //    other gRPC traffic in the same JVM;
    //  * report both direct and heap, so they stay meaningful when Netty is
    //    forced to heap (e.g. `-Dio.netty.noPreferDirect=true`);
    //  * are exactly the values exported in production as the
    //    `MAILBOX_CLIENT_USED_*` and `MAILBOX_SERVER_USED_*` gauges.
    long baselineClient = senderClientPool(_senderService);
    long baselineServer = receiverServerPool(_receiverService);
    long peakClient = baselineClient;
    long peakServer = baselineServer;
    int sendCount = 0;
    long sendDeadlineNs = System.nanoTime() + SEND_BUDGET_NS;

    try (QueryThreadContext ctx = QueryThreadContext.openForMseTest()) {
      while (System.nanoTime() < sendDeadlineNs) {
        sender.send(block);
        sendCount++;
        if (sender.isTerminated()) {
          break;
        }
        // Sample pool memory periodically rather than on every send to keep
        // the hot loop tight.
        if ((sendCount & 0xff) == 0) {
          peakClient = Math.max(peakClient, senderClientPool(_senderService));
          peakServer = Math.max(peakServer, receiverServerPool(_receiverService));
        }
      }
      peakClient = Math.max(peakClient, senderClientPool(_senderService));
      peakServer = Math.max(peakServer, receiverServerPool(_receiverService));
    }

    // Snapshot the sender stats before `cancel()` — cancel sends an error EOS
    // through `processAndSend`, which would otherwise bump RAW_MESSAGES by 1.
    int rawMessagesBeforeCancel = stats.getInt(MailboxSendOperator.StatKey.RAW_MESSAGES);

    stop.set(true);
    slowReader.join(TimeUnit.SECONDS.toMillis(10));
    sender.cancel(new RuntimeException("test done"));

    long polledCount = polled.get();
    long clientGrowth = peakClient - baselineClient;
    long serverGrowth = peakServer - baselineServer;

    System.out.printf(Locale.ROOT,
        "[GrpcSenderBackpressureReproTest] sent=%d polled=%d ratio=%.1fx%n"
            + "  sender   MAILBOX_CLIENT_USED_*: direct=%dB heap=%dB (peak growth=%dB)%n"
            + "  receiver MAILBOX_SERVER_USED_*: direct=%dB heap=%dB (peak growth=%dB)%n",
        sendCount, polledCount,
        polledCount == 0 ? Double.POSITIVE_INFINITY : (double) sendCount / polledCount,
        _senderService.getMailboxClientUsedDirectMemoryBytes(),
        _senderService.getMailboxClientUsedHeapMemoryBytes(),
        clientGrowth,
        _receiverService.getMailboxServerUsedDirectMemoryBytes(),
        _receiverService.getMailboxServerUsedHeapMemoryBytes(),
        serverGrowth);

    assertEquals(rawMessagesBeforeCancel, sendCount,
        "RAW_MESSAGES stat should match successful send() call count");

    // With proper sender-side flow control, `sendCount` would be close to
    // `polledCount` plus a small in-flight buffer (~ReceivingMailbox.
    // DEFAULT_MAX_PENDING_BLOCKS + the HTTP/2 window worth of messages). The
    // bug we are documenting: the sender outpaces the receiver by many orders
    // of magnitude. We require at least a 10x ratio here to stay robust on
    // slow CI hosts; in practice the ratio is much higher.
    assertTrue(sendCount > polledCount * 10,
        "Sender failed to outpace receiver. sent=" + sendCount + " polled=" + polledCount);
  }

  private static void sleepQuiet(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /// Same value the production `MAILBOX_CLIENT_USED_*` gauges report — sum of
  /// direct + heap so the test stays valid regardless of Netty's buffer mode.
  private static long senderClientPool(MailboxService service) {
    return service.getMailboxClientUsedDirectMemoryBytes() + service.getMailboxClientUsedHeapMemoryBytes();
  }

  /// Same value the production `MAILBOX_SERVER_USED_*` gauges report.
  private static long receiverServerPool(MailboxService service) {
    return service.getMailboxServerUsedDirectMemoryBytes() + service.getMailboxServerUsedHeapMemoryBytes();
  }
}
