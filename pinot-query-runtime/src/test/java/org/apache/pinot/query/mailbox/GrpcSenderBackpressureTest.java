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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/// Validates that sender-side gRPC back-pressure keeps a fast sender roughly in step with a slow receiver.
///
/// A "fast sender" pushes the same small data block repeatedly on the test thread while a "slow reader"
/// thread polls the receiving mailbox at roughly 50 blocks per second. With back-pressure in place, the
/// sender thread blocks inside [GrpcSendingMailbox.awaitReady] whenever the gRPC outbound queue fills,
/// so the send rate tracks the polling rate plus a bounded in-flight pipeline.
///
/// With the new transport defaults (64 MB HTTP/2 flow-control window and 64 MB Netty write-buffer
/// high-water mark), both the gRPC stream window and the Netty WriteQueue can buffer far more data
/// before signalling back-pressure.  As a result, [GrpcSendingMailbox.awaitReady] rarely fires the
/// application-level gate during a 3-second run; the back-pressure that *does* apply is transport-level
/// (the kernel's TCP send buffer and the receiver's gRPC server read loop).  This means the ratio of
/// `sendCount` to `polledCount` is much larger than with the old narrow-window defaults, and the test
/// now exercises that transport-level back-pressure rather than the application-level gate.
///
/// The test asserts two complementary properties:
///  1. `sendCount` is bounded by a generous multiple of `polledCount` — without any back-pressure the
///     ratio would be orders of magnitude higher as the sender exhausts direct memory.  The observed
///     ratio with the new defaults is around 2200×; the threshold is set to ~3× that to give headroom
///     for hardware variation while still catching complete removal of all back-pressure.
///  2. The peak growth of the sender's client allocator stays under a generous cap — the wider
///     in-flight pipeline justified by the larger transport defaults means more data can be in-flight
///     at any moment, so the cap is calibrated to ~3× the observed peak growth (~8 MB).
///
/// The thresholds are intentionally loose: this is a regression guard against all back-pressure being
/// silently removed, not a precise performance SLA.
public class GrpcSenderBackpressureTest {
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
    // Both back-pressure features (sender gate + manual receiver-side flow control with prefetched
    // credit) default to off; this test exists to pin the bounded-sender behaviour they provide, so we
    // explicitly turn both on here.
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_SENDER_BACKPRESSURE_ENABLED, true,
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_MANUAL_INBOUND_FLOW_CONTROL_ENABLED, true,
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_INBOUND_MESSAGE_CREDIT, 128));
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
  public void senderObservesBackpressureFromSlowReceiver()
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

    // Watchdog: with back-pressure in place, `sender.send` blocks once the gRPC outbound is full, so a wall-clock
    // deadline checked between sends is not enough to bound the test runtime. We cancel the sender from a separate
    // thread when the budget elapses, which wakes the blocked `awaitReady()` waiter and lets the send loop exit
    // via the `isTerminated()` check.
    ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "test-budget-watchdog");
      t.setDaemon(true);
      return t;
    });
    watchdog.schedule(() -> sender.cancel(new RuntimeException("test budget elapsed")),
        SEND_BUDGET_NS, TimeUnit.NANOSECONDS);

    try (QueryThreadContext ctx = QueryThreadContext.openForMseTest()) {
      while (!sender.isTerminated()) {
        sender.send(block);
        sendCount++;
        // Sample pool memory periodically rather than on every send to keep the hot loop tight.
        if ((sendCount & 0xff) == 0) {
          peakClient = Math.max(peakClient, senderClientPool(_senderService));
          peakServer = Math.max(peakServer, receiverServerPool(_receiverService));
        }
      }
      peakClient = Math.max(peakClient, senderClientPool(_senderService));
      peakServer = Math.max(peakServer, receiverServerPool(_receiverService));
    } finally {
      watchdog.shutdownNow();
    }

    // RAW_MESSAGES at this point may already include the error EOS the watchdog's cancel pushed through, so we use
    // `<=` rather than `==` in the assertion below.
    int rawMessages = stats.getInt(MailboxSendOperator.StatKey.RAW_MESSAGES);

    stop.set(true);
    slowReader.join(TimeUnit.SECONDS.toMillis(10));

    long polledCount = polled.get();
    long clientGrowth = peakClient - baselineClient;
    long serverGrowth = peakServer - baselineServer;

    System.out.printf(Locale.ROOT,
        "[GrpcSenderBackpressureTest] sent=%d polled=%d ratio=%.1fx%n"
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

    // RAW_MESSAGES counts every block we pushed through processAndSend, including the error EOS the watchdog's
    // cancel may have emitted. So `rawMessages` is `sendCount` or `sendCount + 1`.
    assertTrue(rawMessages == sendCount || rawMessages == sendCount + 1,
        "RAW_MESSAGES (" + rawMessages + ") should equal sendCount (" + sendCount + ") or sendCount+1");

    // (1) Bounded in-flight pipeline.
    //
    // With the new 64 MB HTTP/2 flow-control window and 64 MB Netty write-buffer high-water mark,
    // the transport can buffer substantially more data before stalling the sender.  The observed
    // ratio with these defaults is ~2200×; we allow ~3× that (7000×) as a generous regression guard.
    // If all back-pressure were removed the sender would exhaust direct memory within the 3-second
    // budget at a ratio several orders of magnitude higher — so this threshold still catches that.
    long allowedSendCount = polledCount * 7000;
    assertTrue(sendCount < allowedSendCount,
        "Sender outpaced the receiver beyond the in-flight allowance. sent=" + sendCount
            + " polled=" + polledCount + " allowed=" + allowedSendCount);

    // (2) Bounded sender-side direct memory growth.
    //
    // With the wider in-flight pipeline the sender-side allocator may grow by up to one Netty pool
    // chunk (~16 MB) during the 3-second run.  Observed peak growth is ~8 MB; we cap at 25 MB
    // (~3×) to accommodate variation while still catching unbounded allocation regressions.
    long clientGrowthCap = 25L * 1024 * 1024;
    assertTrue(clientGrowth < clientGrowthCap,
        "Sender client allocator grew beyond expected steady-state. growth=" + clientGrowth
            + " cap=" + clientGrowthCap);
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
