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


/// Pins the application-level back-pressure gate in [GrpcSendingMailbox#awaitReady]
/// under *narrow* transport configs, so that the gate — not the transport layer — is
/// the dominant mechanism keeping the sender in step with the receiver.
///
/// ## Why this companion test exists
///
/// The original [GrpcSenderBackpressureTest] runs with the production-wide defaults
/// (64 MiB HTTP/2 flow-control window, 64 MiB Netty WriteBufferWaterMark high / 32 MiB
/// low). With those wide defaults the transport-level back-pressure alone bounds the
/// `sendCount / polledCount` ratio comfortably under any reasonable assertion at the
/// 128-byte payload used by the test. The wide-defaults assertion (`sendCount > polledCount * 7000`)
/// therefore still passes even if a future refactor deletes the entire `awaitReady`
/// machinery — the test no longer guards what it was originally built to guard.
///
/// A reviewer pointed this out: without a companion that *forces* the application gate
/// to be the dominant signal, the gate could silently regress and CI would stay green.
/// This test closes that gap.
///
/// ## What this test pins
///
/// The transport pipeline is narrowed to the smallest values gRPC/Netty will accept:
///
/// * `pinot.query.runner.grpc.flow.control.window.bytes = 65535` — gRPC's minimum HTTP/2
///   stream window.
/// * `pinot.query.runner.grpc.write.buffer.high.water.mark.bytes = 262144` (256 KiB).
/// * `pinot.query.runner.grpc.write.buffer.low.water.mark.bytes = 131072` (128 KiB).
///
/// At those sizes the wire layer can absorb only a few hundred 128-byte payloads before
/// signalling back-pressure, so the application-level [GrpcSendingMailbox#awaitReady]
/// gate must engage and park the sender on the [java.util.concurrent.locks.Condition]
/// for the test to satisfy the assertion `sendCount <= polledCount * 50 + 10_000`.
///
/// ## Regression risk this test catches
///
/// If someone breaks the `bypassReady || !_backpressureEnabled` short-circuit in
/// `awaitReady`, removes the `Condition` wait, or otherwise deletes the application
/// gate while leaving the transport defaults wide, the wide-defaults companion test
/// will still pass — but this test will fail loudly. The sender will run away to many
/// thousands of times the poll rate, blowing the tight bound asserted here.
///
/// ## Test matrix
///
/// * [GrpcSenderBackpressureTest] — wide defaults, exercises gate + transport in
///   production-ish conditions. Loose assertion.
/// * [GrpcSenderBackpressureDisabledTest] — wide defaults plus kill-switch off; asserts
///   the pre-fix unbounded sender behaviour is preserved when the gate is disabled.
/// * This test — narrow transport configs; asserts the application gate alone is
///   enough to keep the sender bounded.
public class GrpcSenderBackpressureTightGateTest {
  private static final DataSchema SCHEMA = new DataSchema(
      new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.STRING});
  // Same small payload as the companion tests — enough to have a real serialized
  // body but small enough not to exhaust direct memory within the 3-second budget.
  private static final String PAYLOAD = "x".repeat(128);
  private static final long SEND_BUDGET_NS = TimeUnit.SECONDS.toNanos(3);
  private static final long READER_POLL_INTERVAL_MS = 20;

  private MailboxService _senderService;
  private MailboxService _receiverService;

  @BeforeClass
  public void setUp() {
    // Narrow the gRPC/Netty transport pipeline to the minimum the stack will accept,
    // so the application-level awaitReady gate becomes the dominant back-pressure
    // mechanism rather than the transport layer.
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_FLOW_CONTROL_WINDOW_BYTES, 65535,
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES, 262144,
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES, 131072));
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
  public void applicationGateBoundsSenderUnderNarrowTransport()
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

    int sendCount = 0;

    // Watchdog: with the gate active the sender blocks inside awaitReady once the gRPC
    // outbound is full, so a wall-clock deadline checked between sends is not enough to
    // bound the test runtime. We cancel the sender from a separate thread when the
    // budget elapses, which wakes the blocked awaitReady waiter and lets the send loop
    // exit via the isTerminated() check.
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
      }
    } finally {
      watchdog.shutdownNow();
    }

    stop.set(true);
    slowReader.join(TimeUnit.SECONDS.toMillis(10));

    long polledCount = polled.get();

    System.out.printf(Locale.ROOT,
        "[GrpcSenderBackpressureTightGateTest] sent=%d polled=%d ratio=%.1fx%n",
        sendCount, polledCount,
        polledCount == 0 ? Double.POSITIVE_INFINITY : (double) sendCount / polledCount);

    // Tight bound: with the application gate engaged and the transport narrowed so it
    // can buffer at most a few hundred small payloads, the sender should track the
    // 50 polls/sec reader plus a bounded in-flight pipeline. We allow `polledCount * 50`
    // for the per-poll headroom and an additive `10_000` constant to absorb startup
    // burst before the first awaitReady park. If the application gate is silently
    // removed, the sender races to hundreds of thousands of sends and this fails.
    long allowedSendCount = polledCount * 50 + 10_000;
    assertTrue(sendCount <= allowedSendCount,
        "Sender ran away despite narrow transport, suggesting the application gate did not engage. "
            + "sendCount=" + sendCount + " polledCount=" + polledCount + " allowed=" + allowedSendCount);
  }

  private static void sleepQuiet(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
