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


/// Guards the back-pressure kill-switch flag
/// (`pinot.query.runner.grpc.sender.backpressure.enabled`) introduced to
/// allow operators to opt out of gRPC sender-side flow control.
///
/// ## What this test checks
///
/// When the flag is set to `false`, [GrpcSendingMailbox#awaitReady]
/// must short-circuit immediately — i.e. the `!_backpressureEnabled` branch
/// in the guard must fire — so the sender pushes blocks without waiting for the
/// receiver to drain them. Under that condition a fast sender can push orders of
/// magnitude more blocks than a slow receiver polls in the same wall-clock window.
///
/// ## Regression risk
///
/// If someone accidentally removes or inverts the
/// `bypassReady || !_backpressureEnabled` short-circuit in `awaitReady`, the gate
/// would become permanently active and this flag would have no effect. The test
/// would then see `sendCount` track `polledCount` closely (as in the *enabled*
/// repro test), causing the `sendCount > polledCount * 10` assertion to fail and
/// surfacing the regression in CI.
///
/// ## Relation to the companion test
///
/// [GrpcSenderBackpressureTest] verifies the *enabled* (default) path — that the
/// gate keeps the sender in check. This test verifies the *disabled* path — that
/// turning the gate off actually removes it. Together they pin both sides of the
/// boolean.
public class GrpcSenderBackpressureDisabledTest {
  private static final DataSchema SCHEMA = new DataSchema(
      new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.STRING});
  // Same small payload as the companion test — enough to have a real serialized
  // body but small enough not to exhaust direct memory within the 3-second budget.
  private static final String PAYLOAD = "x".repeat(128);
  private static final long SEND_BUDGET_NS = TimeUnit.SECONDS.toNanos(3);
  private static final long READER_POLL_INTERVAL_MS = 20;

  private MailboxService _senderService;
  private MailboxService _receiverService;

  @BeforeClass
  public void setUp() {
    // Disable the back-pressure gate so awaitReady short-circuits unconditionally.
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_SENDER_BACKPRESSURE_ENABLED, false));
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
  public void killSwitchDisablesBackpressureGate()
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

    // Watchdog: with the gate off the sender races ahead without blocking; we still
    // need a watchdog to cap wall-clock time and prevent the loop from filling
    // direct memory before the test completes. The cancel wakes any thread that
    // happens to be inside awaitReady (shouldn't be any with the gate off, but
    // it also covers the isTerminated() check in the send loop).
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
        "[GrpcSenderBackpressureDisabledTest] sent=%d polled=%d ratio=%.1fx%n",
        sendCount, polledCount,
        polledCount == 0 ? Double.POSITIVE_INFINITY : (double) sendCount / polledCount);

    // The sender must have pushed far more blocks than the slow receiver could
    // poll. At 20 ms polling intervals over a 3-second budget the receiver sees
    // roughly 150 polls; with the gate off the sender typically reaches hundreds
    // of thousands of sends. A 10x ratio is an extremely conservative floor —
    // if the gate were accidentally left active, sendCount would track polledCount
    // closely (ratio ~1–3x) and this assertion would fail.
    assertTrue(sendCount > polledCount * 10,
        "Expected the kill-switch to let the sender massively outrpace the receiver, but "
            + "sendCount=" + sendCount + " polledCount=" + polledCount
            + " ratio=" + (polledCount == 0 ? "∞" : sendCount / polledCount)
            + ". This may indicate the backpressure gate is still active despite the flag being false.");
  }

  private static void sleepQuiet(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
