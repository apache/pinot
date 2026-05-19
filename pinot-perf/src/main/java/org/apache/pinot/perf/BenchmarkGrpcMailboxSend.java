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
package org.apache.pinot.perf;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// A/B benchmark for the gRPC sender-side back-pressure gate in `GrpcSendingMailbox`.
///
/// Two real `MailboxService` instances run on localhost; a background drainer thread consumes
/// messages from the receiver. The drainer's pace is controlled by `@Param _drainSleepMicros`,
/// which selects one of two measurement regimes:
///
///  * `_drainSleepMicros = 0` (spin-poll): the drainer keeps the receiver queue empty at full
///    speed. `isReady()` stays `true` permanently — only the **fast-path overhead** of the gate
///    is measured (one volatile read). The A/B between `backpressureEnabled=true/false` is
///    dominated by that single read.
///  * `_drainSleepMicros > 0` (throttled): the drainer parks for the given number of
///    microseconds after every poll, regardless of whether a message was found. This lets the
///    gRPC outbound queue fill, causing `isReady()` to flip to `false` and forcing the sender's
///    `awaitReady()` **slow path** (park/wake on a Condition). The A/B now also captures
///    park/wake overhead.
///
/// Combined with `@Param _backpressureEnabled`:
///
///  * `backpressureEnabled=true`: sender blocks in `awaitReady` until the receiver advances the
///    window. Throughput is rate-limited by the drainer when `_drainSleepMicros > 0`.
///  * `backpressureEnabled=false`: sender pushes unconditionally (pre-fix behaviour). Throughput
///    is unbounded — direct memory grows until something fails or the iteration ends.
///
/// Run with `org.openjdk.jmh.Main org.apache.pinot.perf.BenchmarkGrpcMailboxSend`.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, jvmArgsAppend = {
    "-Xms2g", "-Xmx4g",
    "-XX:MaxDirectMemorySize=4g"
})
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 5)
@State(Scope.Benchmark)
public class BenchmarkGrpcMailboxSend {

  private static final DataSchema SCHEMA = new DataSchema(
      new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.STRING});

  /// Bytes of String payload per row. The block carries one row, so this is roughly the on-wire
  /// size of one gRPC `MailboxContent` chunk plus per-row overhead. With gRPC's default ~1 MB
  /// HTTP/2 stream window, payloads ≥ ~16 KB will start hitting the slow path of `awaitReady`
  /// once the drainer falls behind.
  @Param({"16", "256", "16384", "1048576"})
  public int _payloadBytes;

  /// `true` exercises the sender-side gate added in this PR. `false` restores the pre-fix
  /// behaviour (unconditional `onNext`) — useful as a baseline and as a production kill-switch.
  /// Wired through `MailboxService` via `pinot.query.runner.grpc.sender.backpressure.enabled`.
  @Param({"true", "false"})
  public boolean _backpressureEnabled;

  /// Microseconds the drainer sleeps between polls. `0` means spin-poll (the drainer keeps the
  /// receiver queue empty so back-pressure never activates — useful for measuring the gate's
  /// fast-path overhead). Non-zero values throttle the drainer, letting the gRPC HTTP/2 stream
  /// window fill and forcing the sender's `awaitReady()` slow path (park/wake on a Condition).
  @Param({"0", "100"})
  public int _drainSleepMicros;

  private MailboxService _senderService;
  private MailboxService _receiverService;
  private SendingMailbox _sender;
  private RowHeapDataBlock _block;

  private Thread _drainer;
  private final AtomicBoolean _stop = new AtomicBoolean();

  @Setup
  public void setup()
      throws IOException {
    PinotConfiguration cfg = new PinotConfiguration(Map.of(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_SENDER_BACKPRESSURE_ENABLED, _backpressureEnabled));
    _senderService = new MailboxService("localhost", availablePort(), InstanceType.SERVER, cfg);
    _senderService.start();
    _receiverService = new MailboxService("localhost", availablePort(), InstanceType.SERVER, cfg);
    _receiverService.start();

    // Inlined MailboxIdUtils.toMailboxId(1L, 1, 0, 0, 0) format
    // (requestId_senderStage_senderWorker_recvStage_recvWorker).
    String mailboxId = "1_1_0_0_0";
    long deadlineMs = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
    _sender = _senderService.getSendingMailbox("localhost", _receiverService.getPort(),
        mailboxId, deadlineMs, new StatMap<>(MailboxSendOperator.StatKey.class));
    ReceivingMailbox receiver = _receiverService.getReceivingMailbox(mailboxId);
    receiver.registeredReader(() -> { });

    // Drainer: pace is controlled by _drainSleepMicros.
    // _drainSleepMicros == 0: spin-poll — receiver is drained at full speed; only fast-path
    //   gate overhead is measured (isReady() stays true).
    // _drainSleepMicros > 0: park after every iteration (whether or not a message was found)
    //   so the gRPC outbound queue fills and isReady() flips false, exercising the slow-path
    //   park/wake in awaitReady().
    _drainer = new Thread(() -> {
      while (!_stop.get()) {
        ReceivingMailbox.MseBlockWithStats msg = receiver.poll();
        if (msg == null) {
          if (_drainSleepMicros == 0) {
            Thread.onSpinWait();
          } else {
            LockSupport.parkNanos(_drainSleepMicros * 1_000L);
          }
        } else if (_drainSleepMicros > 0) {
          LockSupport.parkNanos(_drainSleepMicros * 1_000L);
        }
      }
    }, "bench-grpc-mailbox-drainer");
    _drainer.setDaemon(true);
    _drainer.start();

    StringBuilder sb = new StringBuilder(_payloadBytes);
    for (int i = 0; i < _payloadBytes; i++) {
      sb.append('x');
    }
    _block = new RowHeapDataBlock(Collections.singletonList(new Object[]{sb.toString()}), SCHEMA);
  }

  private static int availablePort()
      throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @TearDown
  public void teardown()
      throws InterruptedException {
    try {
      _sender.cancel(new RuntimeException("bench done"));
    } catch (Exception ignored) {
      // best-effort
    }
    _stop.set(true);
    _drainer.join(TimeUnit.SECONDS.toMillis(10));
    _senderService.shutdown();
    _receiverService.shutdown();
  }

  /// `GrpcSendingMailbox.send` calls `QueryThreadContext.checkTerminationAndSampleUsage`, which
  /// requires a context to be open on the calling thread. Opening it as a thread-scoped @State
  /// ensures the JMH worker thread has one open across the iteration, without paying
  /// open/close cost per @Benchmark invocation.
  @State(Scope.Thread)
  public static class ThreadCtx {
    private QueryThreadContext _ctx;

    @Setup
    public void setup() {
      _ctx = QueryThreadContext.openForMseTest();
    }

    @TearDown
    public void tearDown() {
      _ctx.close();
    }
  }

  @Benchmark
  public void send(ThreadCtx ignored) {
    _sender.send(_block);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkGrpcMailboxSend.class.getSimpleName())
        .build()).run();
  }
}
