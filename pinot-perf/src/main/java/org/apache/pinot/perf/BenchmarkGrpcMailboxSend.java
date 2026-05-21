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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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


/// Benchmark for the gRPC sender → receiver MSE mailbox path.
///
/// One @Benchmark invocation sends a fixed total payload (`TOTAL_BYTES = 128 MiB`) split into
/// pre-computed `MseBlock.Data` blocks of size `_blockSizeBytes`, and waits until every block has
/// been polled out of the receiver mailbox. Measured time is therefore end-to-end "ship a request
/// of this size and process it on the other side" — closer to the unit Pinot actually cares about
/// than throughput of one-block-at-a-time hot-loop send.
///
/// Axes:
///
///  * `_blockSizeBytes` — three representative block sizes:
///      - `8 KiB`   → 16384 blocks per invocation; sender is dominated by per-block overhead.
///      - `8 MiB`   → 16 blocks; one block fits in a single gRPC chunk
///        (`maxInboundMessageSize / 2 ≈ 8 MiB`).
///      - `32 MiB`  → 4 blocks; each block is split by `toByteStrings` into ~4 gRPC chunks.
///  * `_backpressureEnabled` — toggles the `GrpcSendingMailbox` `isReady()`-gate. `false` reverts
///    to the pre-fix unconditional `onNext` (kill-switch).
///  * `_flowControlWindowBytes` — HTTP/2 per-stream inbound window the receiver advertises
///    (`pinot.query.runner.grpc.flow.control.window.bytes`). Sweeps `{64 KiB, 1 MiB, 64 MiB}`,
///    spanning the historical default, BDP-estimated LAN value, and the new MB-scale default.
///
/// The drainer runs as a separate thread (modelling the cross-operator boundary in a real query),
/// blocks on a `Semaphore` released by the receiver's `Reader` callback, and counts down a
/// per-invocation `CountDownLatch` for each polled block. The @Benchmark thread sends every block
/// then awaits the latch.
///
/// Run with `org.openjdk.jmh.Main org.apache.pinot.perf.BenchmarkGrpcMailboxSend`.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {
    "-Xms2g", "-Xmx4g",
    "-XX:MaxDirectMemorySize=4g"
})
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@State(Scope.Benchmark)
public class BenchmarkGrpcMailboxSend {

  /// Total bytes shipped per @Benchmark invocation. Sized so that even at 32 MiB-per-block we
  /// get a handful of blocks to bound the per-invocation latency.
  private static final long TOTAL_BYTES = 128L * 1024 * 1024;

  private static final DataSchema SCHEMA = new DataSchema(
      new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.STRING});

  /// Approximate bytes per `MseBlock.Data` payload. Each block carries a single String column,
  /// one row, where the String is roughly `_blockSizeBytes` characters (ASCII → 1 byte per char
  /// when serialised on the wire, ~2 bytes on heap). The number of blocks per invocation is
  /// `ceil(TOTAL_BYTES / _blockSizeBytes)`.
  @Param({"8192", "8388608", "33554432"})
  public int _blockSizeBytes;

  /// `true` exercises the sender-side `isReady()`-gate; `false` restores the pre-fix
  /// unconditional `onNext` (kill-switch). Wired via
  /// `pinot.query.runner.grpc.sender.backpressure.enabled`.
  @Param({"true", "false"})
  public boolean _backpressureEnabled;

  /// HTTP/2 per-stream flow-control window the receiver advertises, in bytes
  /// (`pinot.query.runner.grpc.flow.control.window.bytes`):
  ///  * `65535`    (~64 KiB) — historical gRPC default; gate engages frequently.
  ///  * `1048576`  (~1 MiB)  — typical BDP-estimated LAN value.
  ///  * `67108864` (64 MiB)  — Pinot's new default; gate rarely engages under typical loads.
  @Param({"65535", "1048576", "67108864"})
  public int _flowControlWindowBytes;

  private MailboxService _senderService;
  private MailboxService _receiverService;
  private SendingMailbox _sender;
  private ReceivingMailbox _receiver;
  private List<RowHeapDataBlock> _blocks;

  // Drainer signalling.
  private final Semaphore _readSignal = new Semaphore(0);
  private final AtomicBoolean _stop = new AtomicBoolean();
  private Thread _drainer;
  private volatile CountDownLatch _drainedLatch;

  @Setup
  public void setup()
      throws IOException {
    // The GrpcMailboxServer fail-fast validation requires flowControlWindow >= maxInboundMessageSize. The narrow window
    // axis values (65535, 1 MiB) are smaller than the 16 MiB default for max-inbound-message-size, so we also clamp the
    // max-msg-size down to the window. Math.min keeps the 64 MiB-window cell using the production default 16 MiB
    // max-msg-size; the two narrow-window cells pin max-msg-size down to the window so the validation passes.
    PinotConfiguration cfg = new PinotConfiguration(Map.of(
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_SENDER_BACKPRESSURE_ENABLED, _backpressureEnabled,
        CommonConstants.MultiStageQueryRunner.KEY_OF_GRPC_FLOW_CONTROL_WINDOW_BYTES, _flowControlWindowBytes,
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES,
            Math.min(_flowControlWindowBytes,
                CommonConstants.MultiStageQueryRunner.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES)));
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
    _receiver = _receiverService.getReceivingMailbox(mailboxId);
    // Reader callback fires once per `offer` — wake the drainer.
    _receiver.registeredReader(_readSignal::release);

    // Pre-compute the block list. All blocks share the same payload String (immutable, so
    // sharing is safe and keeps heap usage bounded). The serialiser still emits independent
    // byte buffers per send.
    int numBlocks = (int) ((TOTAL_BYTES + _blockSizeBytes - 1) / _blockSizeBytes);
    char[] chars = new char[_blockSizeBytes];
    Arrays.fill(chars, 'x');
    String payload = new String(chars);
    _blocks = new ArrayList<>(numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      _blocks.add(new RowHeapDataBlock(
          Collections.singletonList(new Object[]{payload}), SCHEMA));
    }

    // Drainer: block on the reader signal, then drain everything available with non-blocking
    // poll. The semaphore may accumulate spurious permits (we poll more aggressively than the
    // reader fires) but those just cause a no-op wakeup later, which is harmless.
    _drainer = new Thread(() -> {
      while (!_stop.get()) {
        try {
          _readSignal.acquire();
        } catch (InterruptedException e) {
          return;
        }
        while (_receiver.poll() != null) {
          CountDownLatch latch = _drainedLatch;
          if (latch != null) {
            latch.countDown();
          }
        }
      }
    }, "bench-grpc-mailbox-drainer");
    _drainer.setDaemon(true);
    _drainer.start();
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
    _readSignal.release();
    _drainer.join(TimeUnit.SECONDS.toMillis(10));
    _senderService.shutdown();
    _receiverService.shutdown();
  }

  /// `GrpcSendingMailbox.send` calls `QueryThreadContext.checkTerminationAndSampleUsage`, which
  /// requires a context to be open on the calling thread. Opening it as a thread-scoped @State
  /// ensures the JMH worker thread has one open across the iteration.
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
  public void sendRequest(ThreadCtx ignored)
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(_blocks.size());
    _drainedLatch = latch;
    for (RowHeapDataBlock block : _blocks) {
      _sender.send(block);
    }
    latch.await();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkGrpcMailboxSend.class.getSimpleName())
        .build()).run();
  }
}
