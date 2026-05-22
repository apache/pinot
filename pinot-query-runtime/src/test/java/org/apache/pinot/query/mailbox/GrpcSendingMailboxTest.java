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

import com.google.protobuf.ByteString;
import io.grpc.stub.ClientCallStreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockEquals;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class GrpcSendingMailboxTest {

  @Test
  public void sendDataThrowsWhenQueryTerminated() {
    ChannelManager channelManager = Mockito.mock(ChannelManager.class);
    GrpcSendingMailbox mailbox = new GrpcSendingMailbox("test-mailbox", channelManager, "localhost", 0, Long.MAX_VALUE,
        new StatMap<>(MailboxSendOperator.StatKey.class), 4 * 1024 * 1024, true);
    RowHeapDataBlock block = new RowHeapDataBlock(Collections.singletonList(new Object[]{"val"}),
        new DataSchema(new String[]{"foo"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));

    try (QueryThreadContext ctx = QueryThreadContext.openForMseTest()) {
      ctx.getExecutionContext().terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "test");

      // Termination check at the top of send(MseBlock.Data) fires before the gRPC channel is touched.
      Assert.assertThrows(TerminationException.class, () -> mailbox.send(block));
      Mockito.verifyNoInteractions(channelManager);
    }
  }

  /// Regression test for the lazy-initialization data race on `_contentObserver`. Before the fix, both `sendInternal`
  /// and `cancel` had an unsynchronized `if (_contentObserver == null) { _contentObserver = getContentObserver(); }`
  /// pattern. `_contentObserver` is `volatile` so individual reads/writes are atomic, but two threads racing through
  /// the check-then-act could both observe `null` and each call `getContentObserver()`, opening two gRPC streams for
  /// the same mailbox id. The fix funnels both call sites through `ensureContentObserverInitialized()`, which uses
  /// the standard double-checked-lock idiom under `_readyLock`.
  ///
  /// The test subclasses [GrpcSendingMailbox] to count `getContentObserver` calls and return a no-op observer (so we
  /// never touch the real gRPC stack). Two threads — sender and canceller — are synchronized on a [CyclicBarrier]
  /// so they enter their respective methods together, then we assert exactly one observer was opened. Multiple
  /// iterations surface the race under timing variance.
  @Test
  public void concurrentSendAndCancelInitializeContentObserverExactlyOnce()
      throws Exception {
    int iterations = 200;
    ChannelManager channelManager = Mockito.mock(ChannelManager.class);
    DataSchema schema =
        new DataSchema(new String[]{"foo"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < iterations; i++) {
        CountingGrpcSendingMailbox mailbox = new CountingGrpcSendingMailbox("race-mailbox-" + i, channelManager);
        RowHeapDataBlock block = new RowHeapDataBlock(Collections.singletonList(new Object[]{"val"}), schema);

        CyclicBarrier barrier = new CyclicBarrier(2);

        Future<?> senderFuture = executor.submit(() -> {
          try (QueryThreadContext ctx = QueryThreadContext.openForMseTest()) {
            barrier.await();
            mailbox.send(block);
          } catch (Exception e) {
            // Sender may legitimately observe isTerminated() = true if cancel won the race to flip the flag before
            // sendInternal's gate check. That is the cooperative path we want; swallow so we can still assert on the
            // counter.
          }
          return null;
        });

        Future<?> cancelFuture = executor.submit(() -> {
          try {
            barrier.await();
            mailbox.cancel(new RuntimeException("race-test"));
          } catch (Exception e) {
            // ignore: cancel is defensive and may catch exceptions from a closed observer
          }
          return null;
        });

        senderFuture.get(10, TimeUnit.SECONDS);
        cancelFuture.get(10, TimeUnit.SECONDS);

        assertEquals(mailbox.getContentObserverCalls(), 1,
            "Iteration " + i + ": expected exactly one content observer to be opened, but got "
                + mailbox.getContentObserverCalls()
                + ". Two streams indicates a lazy-init race between send() and cancel().");
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor did not terminate in time");
    }
  }

  /// Regression test pinning the close()-on-never-sent-mailbox no-op contract.
  ///
  /// Before commit aeaacc893d ("Fix close() leaving the gRPC sender stream half-open") the close() path had an
  /// `if (_contentObserver != null)` short-circuit, so a mailbox that was constructed, never had `send`/`cancel`
  /// called on it, and then was closed was a silent no-op on the wire. aeaacc893d replaced that short-circuit
  /// with `ensureContentObserverInitialized()`, which opened a fresh gRPC stream just to push an error EOS and
  /// half-close — three round-trips on a stream that never needed to exist. For query stages pruned before any
  /// data flows that is wasted I/O and a new exception surface (channel open can throw at shutdown).
  ///
  /// This test reproduces the regression: it constructs a [CountingGrpcSendingMailbox], skips `send`/`cancel`,
  /// calls `close()`, and asserts the content-observer counter stayed at 0. On the regressed code the counter
  /// goes to 1 because `ensureContentObserverInitialized()` opens a stream.
  ///
  /// The cancel() path intentionally keeps the eager-open behaviour — there may be a receiver-side reader blocked
  /// on this stream waiting for the cancel signal — so this test is scoped to close() only.
  @Test
  public void closeOnNeverSentMailboxDoesNotOpenStream()
      throws Exception {
    ChannelManager channelManager = Mockito.mock(ChannelManager.class);
    CountingGrpcSendingMailbox mailbox = new CountingGrpcSendingMailbox("close-no-op", channelManager);

    mailbox.close();

    assertEquals(mailbox.getContentObserverCalls(), 0,
        "close() on a never-sent mailbox must be a silent no-op — opening a gRPC stream just to half-close it "
            + "wastes round-trips and introduces a new exception surface at shutdown.");
    Mockito.verifyNoInteractions(channelManager);
  }

  /// Test subclass: counts how many times [#getContentObserver] is called and returns a Mockito stub that mimics a
  /// healthy stream (isReady() → true, onNext / onCompleted / cancel are no-ops). The real gRPC machinery is never
  /// touched, so the only thing that controls observer-opening is the lazy-init logic inside [GrpcSendingMailbox].
  private static final class CountingGrpcSendingMailbox extends GrpcSendingMailbox {
    private final AtomicInteger _getContentObserverCalls = new AtomicInteger();

    CountingGrpcSendingMailbox(String id, ChannelManager channelManager) {
      // backpressureEnabled = false so awaitReady short-circuits without touching the observer's isReady() — we want
      // the test to exercise the lazy-init race specifically, not the back-pressure gate.
      super(id, channelManager, "localhost", 0, Long.MAX_VALUE,
          new StatMap<>(MailboxSendOperator.StatKey.class), 4 * 1024 * 1024, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    ClientCallStreamObserver<MailboxContent> getContentObserver() {
      _getContentObserverCalls.incrementAndGet();
      ClientCallStreamObserver<MailboxContent> observer = Mockito.mock(ClientCallStreamObserver.class);
      Mockito.when(observer.isReady()).thenReturn(true);
      return observer;
    }

    int getContentObserverCalls() {
      return _getContentObserverCalls.get();
    }
  }

  /// Regression test for the EOS-vs-cancel race that throws `IllegalStateException("call already half-closed")`.
  ///
  /// Before the fix, two interleavings could escape [GrpcSendingMailbox]:
  ///  * cancel acquires `_readyLock` first, pushes its error EOS payload + `onCompleted()`, then `send(Eos)` reaches
  ///    its outer half-close in `send(MseBlock.Eos)` and calls `_contentObserver.onCompleted()` on the now
  ///    half-closed stream — `ClientCallStreamObserver` raises `IllegalStateException`.
  ///  * cancel completes fully (payload + `onCompleted`) before `send(Eos)`'s `sendContent` even acquires the lock;
  ///    `sendContent` skips its in-lock `isTerminated()` re-check because `bypassReady=true` and calls
  ///    `_contentObserver.onNext(content)` on the half-closed stream — same `IllegalStateException`.
  ///
  /// The test uses a [HalfCloseEnforcingMailbox] whose observer mimics the gRPC contract: once `onCompleted()` has
  /// been observed, subsequent `onNext()` / `onCompleted()` raise `IllegalStateException`. Two threads — one calling
  /// `send(SuccessMseBlock)` and one calling `cancel(...)` — are released through a [CyclicBarrier] each iteration.
  /// Across many iterations, both interleavings show up.
  @Test
  public void concurrentSendEosAndCancelDoesNotThrow()
      throws Exception {
    int iterations = 200;
    ChannelManager channelManager = Mockito.mock(ChannelManager.class);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < iterations; i++) {
        HalfCloseEnforcingMailbox mailbox = new HalfCloseEnforcingMailbox("eos-race-" + i, channelManager);
        CyclicBarrier barrier = new CyclicBarrier(2);

        Future<Throwable> senderFuture = executor.submit(() -> {
          Throwable thrown = null;
          try (QueryThreadContext ctx = QueryThreadContext.openForMseTest()) {
            barrier.await();
            mailbox.send(SuccessMseBlock.INSTANCE, List.of());
          } catch (Throwable t) {
            thrown = t;
          }
          return thrown;
        });

        Future<Throwable> cancelFuture = executor.submit(() -> {
          Throwable thrown = null;
          try {
            barrier.await();
            mailbox.cancel(new RuntimeException("eos-race-test"));
          } catch (Throwable t) {
            thrown = t;
          }
          return thrown;
        });

        Throwable senderError = senderFuture.get(10, TimeUnit.SECONDS);
        Throwable cancelError = cancelFuture.get(10, TimeUnit.SECONDS);

        Assert.assertNull(senderError, "Iteration " + i + ": send(Eos) threw "
            + (senderError != null ? senderError.toString() : "null")
            + ". The EOS path must swallow IllegalStateException from a racing cancel.");
        Assert.assertNull(cancelError, "Iteration " + i + ": cancel() threw "
            + (cancelError != null ? cancelError.toString() : "null"));
        assertTrue(mailbox.isTerminated(),
            "Iteration " + i + ": mailbox should be terminated after send(Eos) + cancel");
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor did not terminate in time");
    }
  }

  /// Test subclass whose stub observer enforces the real gRPC half-close contract: once `onCompleted()` is observed,
  /// subsequent `onNext()` / `onCompleted()` raise `IllegalStateException("call already half-closed")`. That's the
  /// exact escape path Fix 1 prevents — without it, `send(Eos)` and `cancel` racing would propagate this exception
  /// out of `GrpcSendingMailbox`.
  private static final class HalfCloseEnforcingMailbox extends GrpcSendingMailbox {
    HalfCloseEnforcingMailbox(String id, ChannelManager channelManager) {
      // backpressureEnabled = false so awaitReady short-circuits without spinning on isReady(); we want to exercise
      // the half-close ordering, not the back-pressure gate.
      super(id, channelManager, "localhost", 0, Long.MAX_VALUE,
          new StatMap<>(MailboxSendOperator.StatKey.class), 4 * 1024 * 1024, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    ClientCallStreamObserver<MailboxContent> getContentObserver() {
      ClientCallStreamObserver<MailboxContent> observer = Mockito.mock(ClientCallStreamObserver.class);
      AtomicBoolean halfClosed = new AtomicBoolean();
      Mockito.when(observer.isReady()).thenReturn(true);
      Mockito.doAnswer(invocation -> {
        if (halfClosed.get()) {
          throw new IllegalStateException("call already half-closed");
        }
        return null;
      }).when(observer).onNext(Mockito.any());
      Mockito.doAnswer(invocation -> {
        if (!halfClosed.compareAndSet(false, true)) {
          throw new IllegalStateException("call already half-closed");
        }
        return null;
      }).when(observer).onCompleted();
      return observer;
    }
  }

  @Test(dataProvider = "byteBuffersDataProvider")
  public void testByteBuffersToByteStrings(int[] byteBufferSizes, int maxByteStringSize) {
    List<ByteBuffer> input =
        Arrays.stream(byteBufferSizes).mapToObj(this::randomByteBuffer).collect(Collectors.toList());
    ByteBuffer expected = concatenateBuffers(input);

    List<ByteString> output = GrpcSendingMailbox.toByteStrings(input, maxByteStringSize);
    for (ByteString chunk : output.subList(0, output.size() - 1)) {
      assertEquals(chunk.size(), maxByteStringSize);
    }
    assertTrue(output.get(output.size() - 1).size() <= maxByteStringSize);
    ByteBuffer actual =
        concatenateBuffers(output.stream().map(ByteString::asReadOnlyByteBuffer).collect(Collectors.toList()));

    assertEquals(actual, expected);
  }

  @Test(dataProvider = "testDataBlockToByteStringsProvider")
  public void testDataBlockToByteStrings(String name, int maxByteStringSize)
      throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteString> output = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);
    for (ByteString chunk : output) {
      assertTrue(chunk.size() <= maxByteStringSize);
    }

    DataBlock deserialized = DataBlockUtils.deserialize(
        output.stream().map(byteString -> byteString.asReadOnlyByteBuffer().slice()).collect(Collectors.toList()));

    DataBlockEquals.checkSameContent(dataBlock, deserialized, "Rebuilt data block (" + name + ") does not match.");
  }

  @Test(dataProvider = "testDataBlockToByteStringsProvider")
  public void testToByteStringDataBuffers(String name, int maxByteStringSize)
      throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteString> output = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);
    for (ByteString chunk : output) {
      assertTrue(chunk.size() <= maxByteStringSize);
    }

    List<DataBuffer> asGrpc =
        output.stream().map(byteString -> PinotByteBuffer.wrap(byteString.asReadOnlyByteBuffer().slice()))
        .collect(Collectors.toList());
    List<DataBuffer> directSerialize =
        dataBlock.serialize().stream().map(PinotByteBuffer::wrap).collect(Collectors.toList());

    try (CompoundDataBuffer grpc = new CompoundDataBuffer(asGrpc, ByteOrder.BIG_ENDIAN, false);
        CompoundDataBuffer direct = new CompoundDataBuffer(directSerialize, ByteOrder.BIG_ENDIAN, false)) {
      assertEquals(grpc, direct);
    }
  }

  @Test(dataProvider = "testDataBlockToByteStringsProvider")
  public void testDataBlockReusable(String name, int maxByteStringSize) throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteString> split1 = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);
    List<ByteString> split2 = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);

    assertEquals(split1, split2);
  }

  @DataProvider(name = "byteBuffersDataProvider")
  public Object[][] byteBuffersDataProvider() {
    // byteBufferSizes / maxByteStringSize
    return new Object[][]{
        {new int[]{1024}, 1024},
        {new int[]{1024}, 200},
        {new int[]{1024}, 1},
        {new int[]{100, 200, 300, 400}, 220},
        {new int[]{100, 200, 300, 400}, 1000}
    };
  }

  @DataProvider(name = "testDataBlockToByteStringsProvider")
  public Object[][] testDataBlockToByteStringsProvider() throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteBuffer> dataBlockSer = dataBlock.serialize();
    int totalSize = dataBlockSer.stream().mapToInt(ByteBuffer::remaining).sum();
    int largestChunk = dataBlockSer.stream().mapToInt(ByteBuffer::remaining).max().orElse(0);
    return new Object[][]{
        {"oneByteString", totalSize},
        {"largestChunk", largestChunk},
        {"maxInteger", Integer.MAX_VALUE},
        {"forceSplit", largestChunk / 3}
    };
  }

  private static DataBlock buildTestDataBlock()
      throws IOException {
    int numRows = 1;
    DataSchema dataSchema = new DataSchema(
        new String[]{
            "valueInt"
        },
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT
        });

    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(new Object[] {i});
    }

    return DataBlockBuilder.buildFromRows(rows, dataSchema);
  }

  private ByteBuffer concatenateBuffers(List<ByteBuffer> buffers) {
    int totalSize = buffers.stream().mapToInt(ByteBuffer::remaining).sum();
    ByteBuffer all = ByteBuffer.allocate(totalSize);
    for (ByteBuffer bb : buffers) {
      all.put(bb.slice());
    }
    return all.flip();
  }

  private ByteBuffer randomByteBuffer(int size) {
    ByteBuffer b = ByteBuffer.allocate(size);
    Random rnd = new Random();
    while (b.hasRemaining()) {
      b.put((byte) rnd.nextInt(256));
    }
    return b.flip();
  }
}
