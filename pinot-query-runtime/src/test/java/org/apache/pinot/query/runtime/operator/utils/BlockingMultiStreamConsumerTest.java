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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator.Type;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for the per-stream pull mode of {@link BlockingMultiStreamConsumer} (the {@link
 * BlockingMultiStreamConsumer.StreamHandle} / {@link BlockingMultiStreamConsumer#streamHandles()} /
 * {@code StreamHandle.readBlocking()} API plus the single-mode guard). The round-robin path is covered separately by
 * {@code MailboxReceiveOperatorTest}.
 */
public class BlockingMultiStreamConsumerTest {
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
  private static final int RECEIVER_STAGE_ID = 0;
  private static final int SENDER_STAGE_ID = 1;
  private static final long FAR_FUTURE = System.currentTimeMillis() + 60_000L;

  private static BlockingMultiStreamConsumer.OfMseBlock newConsumer(long deadlineMs, List<FakeStream> streams) {
    OpChainExecutionContext context = mock(OpChainExecutionContext.class);
    when(context.getId()).thenReturn(mock(OpChainId.class));
    when(context.getStageId()).thenReturn(RECEIVER_STAGE_ID);
    when(context.getPassiveDeadlineMs()).thenReturn(deadlineMs);
    return new BlockingMultiStreamConsumer.OfMseBlock(context, streams, SENDER_STAGE_ID);
  }

  /**
   * Serializes a {@link MultiStageQueryStats} with a fixed leaf-side operator shape, so the consumer can merge it on
   * success EOS. Two EOS blocks with the same shape merge cleanly (see {@code
   * MailboxReceiveOperatorTest#differentUpstreamStatsProduceEmptyStats} for the differing-shape case).
   */
  private static List<DataBuffer> leafStats()
      throws IOException {
    return new MultiStageQueryStats.Builder(SENDER_STAGE_ID).addLast(
        open -> open.addLastOperator(Type.MAILBOX_SEND, new StatMap<>(MailboxSendOperator.StatKey.class))
            .addLastOperator(Type.LEAF, new StatMap<>(LeafOperator.StatKey.class))
            .close()).build().serialize();
  }

  @Test
  public void dataThenEosPerHandleKeepsStats()
      throws IOException {
    Object[] row0 = new Object[]{0, 0};
    Object[] row1 = new Object[]{1, 1};
    FakeStream s0 = new FakeStream("s0");
    FakeStream s1 = new FakeStream("s1");
    s0.enqueue(OperatorTestUtil.blockWithStats(DATA_SCHEMA, row0));
    s0.enqueue(OperatorTestUtil.eosWithStats(leafStats()));
    s1.enqueue(OperatorTestUtil.blockWithStats(DATA_SCHEMA, row1));
    s1.enqueue(OperatorTestUtil.eosWithStats(leafStats()));

    BlockingMultiStreamConsumer.OfMseBlock consumer = newConsumer(FAR_FUTURE, List.of(s0, s1));
    List<BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats>> handles =
        consumer.streamHandles();
    assertEquals(handles.size(), 2);
    // streamHandles() is idempotent.
    assertSame(consumer.streamHandles(), handles);

    BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats> h0 = handles.get(0);
    BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats> h1 = handles.get(1);
    assertEquals(h0.getId(), "s0");
    assertEquals(h1.getId(), "s1");

    // Each handle reads its own data block first, independent of the other.
    assertEquals(((MseBlock.Data) h0.readBlocking().getBlock()).asRowHeap().getRows().get(0), row0);
    assertFalse(h0.isExhausted());
    assertEquals(((MseBlock.Data) h1.readBlocking().getBlock()).asRowHeap().getRows().get(0), row1);
    assertFalse(h1.isExhausted());

    // Then the success EOS; the handle is exhausted afterwards.
    assertTrue(h0.readBlocking().getBlock().isSuccess());
    assertTrue(h0.isExhausted());
    assertTrue(h1.readBlocking().getBlock().isSuccess());
    assertTrue(h1.isExhausted());

    // Reading an exhausted handle returns the cached EOS without polling (and without re-merging stats).
    assertTrue(h0.readBlocking().getBlock().isSuccess());

    // Identical upstream shapes merge cleanly, so upstream stats survive.
    MultiStageQueryStats stats = consumer.calculateStats();
    assertNotNull(stats.getUpstreamStageStats(SENDER_STAGE_ID),
        "Upstream stats should be retained when both EOS blocks share the same shape");
  }

  @Test
  public void errorIsCachedAndShortCircuitsEveryHandle() {
    FakeStream s0 = new FakeStream("s0");
    FakeStream s1 = new FakeStream("s1");
    s0.enqueue(OperatorTestUtil.errorWithEmptyStats(new RuntimeException("boom")));
    // s1 has data, but the global error must short-circuit it before that data is ever read.
    s1.enqueue(OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}));

    BlockingMultiStreamConsumer.OfMseBlock consumer = newConsumer(FAR_FUTURE, List.of(s0, s1));
    List<BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats>> handles =
        consumer.streamHandles();

    ReceivingMailbox.MseBlockWithStats err0 = handles.get(0).readBlocking();
    assertTrue(err0.getBlock().isError());
    assertTrue(((ErrorMseBlock) err0.getBlock()).getErrorMessages().get(QueryErrorCode.UNKNOWN).contains("boom"));

    // The other handle returns the very same error element, not its pending data block.
    ReceivingMailbox.MseBlockWithStats err1 = handles.get(1).readBlocking();
    assertSame(err1, err0);
  }

  @Test
  public void timeoutReturnsErrorBlockWithSerializedStats() {
    FakeStream s0 = new FakeStream("s0");
    // Deadline already in the past => the deadline loop times out immediately with no data.
    BlockingMultiStreamConsumer.OfMseBlock consumer =
        newConsumer(System.currentTimeMillis() - 1L, List.of(s0));
    BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats> h0 =
        consumer.streamHandles().get(0);

    ReceivingMailbox.MseBlockWithStats block = h0.readBlocking();
    assertTrue(block.getBlock().isError());
    assertTrue(((ErrorMseBlock) block.getBlock()).getErrorMessages().containsKey(QueryErrorCode.EXECUTION_TIMEOUT));
    // The timeout element carries the serialized accumulated stats (mirrors onException(code, msg)).
    assertNotNull(block.getSerializedStats());

    // The timeout latched a global error, so the handle keeps returning it.
    assertSame(h0.readBlocking(), block);
  }

  @Test
  public void modeGuardRejectsMixingReads() {
    // Round-robin first (empty mailboxes -> immediate success), then per-stream must throw.
    BlockingMultiStreamConsumer.OfMseBlock roundRobinFirst = newConsumer(FAR_FUTURE, List.of());
    assertTrue(roundRobinFirst.readBlockBlocking().getBlock().isSuccess());
    assertThrows(IllegalStateException.class, roundRobinFirst::streamHandles);

    // Per-stream first, then round-robin must throw.
    BlockingMultiStreamConsumer.OfMseBlock perStreamFirst = newConsumer(FAR_FUTURE, List.of(new FakeStream("s0")));
    perStreamFirst.streamHandles();
    assertThrows(IllegalStateException.class, perStreamFirst::readBlockBlocking);
  }

  @Test
  public void readBlockingUnblocksOnNewDataNotification()
      throws InterruptedException {
    FakeStream s0 = new FakeStream("s0");
    BlockingMultiStreamConsumer.OfMseBlock consumer = newConsumer(FAR_FUTURE, List.of(s0));
    BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats> h0 =
        consumer.streamHandles().get(0);

    ReceivingMailbox.MseBlockWithStats data = OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{7, 7});
    // Producer thread: after the consumer is (very likely) blocked, enqueue a block and fire the new-data callback.
    Thread producer = new Thread(() -> {
      try {
        Thread.sleep(150L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      s0.enqueue(data);
      s0.fireNewData();
    });
    producer.start();

    // First optimistic poll sees nothing, so this blocks on the shared wakeup until the producer fires it.
    ReceivingMailbox.MseBlockWithStats read = h0.readBlocking();
    producer.join();
    assertSame(read, data);
  }

  @Test
  public void earlyTerminateDelegatesToStream() {
    FakeStream s0 = new FakeStream("s0");
    BlockingMultiStreamConsumer.OfMseBlock consumer = newConsumer(FAR_FUTURE, List.of(s0));
    consumer.streamHandles().get(0).earlyTerminate();
    assertTrue(s0._earlyTerminated);
  }

  /**
   * {@link BlockingMultiStreamConsumer.StreamHandle#poll()} is the non-blocking primitive the k-way merge's cooperative
   * drain relies on: it must never park, must surface data/EOS exactly like {@code readBlocking()} does, and must not
   * re-poll an already-exhausted stream (mirrors the mailbox-release comment on the {@code Handle} implementation).
   */
  @Test
  public void pollIsNonBlockingAndTracksExhaustion() {
    FakeStream s0 = new FakeStream("s0");
    BlockingMultiStreamConsumer.OfMseBlock consumer = newConsumer(FAR_FUTURE, List.of(s0));
    BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats> h0 =
        consumer.streamHandles().get(0);

    // Nothing scripted yet: poll() must return null immediately rather than parking.
    assertNull(h0.poll());
    assertFalse(h0.isExhausted());

    Object[] row = new Object[]{9, 9};
    s0.enqueue(OperatorTestUtil.blockWithStats(DATA_SCHEMA, row));
    ReceivingMailbox.MseBlockWithStats data = h0.poll();
    assertNotNull(data);
    assertEquals(((MseBlock.Data) data.getBlock()).asRowHeap().getRows().get(0), row);
    assertFalse(h0.isExhausted());

    // Drained again with nothing scripted: back to null, not blocking.
    assertNull(h0.poll());

    s0.enqueue(OperatorTestUtil.eosWithEmptyStats());
    assertTrue(h0.poll().getBlock().isSuccess());
    assertTrue(h0.isExhausted());

    // An exhausted handle's poll() returns null without touching the (already-released) underlying stream again.
    assertNull(h0.poll());
  }

  @Test
  public void pollShortCircuitsOnGlobalError() {
    FakeStream s0 = new FakeStream("s0");
    FakeStream s1 = new FakeStream("s1");
    s0.enqueue(OperatorTestUtil.errorWithEmptyStats(new RuntimeException("boom")));
    s1.enqueue(OperatorTestUtil.blockWithStats(DATA_SCHEMA, new Object[]{1, 1}));

    BlockingMultiStreamConsumer.OfMseBlock consumer = newConsumer(FAR_FUTURE, List.of(s0, s1));
    List<BlockingMultiStreamConsumer.StreamHandle<ReceivingMailbox.MseBlockWithStats>> handles =
        consumer.streamHandles();

    ReceivingMailbox.MseBlockWithStats err = handles.get(0).poll();
    assertNotNull(err);
    assertTrue(err.getBlock().isError());
    // The other stream still has data queued, but the global error must short-circuit its poll() too.
    assertSame(handles.get(1).poll(), err);
  }

  @Test
  public void awaitDataOrTerminalReturnsNullOnWakeAndErrorOnTimeout()
      throws InterruptedException {
    FakeStream s0 = new FakeStream("s0");
    BlockingMultiStreamConsumer.OfMseBlock consumer = newConsumer(FAR_FUTURE, List.of(s0));
    consumer.streamHandles();

    Thread producer = new Thread(() -> {
      try {
        Thread.sleep(150L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      s0.fireNewData();
    });
    producer.start();
    // Parks until the producer's new-data signal wakes it; returns null (caller should re-poll) rather than an
    // element, since awaitDataOrTerminal() never reads from any stream itself.
    assertNull(consumer.awaitDataOrTerminal());
    producer.join();

    // A deadline already in the past must return the cached timeout error instead of parking.
    BlockingMultiStreamConsumer.OfMseBlock timedOut =
        newConsumer(System.currentTimeMillis() - 1L, List.of(new FakeStream("s1")));
    timedOut.streamHandles();
    ReceivingMailbox.MseBlockWithStats element = timedOut.awaitDataOrTerminal();
    assertNotNull(element);
    assertTrue(element.getBlock().isError());
    assertTrue(((ErrorMseBlock) element.getBlock()).getErrorMessages().containsKey(QueryErrorCode.EXECUTION_TIMEOUT));
  }

  /**
   * A hand-written {@link AsyncStream} whose {@link #poll()} drains a scripted queue (empty queue => not ready yet) and
   * which exposes a way to fire the captured new-data callback, so tests can drive the per-stream blocking loop without
   * any real mailbox infrastructure.
   */
  private static class FakeStream implements AsyncStream<ReceivingMailbox.MseBlockWithStats> {
    private final Object _id;
    private final Deque<ReceivingMailbox.MseBlockWithStats> _scripted = new ArrayDeque<>();
    @Nullable
    private OnNewData _listener;
    private volatile boolean _earlyTerminated;

    FakeStream(Object id) {
      _id = id;
    }

    void enqueue(ReceivingMailbox.MseBlockWithStats block) {
      _scripted.addLast(block);
    }

    void fireNewData() {
      if (_listener != null) {
        _listener.newDataAvailable();
      }
    }

    @Override
    public Object getId() {
      return _id;
    }

    @Nullable
    @Override
    public ReceivingMailbox.MseBlockWithStats poll() {
      return _scripted.pollFirst();
    }

    @Override
    public void addOnNewDataListener(OnNewData onNewData) {
      _listener = onNewData;
    }

    @Override
    public void cancel() {
    }

    @Override
    public void earlyTerminate() {
      _earlyTerminated = true;
    }
  }
}
