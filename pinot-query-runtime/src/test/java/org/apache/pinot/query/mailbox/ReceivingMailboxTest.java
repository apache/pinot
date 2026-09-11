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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class ReceivingMailboxTest {

  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
  private static final MseBlock.Data DATA_BLOCK = new RowHeapDataBlock(List.of(), DATA_SCHEMA, null);
  private static final ErrorMseBlock ERROR_BLOCK = ErrorMseBlock.fromError(QueryErrorCode.INTERNAL, "test");
  private ReceivingMailbox.Reader _reader;

  @BeforeClass
  public void setUp() {
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
  }

  @BeforeMethod
  public void setUpMethod() {
    _reader = mock(ReceivingMailbox.Reader.class);
  }

  @Test
  public void tooManyDataBlocksTheWriter() {
    int size = 2;
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", size);
    receivingMailbox.registeredReader(_reader);

    // Offer up to capacity
    for (int i = 0; i < size; i++) {
      ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
      assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer up to capacity");
    }
    // Offer one more should cause timeout
    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK,
        "Should timeout when offering over capacity");
    ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
    assertNotNull(read, "Should be able to read the timeout error block");
    MseBlock block = read.getBlock();
    assertTrue(block.isError(), "The block should be an error block");
    ErrorMseBlock errorBlock = (ErrorMseBlock) block;
    assertTrue(errorBlock.getErrorMessages().containsKey(QueryErrorCode.EXECUTION_TIMEOUT),
        "The error block should contain timeout error");
  }

  @Test
  public void offerAfterEos() {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);

    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer before EOS");

    status = receivingMailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK, "Should be able to offer EOS");

    // Data offer after EOS should be rejected
    status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
        "Should not be able to offer after EOS");

    // Success offer after EOS should be rejected
    status = receivingMailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
        "Should not be able to offer after EOS");

    // Error offer after EOS should be rejected
    status = receivingMailbox.offer(ErrorMseBlock.fromError(QueryErrorCode.INTERNAL, "test"), List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
        "Should not be able to offer after EOS");
  }

  @Test
  public void shouldReadDataInOrder() {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);
    receivingMailbox.registeredReader(_reader);

    MseBlock[] offeredBlocks = new MseBlock[]{
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null)
    };
    for (MseBlock block : offeredBlocks) {
      ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(block, List.of(), 10);
      assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer before EOS");
    }

    for (MseBlock offered : offeredBlocks) {
      ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
      assertNotNull(read, "Should be able to read offered blocks");
      assertEquals(read.getBlock(), offered, "Should read blocks in the order they were offered");
    }

    assertNull(receivingMailbox.poll(), "No more blocks to read, should return null");
  }

  @Test
  public void lateEosRead() {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);
    receivingMailbox.registeredReader(_reader);

    MseBlock[] offeredBlocks = new MseBlock[]{
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null)
    };
    for (MseBlock block : offeredBlocks) {
      ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(block, List.of(), 10);
      assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer before EOS");
    }

    for (MseBlock offered : offeredBlocks) {
      ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
      assertNotNull(read, "Should be able to read offered blocks");
      assertEquals(read.getBlock(), offered, "Should read blocks in the order they were offered");
    }

    // Offer EOS after all data blocks are read
    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK, "Should be able to offer EOS");

    ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
    assertNotNull(read, "Should be able to read EOS");
    assertEquals(read.getBlock(), SuccessMseBlock.INSTANCE, "Should read EOS block");

    // Offer after EOS should be rejected
    status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
        "Should not be able to offer after EOS");

    // Poll again should return the EOS
    ReceivingMailbox.MseBlockWithStats latePoll = receivingMailbox.poll();
    assertNotNull(latePoll, "Should be able to read EOS");
    assertEquals(latePoll.getBlock(), SuccessMseBlock.INSTANCE, "Should read EOS block");
  }

  @Test
  public void bufferedDataIsKeptOnSuccess() {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);
    receivingMailbox.registeredReader(_reader);

    MseBlock[] offeredBlocks = new MseBlock[]{
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null)
    };
    for (MseBlock block : offeredBlocks) {
      ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(block, List.of(), 10);
      assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer before EOS");
    }
    // Offer EOS
    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK, "Should be able to offer EOS");

    for (MseBlock offered : offeredBlocks) {
      ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
      assertNotNull(read, "Should be able to read offered blocks");
      assertEquals(read.getBlock(), offered, "Should read blocks in the order they were offered");
    }
    ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
    assertNotNull(read, "Should be able to read EOS");
    assertEquals(read.getBlock(), SuccessMseBlock.INSTANCE, "Should read EOS block");
  }

  @Test
  public void bufferedDataIsDiscardedOnError() {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);
    receivingMailbox.registeredReader(_reader);
    ErrorMseBlock errorBlock = ErrorMseBlock.fromException(new RuntimeException("Test error"));

    MseBlock[] offeredBlocks = new MseBlock[]{
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null),
        new RowHeapDataBlock(List.of(), DATA_SCHEMA, null)
    };
    for (MseBlock block : offeredBlocks) {
      ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(block, List.of(), 10);
      assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer before EOS");
    }
    // Offer EOS
    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(errorBlock, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK, "Should be able to offer EOS");

    ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
    assertNotNull(read, "Should be able to read EOS");
    assertEquals(read.getBlock(), errorBlock, "Should read EOS block");
  }

  @Test
  public void dataAfterSuccess() {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);
    receivingMailbox.registeredReader(_reader);

    // Offer EOS
    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK, "Should be able to offer EOS");

    ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
    assertNotNull(read, "Should be able to read EOS");
    assertEquals(read.getBlock(), SuccessMseBlock.INSTANCE, "Should read EOS block");

    // Offer after EOS should be rejected
    status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
        "Should not be able to offer after EOS");

    // Poll again should return the EOS
    ReceivingMailbox.MseBlockWithStats latePoll = receivingMailbox.poll();
    assertNotNull(latePoll, "Should be able to read EOS");
    assertEquals(latePoll.getBlock(), SuccessMseBlock.INSTANCE, "Should read EOS block");
  }

  @Test
  public void dataAfterError() {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);
    receivingMailbox.registeredReader(_reader);

    // Offer EOS
    ErrorMseBlock errorBlock = ErrorMseBlock.fromException(new RuntimeException("Test error"));
    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(errorBlock, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK, "Should be able to offer EOS");

    ReceivingMailbox.MseBlockWithStats read = receivingMailbox.poll();
    assertNotNull(read, "Should be able to read EOS");
    assertEquals(read.getBlock(), errorBlock, "Should read EOS block");

    // Offer after EOS should be rejected
    status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
        "Should not be able to offer after EOS");

    // Poll again should return the EOS
    ReceivingMailbox.MseBlockWithStats latePoll = receivingMailbox.poll();
    assertNotNull(latePoll, "Should be able to read EOS");
    assertEquals(latePoll.getBlock(), errorBlock, "Should read EOS block");
  }

  @Test(timeOut = 10_000)
  public void earlyTerminateUnblocksOffers()
      throws ExecutionException, InterruptedException, TimeoutException {
    int maxPendingBlocks = 2;
    ReceivingMailbox mailbox = new ReceivingMailbox("id", maxPendingBlocks);

    ExecutorService offerEx = Executors.newCachedThreadPool();
    try {
      for (int i = 0; i < maxPendingBlocks; i++) {
        CompletableFuture<ReceivingMailbox.ReceivingMailboxStatus> future = offer(DATA_BLOCK, mailbox, offerEx);
        future.join();
      }
      CompletableFuture<ReceivingMailbox.ReceivingMailboxStatus> blocked = offer(DATA_BLOCK, mailbox, offerEx);
      Thread.sleep(100); // a little wait to facilitate the offer to be blocked
      mailbox.earlyTerminate();
      ReceivingMailbox.ReceivingMailboxStatus status = blocked.get(10_000, TimeUnit.MILLISECONDS);
      assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.WAITING_EOS);
    } finally {
      offerEx.shutdownNow();
    }
  }

  @Test(timeOut = 10_000)
  public void readingUnblocksWriters()
      throws ExecutionException, InterruptedException {
    int maxPendingBlocks = 2;
    ReceivingMailbox mailbox = new ReceivingMailbox("id", maxPendingBlocks);
    mailbox.registeredReader(_reader);

    ExecutorService offerEx = Executors.newSingleThreadExecutor();
    try {
      for (int i = 0; i < maxPendingBlocks; i++) {
        offer(DATA_BLOCK, mailbox, offerEx);
      }
      CompletableFuture<ReceivingMailbox.ReceivingMailboxStatus> blocked = offer(DATA_BLOCK, mailbox, offerEx);

      int numRead = 0;
      do {
        ReceivingMailbox.MseBlockWithStats poll = mailbox.poll();
        if (poll == null) {
          // No more to read
          Thread.sleep(10);
        } else {
          numRead++;
          assertEquals(poll.getBlock(), DATA_BLOCK, "The read block should match the sent block");
        }
      } while (numRead < maxPendingBlocks + 1);
      assertEquals(mailbox.getNumPendingBlocks(), 0, "All blocks should have been read");
      assertTrue(blocked.isDone(), "The blocked offer should be unblocked by reading");
      assertEquals(blocked.get(), ReceivingMailbox.ReceivingMailboxStatus.SUCCESS,
          "The unblocked offer should succeed");
    } finally {
      offerEx.shutdownNow();
    }
  }

  CompletableFuture<ReceivingMailbox.ReceivingMailboxStatus> offer(MseBlock block, ReceivingMailbox receivingMailbox,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> receivingMailbox.offer(block, List.of(), 10_000), executor);
  }

  @Test
  public void testResourceTracking()
      throws Exception {
    QueryThreadContext threadContext = mock(QueryThreadContext.class);

    // Receive after setting thread context
    TestReceivingMailbox receivingMailbox = new TestReceivingMailbox("id", threadContext);
    receivingMailbox.registerReceiveOperatorThreadContext(threadContext);
    receivingMailbox.offerRaw(DataBlockUtils.serialize(DATA_BLOCK.asSerialized().getDataBlock()), 10_000);
    assertTrue(receivingMailbox._resourceUsageUpdated);

    // Receive before setting thread context
    receivingMailbox = new TestReceivingMailbox("id", threadContext);
    receivingMailbox.offerRaw(DataBlockUtils.serialize(DATA_BLOCK.asSerialized().getDataBlock()), 10_000);
    receivingMailbox.registerReceiveOperatorThreadContext(threadContext);
    assertTrue(receivingMailbox._resourceUsageUpdated);
  }

  /// An error EOS drains the queued data blocks and makes every parked sender give up. While those offers are still
  /// in flight `poll` returns `null` and asks the reader to wait for them, but a sender that gives up returns
  /// `ALREADY_TERMINATED` without notifying, and the reader's notification slot coalesces, so the EOS notification
  /// cannot stand in for the missing one. Unless the last pending offer notifies on its way out, the reader parks
  /// until the query deadline and the query reports a timeout instead of the error that caused it.
  ///
  /// Asserting on the notification count rather than on a blocking read keeps this deterministic: the wake-up is
  /// delivered when the last pending offer completes, whether or not the reader managed to poll first.
  @Test(timeOut = 60_000)
  public void shouldNotifyReaderWhenLastPendingOfferGivesUp()
      throws Exception {
    CountingReader reader = new CountingReader();
    ReceivingMailbox mailbox = new ReceivingMailbox("id", 1);
    mailbox.registeredReader(reader);

    // Fill the only queue slot, so the sender below has to park waiting for space.
    assertEquals(mailbox.offer(DATA_BLOCK, List.of(), 10_000), ReceivingMailbox.ReceivingMailboxStatus.SUCCESS);
    assertEquals(reader._notifications.get(), 1, "The buffered data block should notify the reader");

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      AtomicReference<Thread> senderThread = new AtomicReference<>();
      CountDownLatch offering = new CountDownLatch(1);
      Future<ReceivingMailbox.ReceivingMailboxStatus> sender = executor.submit(() -> {
        senderThread.set(Thread.currentThread());
        offering.countDown();
        return mailbox.offer(DATA_BLOCK, List.of(), 30_000);
      });
      assertTrue(offering.await(30, TimeUnit.SECONDS), "Sender should reach the offer call");
      awaitParked(senderThread.get());

      // The error EOS drains the buffered block and rejects the parked sender.
      assertEquals(mailbox.offer(ERROR_BLOCK, List.of(), 10_000), ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK);
      assertEquals(reader._notifications.get(), 2, "The EOS should notify the reader");
      assertEquals(sender.get(30, TimeUnit.SECONDS), ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
          "The parked sender should be rejected by the error EOS");

      // Once the sender has given up the mailbox is readable again, so the reader owes one more notification. Without
      // it a reader that already polled and was told to wait for the pending offer is never woken again.
      assertEquals(reader._notifications.get(), 3,
          "The last pending offer should notify the reader on its way out, but the reader was left blocked");
      ReceivingMailbox.MseBlockWithStats read = mailbox.poll();
      assertNotNull(read, "The reader should be able to read the EOS");
      assertTrue(read.getBlock().isError(), "The EOS should be the error block");
    } finally {
      executor.shutdownNow();
    }
  }

  /// Waits until the sender thread is parked waiting for space in the queue.
  private static void awaitParked(Thread thread)
      throws Exception {
    long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (thread.getState() != Thread.State.TIMED_WAITING) {
      assertTrue(System.nanoTime() < deadlineNs, "Sender never parked waiting for space in the queue");
      Thread.sleep(1);
    }
  }

  /// Counts the wake-ups delivered to the reader. The production reader coalesces them into a single slot and always
  /// polls the mailbox before parking on that slot, so a wake-up dropped while it is parked is never recovered.
  private static class CountingReader implements ReceivingMailbox.Reader {
    final AtomicInteger _notifications = new AtomicInteger();

    @Override
    public void blockReadyToRead() {
      _notifications.incrementAndGet();
    }
  }

  private static class TestReceivingMailbox extends ReceivingMailbox {
    final QueryThreadContext _expectedThreadContext;
    boolean _resourceUsageUpdated;

    public TestReceivingMailbox(String id, QueryThreadContext expectedThreadContext) {
      super(id);
      _expectedThreadContext = expectedThreadContext;
    }

    @Override
    void updateResourceUsage(QueryThreadContext threadContext, long cpuTimeNs, long allocatedBytes) {
      assertSame(threadContext, _expectedThreadContext);
      assertTrue(cpuTimeNs > 0);
      assertTrue(allocatedBytes > 0);
      _resourceUsageUpdated = true;
    }
  }
}
