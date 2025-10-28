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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ReceivingMailboxTest {

  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"intCol"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
  private static final MseBlock.Data DATA_BLOCK = new RowHeapDataBlock(List.of(), DATA_SCHEMA, null);
  private ReceivingMailbox.Reader _reader;

  @BeforeMethod
  public void setUp() {
    _reader = Mockito.mock(ReceivingMailbox.Reader.class);
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

    MseBlock[] offeredBlocks = new MseBlock[] {
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

    MseBlock[] offeredBlocks = new MseBlock[] {
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

    MseBlock[] offeredBlocks = new MseBlock[] {
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

    MseBlock[] offeredBlocks = new MseBlock[] {
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
}
