package org.apache.pinot.query.mailbox;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


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
  public void tooManyDataBlockTheWriter()
      throws InterruptedException, TimeoutException {
    int size = 2;
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", size);

    // Offer up to capacity
    for (int i = 0; i < size; i++) {
      ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
      assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer up to capacity");
    }
    // Offer one more should be rejected
    try {
      ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
      fail("Should have thrown timeout exception, but " + status + " was returned");
    } catch (TimeoutException e) {
      // expected
    }
  }

  @Test
  public void offerAfterEos() throws InterruptedException, TimeoutException {
    ReceivingMailbox receivingMailbox = new ReceivingMailbox("id", 10);

    ReceivingMailbox.ReceivingMailboxStatus status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.SUCCESS, "Should be able to offer before EOS");

    status = receivingMailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK, "Should be able to offer EOS");

    // Offer after EOS should be rejected
    status = receivingMailbox.offer(DATA_BLOCK, List.of(), 10);
    assertEquals(status, ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED,
        "Should not be able to offer after EOS");
  }

  @Test
  public void shouldReadDataInOrder() throws InterruptedException, TimeoutException {
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
  public void lateEosRead() throws InterruptedException, TimeoutException {
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
  public void bufferedDataIsKeptOnSuccess() throws InterruptedException, TimeoutException {
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
  public void bufferedDataIsDiscardedOnError() throws InterruptedException, TimeoutException {
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
  public void dataAfterSuccess() throws InterruptedException, TimeoutException {
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
  public void dataAfterError() throws InterruptedException, TimeoutException {
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
}