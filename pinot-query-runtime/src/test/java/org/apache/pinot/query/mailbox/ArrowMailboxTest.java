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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.ReceivingMailbox.ReceivingMailboxStatus;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;


/** Exercises mailbox ownership independently of gRPC scheduling. */
public class ArrowMailboxTest {
  static final DataSchema SCHEMA = new DataSchema(new String[]{"key", "text", "bytes"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.BYTES});

  @Test
  public void testChunkedIpcOutlivesSender()
      throws Exception {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext receiver = newContext(buffers, 1)) {
      ReceivingMailbox mailbox = newMailbox(2);
      assertFalse(mailbox.canReceiveArrow());
      mailbox.enableArrow(receiver);
      assertTrue(mailbox.canReceiveArrow());
      List<ByteBuffer> payload = payload(sender);
      sender.close();
      assertEquals(mailbox.offerRaw(payload, 1000), ReceivingMailboxStatus.SUCCESS);
      ArrowBlock received = (ArrowBlock) mailbox.poll().getBlock();
      try {
        assertRows(received);
      } finally {
        received.release();
      }
      assertEquals(receiver.getAllocator().getAllocatedMemory(), 0L);
      mailbox.closeArrow();
      assertFalse(mailbox.canReceiveArrow());
    }
  }

  @Test
  public void testLocalBroadcastOwnsIndependentRoots() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext receiver1 = newContext(buffers, 1);
        ArrowQueryContext receiver2 = newContext(buffers, 2)) {
      ReceivingMailbox first = newMailbox(1);
      ReceivingMailbox second = newMailbox(1);
      first.enableArrow(receiver1);
      second.enableArrow(receiver2);
      ArrowBlock source = newBlock(sender);
      try {
        assertEquals(first.offer(source, List.of(), 1000), ReceivingMailboxStatus.SUCCESS);
        assertEquals(second.offer(source, List.of(), 1000), ReceivingMailboxStatus.SUCCESS);
        assertRows(source);
      } finally {
        source.release();
      }
      sender.close();
      first.cancel();
      receiver1.close();
      ArrowBlock received = (ArrowBlock) second.poll().getBlock();
      try {
        assertNotSame(received, source);
        assertRows(received);
      } finally {
        received.release();
      }
      second.closeArrow();
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testLocalUnknownCapabilityCopiesToHeap() {
    try (ArrowBuffers buffers = newBuffers(); ArrowQueryContext sender = newContext(buffers, 0)) {
      ReceivingMailbox mailbox = newMailbox(1);
      ArrowBlock source = newBlock(sender);
      try {
        assertEquals(mailbox.offer(source, List.of(), 1000), ReceivingMailboxStatus.SUCCESS);
      } finally {
        source.release();
      }
      sender.close();
      MseBlock.Data received = (MseBlock.Data) mailbox.poll().getBlock();
      assertTrue(received.isRowHeap());
      assertRows(received);
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testBlockedHeapFallbackDoesNotPreventReceiverRegistration()
      throws Exception {
    try (ArrowBuffers buffers = newBuffers(); ArrowQueryContext sender = newContext(buffers, 0)) {
      ArrowBlock source = newBlock(sender);
      ReceivingMailbox mailbox = new ReceivingMailbox("arrow-startup", 1);
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        assertEquals(mailbox.offer(source, List.of(), 1000), ReceivingMailboxStatus.SUCCESS);
        AtomicReference<Thread> writer = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Future<ReceivingMailboxStatus> pending = executor.submit(() -> {
          writer.set(Thread.currentThread());
          started.countDown();
          return mailbox.offer(source, List.of(), 10_000);
        });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        TestUtils.waitForCondition(ignored -> writer.get().getState() == Thread.State.TIMED_WAITING,
            5000L, "Sender did not block on the full queue");
        executor.submit(() -> mailbox.registerReceiveOperatorThreadContext(null)).get(5, TimeUnit.SECONDS);
        mailbox.registeredReader(() -> { });
        assertRows((MseBlock.Data) mailbox.poll().getBlock());
        assertEquals(pending.get(5, TimeUnit.SECONDS), ReceivingMailboxStatus.SUCCESS);
        assertRows((MseBlock.Data) mailbox.poll().getBlock());
      } finally {
        mailbox.cancel();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        source.release();
      }
    }
  }

  @Test
  public void testUnnegotiatedIpcIsRejected()
      throws Exception {
    try (ArrowBuffers buffers = newBuffers(); ArrowQueryContext sender = newContext(buffers, 0)) {
      ReceivingMailbox mailbox = newMailbox(1);
      assertEquals(mailbox.offerRaw(payload(sender), 1000), ReceivingMailboxStatus.LAST_BLOCK);
      assertTrue(mailbox.poll().getBlock().isError());
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testEarlyTerminationRejectsIpcWithoutAllocating()
      throws Exception {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext receiver = newContext(buffers, 1)) {
      ReceivingMailbox mailbox = newMailbox(1);
      mailbox.enableArrow(receiver);
      List<ByteBuffer> payload = payload(sender);
      assertEquals(mailbox.offerRaw(payload, 1000), ReceivingMailboxStatus.SUCCESS);
      mailbox.earlyTerminate();
      assertEquals(receiver.getAllocator().getAllocatedMemory(), 0L);
      assertEquals(mailbox.offerRaw(payload, 1000), ReceivingMailboxStatus.WAITING_EOS);
      assertEquals(receiver.getAllocator().getAllocatedMemory(), 0L);
      assertEquals(mailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 1000), ReceivingMailboxStatus.LAST_BLOCK);
      assertTrue(mailbox.poll().getBlock().isSuccess());
    }
  }

  @Test
  public void testCancelAfterEosDrainsRetainedBlocks() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext receiver = newContext(buffers, 1)) {
      ReceivingMailbox mailbox = newMailbox(1);
      mailbox.enableArrow(receiver);
      ArrowBlock source = newBlock(sender);
      try {
        assertEquals(mailbox.offer(source, List.of(), 1000), ReceivingMailboxStatus.SUCCESS);
        assertEquals(mailbox.offer(SuccessMseBlock.INSTANCE, List.of(), 1000), ReceivingMailboxStatus.LAST_BLOCK);
        mailbox.cancel();
        assertEquals(mailbox.getNumPendingBlocks(), 0);
        receiver.close();
        assertRows(source);
      } finally {
        source.release();
      }
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testTimeoutReleasesRejectedAndQueuedIpc()
      throws Exception {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext receiver = newContext(buffers, 1)) {
      ReceivingMailbox mailbox = newMailbox(1);
      mailbox.enableArrow(receiver);
      List<ByteBuffer> payload = payload(sender);
      assertEquals(mailbox.offerRaw(payload, 1000), ReceivingMailboxStatus.SUCCESS);
      assertEquals(mailbox.offerRaw(payload, 1), ReceivingMailboxStatus.LAST_BLOCK);
      assertTrue(mailbox.poll().getBlock().isError());
      assertEquals(receiver.getAllocator().getAllocatedMemory(), 0L);
      mailbox.cancel();
    }
  }

  @Test
  public void testCancelWaitsForBlockedIpcOffer()
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext receiver = newContext(buffers, 1)) {
      ReceivingMailbox mailbox = newMailbox(1);
      mailbox.enableArrow(receiver);
      List<ByteBuffer> payload = payload(sender);
      assertEquals(mailbox.offerRaw(payload, 1000), ReceivingMailboxStatus.SUCCESS);
      CountDownLatch started = new CountDownLatch(1);
      AtomicReference<Thread> writer = new AtomicReference<>();
      Future<ReceivingMailboxStatus> pending = executor.submit(() -> {
        writer.set(Thread.currentThread());
        started.countDown();
        return mailbox.offerRaw(payload, 10_000);
      });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      TestUtils.waitForCondition(ignored -> writer.get().getState() == Thread.State.TIMED_WAITING,
          5000L, "Writer did not block on the full queue");
      mailbox.cancel();
      receiver.close();
      ReceivingMailboxStatus status = pending.get(5, TimeUnit.SECONDS);
      assertTrue(status == ReceivingMailboxStatus.ALREADY_TERMINATED || status == ReceivingMailboxStatus.WAITING_EOS);
      assertEquals(buffers.getAllocatedMemory(), 0L);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testTruncatedIpcReclaimsPartialDecode()
      throws Exception {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext receiver = newContext(buffers, 1)) {
      ReceivingMailbox mailbox = newMailbox(1);
      mailbox.enableArrow(receiver);
      List<ByteBuffer> payload = payload(sender);
      ByteBuffer last = payload.get(payload.size() - 1);
      last.limit(last.limit() - 1);
      assertEquals(mailbox.offerRaw(payload, 1000), ReceivingMailboxStatus.LAST_BLOCK);
      assertTrue(mailbox.poll().getBlock().isError());
      assertEquals(receiver.getAllocator().getAllocatedMemory(), 0L);
      mailbox.cancel();
    }
  }

  @Test
  public void testReceiverBudgetFailureIsResourceError()
      throws Exception {
    try (ArrowBuffers sourceBuffers = newBuffers();
        ArrowBuffers receiverBuffers = new ArrowBuffers(true, new RootAllocator(1024 * 1024), 0, 1);
        ArrowQueryContext sender = newContext(sourceBuffers, 0);
        ArrowQueryContext receiver = newContext(receiverBuffers, 1)) {
      ReceivingMailbox mailbox = newMailbox(1);
      mailbox.enableArrow(receiver);
      assertEquals(mailbox.offerRaw(payload(sender), 1000), ReceivingMailboxStatus.LAST_BLOCK);
      ErrorMseBlock error = (ErrorMseBlock) mailbox.poll().getBlock();
      assertTrue(error.getErrorMessages().containsKey(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED),
          error.getErrorMessages().toString());
      assertEquals(receiver.getAllocator().getAllocatedMemory(), 0L);
      mailbox.cancel();
    }
  }

  static ArrowBuffers newBuffers() {
    return new ArrowBuffers(true, new RootAllocator(16 * 1024 * 1024), 0, 8 * 1024 * 1024);
  }

  static ArrowQueryContext newContext(ArrowBuffers buffers, int workerId) {
    return buffers.newQueryContext("mailbox-test-" + workerId);
  }

  static ArrowBlock newBlock(ArrowQueryContext context) {
    return ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(rows(), SCHEMA), context);
  }

  static void assertRows(MseBlock.Data data) {
    assertEquals(data.getDataSchema(), SCHEMA);
    List<Object[]> expected = rows();
    List<Object[]> actual = data.asRowHeap().getRows();
    assertEquals(actual.size(), expected.size());
    for (int i = 0; i < actual.size(); i++) {
      assertEquals(actual.get(i), expected.get(i), "row " + i);
    }
  }

  static List<Object[]> rows() {
    return List.of(
        new Object[]{Integer.MIN_VALUE, "repeat", new ByteArray(new byte[]{0, -1})},
        new Object[]{0, "repeat", null},
        new Object[]{Integer.MAX_VALUE, "\u20AC", new ByteArray(new byte[0])},
        new Object[]{null, null, new ByteArray(new byte[]{3})});
  }

  private static List<ByteBuffer> payload(ArrowQueryContext context)
      throws Exception {
    ArrowBlock source = newBlock(context);
    try {
      List<ByteString> chunks = GrpcSendingMailbox.toByteStrings(source.getDataBlock(), 127);
      assertTrue(chunks.size() > 1);
      List<ByteBuffer> payload = new ArrayList<>(chunks.size());
      for (ByteString chunk : chunks) {
        assertTrue(chunk.size() <= 127);
        payload.add(chunk.asReadOnlyByteBuffer());
      }
      return payload;
    } finally {
      source.release();
    }
  }

  private static ReceivingMailbox newMailbox(int capacity) {
    ReceivingMailbox mailbox = new ReceivingMailbox("arrow-test", capacity);
    mailbox.registeredReader(() -> { });
    assertNotNull(mailbox.getStatMap());
    return mailbox;
  }
}
