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
package org.apache.pinot.query.mailbox.materialized;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockEquals;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Behavioral tests for the producer-local materialized mailbox store.
public class MaterializedMailboxStoreTest {
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final long REQUEST_ID = 17L;
  private static final int STAGE_ID = 3;
  private static final int WORKER_ID = 2;
  private static final long DEADLINE_DELAY_MS = 10_000L;

  @DataProvider(name = "partitionContents")
  public Object[][] partitionContents() {
    return new Object[][]{
        {0, 0},
        {2, 3}
    };
  }

  @Test(dataProvider = "partitionContents")
  public void testCommitPublishesExactDescriptorAndRoundTrips(int numBlocks, int rowsPerBlock)
      throws Exception {
    Path root = Files.createTempDirectory("materialized-mailbox-store");
    MaterializedMailboxStore store = new MaterializedMailboxStore(root, "producer", 1234);
    MaterializedMailboxKey key = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, numBlocks);
    List<DataBlock> expectedBlocks = createBlocks(numBlocks, rowsPerBlock);
    AtomicReference<Worker.MaterializedPartitionHandle> committed = new AtomicReference<>();

    try (MaterializedMailboxWriter writer = store.createWriter(key, committed::set)) {
      for (DataBlock block : expectedBlocks) {
        writer.write(block);
      }
      Worker.MaterializedPartitionHandle descriptor = writer.commit();

      assertEquals(committed.get(), descriptor);
      assertEquals(descriptor.getRequestId(), key.getRequestId());
      assertEquals(descriptor.getProducerStageId(), key.getProducerStageId());
      assertEquals(descriptor.getProducerWorkerId(), key.getProducerWorkerId());
      assertEquals(descriptor.getLogicalPartitionId(), key.getLogicalPartitionId());
      assertEquals(descriptor.getHost(), "producer");
      assertEquals(descriptor.getTransferPort(), 1234);
      assertEquals(descriptor.getRowCount(), (long) numBlocks * rowsPerBlock);
      assertEquals(descriptor.getByteCount(), Files.size(store.getCommittedPath(key)));

      List<DataBlock> actualBlocks = new ArrayList<>();
      try (MaterializedMailboxStore.RecordIterator records = store.read(key, deadlineMs())) {
        IteratorUtils.forEachRemaining(records, payload ->
            actualBlocks.add(DataBlockUtils.deserialize(List.of(ByteBuffer.wrap(payload)))));
      }
      assertDataBlocks(expectedBlocks, actualBlocks);
      assertFalse(Files.exists(store.getCommittedPath(key)));
    } finally {
      store.close();
    }
  }

  @Test
  public void testReadWaitsForAtomicCommit()
      throws Exception {
    Path root = Files.createTempDirectory("materialized-mailbox-wait");
    MaterializedMailboxStore store = new MaterializedMailboxStore(root, "producer", 1234);
    MaterializedMailboxKey key = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 0);
    DataBlock block = createBlocks(1, 1).get(0);
    CountDownLatch readerStarted = new CountDownLatch(1);
    CompletableFuture<List<byte[]>> read = CompletableFuture.supplyAsync(() -> {
      List<byte[]> records = new ArrayList<>();
      readerStarted.countDown();
      try (MaterializedMailboxStore.RecordIterator iterator = store.read(key, deadlineMs())) {
        iterator.forEachRemaining(records::add);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return records;
    });

    try {
      assertTrue(readerStarted.await(DEADLINE_DELAY_MS, TimeUnit.MILLISECONDS));
      awaitWaitingReader(store, key);
      assertFalse(read.isDone());
      try (MaterializedMailboxWriter writer = store.createWriter(key, descriptor -> {
      })) {
        writer.write(block);
        writer.commit();
      }
      assertEquals(read.get(DEADLINE_DELAY_MS, TimeUnit.MILLISECONDS).size(), 1);
    } finally {
      store.close();
    }
  }

  @DataProvider(name = "cleanupOperations")
  public Object[][] cleanupOperations() {
    return new Object[][]{
        {"request"},
        {"shutdown"}
    };
  }

  @Test(dataProvider = "cleanupOperations")
  public void testCleanupRemovesCommittedAndRejectsPendingReads(String cleanupOperation)
      throws Exception {
    Path root = Files.createTempDirectory("materialized-mailbox-cleanup");
    MaterializedMailboxStore store = new MaterializedMailboxStore(root, "producer", 1234);
    MaterializedMailboxKey committedKey = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 0);
    MaterializedMailboxKey pendingKey = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 1);
    try (MaterializedMailboxWriter writer = store.createWriter(committedKey, descriptor -> {
    })) {
      writer.commit();
    }
    MaterializedMailboxStore.RecordIterator committedRead = store.read(committedKey, deadlineMs());
    CompletableFuture<Void> pendingRead = CompletableFuture.runAsync(() -> {
      try (MaterializedMailboxStore.RecordIterator records = store.read(pendingKey, deadlineMs())) {
        records.forEachRemaining(payload -> {
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    awaitWaitingReader(store, pendingKey);

    if ("request".equals(cleanupOperation)) {
      store.cleanupRequest(REQUEST_ID);
    } else {
      store.close();
    }

    assertThrows(Exception.class, () -> pendingRead.get(DEADLINE_DELAY_MS, TimeUnit.MILLISECONDS));
    assertFalse(committedRead.hasNext());
    assertFalse(Files.exists(store.getCommittedPath(committedKey)));
    assertEquals(Files.exists(root), "request".equals(cleanupOperation));
    store.close();
  }

  @Test
  public void testCleanupRejectsRegisteredWriterAndReader()
      throws Exception {
    Path root = Files.createTempDirectory("materialized-mailbox-cleanup-race");
    MaterializedMailboxStore store = new MaterializedMailboxStore(root, "producer", 1234);
    MaterializedMailboxKey writerKey = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 0);
    MaterializedMailboxKey readerKey = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 1);
    MaterializedMailboxWriter writer = store.createWriter(writerKey, descriptor -> {
    });
    CompletableFuture<Void> pendingRead = CompletableFuture.runAsync(() -> {
      try (MaterializedMailboxStore.RecordIterator records = store.read(readerKey, deadlineMs())) {
        records.forEachRemaining(payload -> {
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    awaitWaitingReader(store, readerKey);

    store.cleanupRequest(REQUEST_ID);

    assertThrows(Exception.class, () -> pendingRead.get(DEADLINE_DELAY_MS, TimeUnit.MILLISECONDS));
    assertThrows(IOException.class, writer::commit);
    assertThrows(IOException.class, () -> store.createWriter(writerKey, descriptor -> {
    }));
    assertThrows(IOException.class, () -> store.read(readerKey, deadlineMs()));
    assertFalse(Files.exists(root.resolve(Long.toString(REQUEST_ID))));
    store.close();
  }

  @Test
  public void testOutputCloseFailureAbortsAndNotifiesReader()
      throws Exception {
    Path root = Files.createTempDirectory("materialized-mailbox-close-failure");
    MaterializedMailboxStore store = new MaterializedMailboxStore(root, "producer", 1234,
        path -> new FilterOutputStream(Files.newOutputStream(path)) {
          @Override
          public void close()
              throws IOException {
            super.close();
            throw new IOException("injected close failure");
          }
        });
    MaterializedMailboxKey key = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 0);
    AtomicReference<Worker.MaterializedPartitionHandle> committed = new AtomicReference<>();
    MaterializedMailboxWriter writer = store.createWriter(key, committed::set);
    CompletableFuture<Void> pendingRead = CompletableFuture.runAsync(() -> {
      try (MaterializedMailboxStore.RecordIterator records = store.read(key, deadlineMs())) {
        records.forEachRemaining(payload -> {
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    awaitWaitingReader(store, key);

    assertThrows(IOException.class, writer::commit);

    assertThrows(Exception.class, () -> pendingRead.get(DEADLINE_DELAY_MS, TimeUnit.MILLISECONDS));
    assertNull(committed.get());
    assertFalse(Files.exists(store.getTemporaryPath(key)));
    store.close();
  }

  @Test
  public void testCommitCallbackFailureAbortsAndNotifiesReader()
      throws Exception {
    Path root = Files.createTempDirectory("materialized-mailbox-commit-failure");
    MaterializedMailboxStore store = new MaterializedMailboxStore(root, "producer", 1234);
    MaterializedMailboxKey key = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 0);
    MaterializedMailboxWriter writer = store.createWriter(key, descriptor -> {
      throw new IllegalStateException("injected commit failure");
    });
    CompletableFuture<Void> pendingRead = CompletableFuture.runAsync(() -> {
      try (MaterializedMailboxStore.RecordIterator records = store.read(key, deadlineMs())) {
        records.forEachRemaining(payload -> {
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    awaitWaitingReader(store, key);

    assertThrows(IOException.class, writer::commit);

    assertThrows(Exception.class, () -> pendingRead.get(DEADLINE_DELAY_MS, TimeUnit.MILLISECONDS));
    assertFalse(Files.exists(store.getCommittedPath(key)));
    store.close();
  }

  @Test
  public void testExplicitReaderCloseDoesNotConsumePartition()
      throws Exception {
    Path root = Files.createTempDirectory("materialized-mailbox-reader-close");
    MaterializedMailboxStore store = new MaterializedMailboxStore(root, "producer", 1234);
    MaterializedMailboxKey key = new MaterializedMailboxKey(REQUEST_ID, STAGE_ID, WORKER_ID, 0);
    try (MaterializedMailboxWriter writer = store.createWriter(key, descriptor -> {
    })) {
      writer.write(createBlocks(1, 1).get(0));
      writer.commit();
    }

    MaterializedMailboxStore.RecordIterator abandoned = store.read(key, deadlineMs());
    abandoned.close();
    assertTrue(Files.exists(store.getCommittedPath(key)));

    try (MaterializedMailboxStore.RecordIterator records = store.read(key, deadlineMs())) {
      assertTrue(records.hasNext());
      records.next();
      assertFalse(records.hasNext());
    }
    assertFalse(Files.exists(store.getCommittedPath(key)));
    store.close();
  }

  @Test
  public void testCleanedRequestTrackingExpires()
      throws Exception {
    AtomicLong currentTimeMs = new AtomicLong();
    MaterializedMailboxStore store = new MaterializedMailboxStore(
        Files.createTempDirectory("materialized-mailbox-expiry"), "producer", 1234, currentTimeMs::get);
    try {
      store.cleanupRequest(REQUEST_ID);
      assertEquals(store.getCleanedRequestCount(), 1);

      currentTimeMs.addAndGet(MaterializedMailboxStore.CLEANED_REQUEST_RETENTION_MS + 1);
      assertEquals(store.getCleanedRequestCount(), 0);
    } finally {
      store.close();
    }
  }

  private static List<DataBlock> createBlocks(int numBlocks, int rowsPerBlock)
      throws Exception {
    List<DataBlock> blocks = new ArrayList<>(numBlocks);
    int value = 0;
    for (int blockId = 0; blockId < numBlocks; blockId++) {
      List<Object[]> rows = new ArrayList<>(rowsPerBlock);
      for (int rowId = 0; rowId < rowsPerBlock; rowId++) {
        rows.add(new Object[]{value++});
      }
      blocks.add(DataBlockBuilder.buildFromRows(rows, DATA_SCHEMA));
    }
    return blocks;
  }

  private static void assertDataBlocks(List<DataBlock> expected, List<DataBlock> actual) {
    assertEquals(actual.size(), expected.size());
    for (int i = 0; i < expected.size(); i++) {
      DataBlockEquals.checkSameContent(actual.get(i), expected.get(i), "Unexpected block at index " + i);
    }
  }

  private static long deadlineMs() {
    return System.currentTimeMillis() + DEADLINE_DELAY_MS;
  }

  private static void awaitWaitingReader(MaterializedMailboxStore store, MaterializedMailboxKey key)
      throws Exception {
    long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_DELAY_MS);
    while (store.getWaitingReaderCount(key) == 0) {
      if (System.nanoTime() >= deadlineNs) {
        throw new AssertionError("Reader did not start waiting for " + key);
      }
      Thread.onSpinWait();
    }
  }

  private static final class IteratorUtils {
    private IteratorUtils() {
    }

    private static <T> void forEachRemaining(java.util.Iterator<T> iterator, ConsumerWithException<T> consumer)
        throws Exception {
      while (iterator.hasNext()) {
        consumer.accept(iterator.next());
      }
    }
  }

  @FunctionalInterface
  private interface ConsumerWithException<T> {
    void accept(T value)
        throws Exception;
  }
}
