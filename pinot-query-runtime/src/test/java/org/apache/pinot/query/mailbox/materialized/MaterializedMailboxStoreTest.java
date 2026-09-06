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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockEquals;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests publication, framed data integrity, and terminal cleanup using real files.
/// Each test owns its store and readers; no state is shared across tests.
public class MaterializedMailboxStoreTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final MaterializedMailboxKey KEY = new MaterializedMailboxKey(17L, 3, 2, 0);
  private static final long TIMEOUT_MS = 10_000;

  @DataProvider
  public Object[][] partitionContents() {
    return new Object[][]{{0}, {2}};
  }

  @Test(dataProvider = "partitionContents")
  public void testPublishAndConsume(int blockCount)
      throws Exception {
    try (MaterializedMailboxStore store = store();
        MaterializedMailboxWriter writer = store.createWriter(KEY, ignored -> { })) {
      DataBlock block = block();
      for (int i = 0; i < blockCount; i++) {
        writer.write(block);
      }
      assertFalse(Files.exists(store.getCommittedPath(KEY)));
      Worker.MaterializedPartitionHandle handle = writer.commit();
      assertEquals(handle.getRequestId(), KEY.getRequestId());
      assertEquals(handle.getProducerStageId(), KEY.getProducerStageId());
      assertEquals(handle.getProducerWorkerId(), KEY.getProducerWorkerId());
      assertEquals(handle.getLogicalPartitionId(), KEY.getLogicalPartitionId());
      assertEquals(handle.getHost(), "producer");
      assertEquals(handle.getTransferPort(), 1234);
      assertEquals(handle.getRowCount(), (long) blockCount * block.getNumberOfRows());
      assertEquals(handle.getByteCount(), Files.size(store.getCommittedPath(KEY)));

      List<byte[]> records = readAll(store, KEY);
      assertEquals(records.size(), blockCount);
      for (byte[] record : records) {
        DataBlockEquals.checkSameContent(DataBlockUtils.deserialize(List.of(ByteBuffer.wrap(record))), block,
            "Materialized block");
      }
      assertFalse(Files.exists(store.getCommittedPath(KEY)));
    }
  }

  @Test
  public void testReaderWaitsForPublication()
      throws Exception {
    AtomicReference<Worker.MaterializedPartitionHandle> published = new AtomicReference<>();
    try (MaterializedMailboxStore store = store();
        MaterializedMailboxWriter writer = store.createWriter(KEY, published::set)) {
      writer.write(block());
      FutureTask<List<byte[]>> pending = pendingRead(store);
      Worker.MaterializedPartitionHandle handle = writer.commit();
      assertEquals(pending.get(TIMEOUT_MS, TimeUnit.MILLISECONDS).size(), 1);
      assertEquals(published.get(), handle);
    }
  }

  @DataProvider
  public Object[][] cleanupOperations() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "cleanupOperations")
  public void testCleanupClosesReadersAndRejectsPublication(boolean shutdown)
      throws Exception {
    MaterializedMailboxKey committedKey = new MaterializedMailboxKey(17L, 3, 2, 1);
    try (MaterializedMailboxStore store = store();
        MaterializedMailboxWriter writer = store.createWriter(KEY, ignored -> { })) {
      try (MaterializedMailboxWriter committed = store.createWriter(committedKey, ignored -> { })) {
        committed.write(block());
        committed.commit();
      }
      try (MaterializedMailboxStore.RecordIterator reader = store.read(committedKey, deadline())) {
        assertTrue(reader.hasNext()); // Also discard prefetched data when the query is cancelled.
        FutureTask<List<byte[]>> pending = pendingRead(store);
        if (shutdown) {
          store.close();
        } else {
          store.cleanupRequest(KEY.getRequestId());
        }
        assertThrows(ExecutionException.class, () -> pending.get(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertFalse(reader.hasNext());
        assertThrows(IOException.class, writer::commit);
        assertThrows(IOException.class, () -> store.createWriter(KEY, ignored -> { }));
        assertThrows(IOException.class, () -> store.read(KEY, deadline()));
        assertFalse(Files.exists(store.getCommittedPath(committedKey)));
      }
    }
  }

  @Test
  public void testFailedPublicationNotifiesReader()
      throws Exception {
    try (MaterializedMailboxStore store = store();
        MaterializedMailboxWriter writer = store.createWriter(KEY, ignored -> {
          throw new IllegalStateException("Publication failed");
        })) {
      FutureTask<List<byte[]>> pending = pendingRead(store);
      assertThrows(IOException.class, writer::commit);
      assertThrows(ExecutionException.class, () -> pending.get(TIMEOUT_MS, TimeUnit.MILLISECONDS));
      assertFalse(Files.exists(store.getCommittedPath(KEY)));
    }
  }

  @Test
  public void testAbandonedReadKeepsPartition()
      throws Exception {
    try (MaterializedMailboxStore store = store();
        MaterializedMailboxWriter writer = store.createWriter(KEY, ignored -> { })) {
      writer.write(block());
      writer.commit();
      try (MaterializedMailboxStore.RecordIterator reader = store.read(KEY, deadline())) {
        assertTrue(reader.hasNext());
      }
      assertTrue(Files.exists(store.getCommittedPath(KEY)));
      assertEquals(readAll(store, KEY).size(), 1);
      assertFalse(Files.exists(store.getCommittedPath(KEY)));
    }
  }

  @DataProvider
  public Object[][] corruptRecords() {
    return new Object[][]{
        {new byte[]{0, 0}}, // A truncated length header is not a clean EOF.
        {ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array()} // Validate size before allocating the payload.
    };
  }

  @Test(dataProvider = "corruptRecords")
  public void testCorruptRecordFailsInsteadOfReturningPartialResults(byte[] contents)
      throws Exception {
    try (MaterializedMailboxStore store = store();
        MaterializedMailboxWriter writer = store.createWriter(KEY, ignored -> { })) {
      writer.commit();
      Files.write(store.getCommittedPath(KEY), contents);
      assertThrows(UncheckedIOException.class, () -> readAll(store, KEY));
      assertTrue(Files.exists(store.getCommittedPath(KEY)));
    }
  }

  private static MaterializedMailboxStore store()
      throws IOException {
    return new MaterializedMailboxStore(Files.createTempDirectory("materialized-mailbox"), "producer", 1234);
  }

  private static DataBlock block()
      throws IOException {
    return DataBlockBuilder.buildFromRows(List.of(new Object[]{1}, new Object[]{2}), SCHEMA);
  }

  private static List<byte[]> readAll(MaterializedMailboxStore store, MaterializedMailboxKey key)
      throws IOException {
    try (MaterializedMailboxStore.RecordIterator reader = store.read(key, deadline())) {
      List<byte[]> records = new ArrayList<>();
      reader.forEachRemaining(records::add);
      return records;
    }
  }

  private static FutureTask<List<byte[]>> pendingRead(MaterializedMailboxStore store)
      throws Exception {
    FutureTask<List<byte[]>> read = new FutureTask<>(() -> readAll(store, KEY));
    Thread thread = new Thread(read);
    thread.start();
    TestUtils.waitForCondition(ignored -> thread.getState() == Thread.State.TIMED_WAITING || read.isDone(),
        TIMEOUT_MS, "Reader did not wait for publication");
    assertFalse(read.isDone());
    return read;
  }

  private static long deadline() {
    return System.currentTimeMillis() + TIMEOUT_MS;
  }
}
