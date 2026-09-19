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
package org.apache.pinot.broker.stats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Runs the shared [StatsStore] contract against [SqliteStatsStore], plus the cases that only a
/// durable, file-backed store can have.
public class SqliteStatsStoreTest extends StatsStoreContractTest {

  private Path _tempDir;

  @Override
  protected StatsStore createStore()
      throws Exception {
    _tempDir = Files.createTempDirectory("sqlite-stats-test-");
    return new SqliteStatsStore(_tempDir);
  }

  @Override
  protected void cleanUp()
      throws Exception {
    deleteRecursively(_tempDir);
  }

  // ---------------------------------------------------------------------------
  // Persistence across reopen
  // ---------------------------------------------------------------------------

  @Test
  public void testPersistenceAcrossReopen()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, List.of(
        seg("seg1", 42L, 500L, 2000L, 0L, 100L, false)
    ));
    _store.close();
    _store = null;

    // Open a new store on the same directory
    SqliteStatsStore store2 = new SqliteStatsStore(_tempDir);
    store2.init();
    try {
      Map<String, Long> crcs = store2.getSegmentCrcs(TABLE_A);
      assertEquals(crcs.size(), 1);
      assertEquals(crcs.get("seg1").longValue(), 42L);

      TableStatistics stats = store2.getTableStats(TABLE_A);
      assertNotNull(stats);
      assertEquals(stats.getRowCount(), 500L);
    } finally {
      store2.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Corruption recovery
  // ---------------------------------------------------------------------------

  @Test
  public void testCorruptionRecovery()
      throws Exception {
    // Write some data, then close
    _store.upsertSegmentStats(TABLE_A, List.of(
        seg("seg1", 1L, 100L, 1000L, 0L, 10L, false)
    ));
    _store.close();
    _store = null;

    // Corrupt the DB file
    Path dbFile = _tempDir.resolve("broker-stats.sqlite");
    Files.write(dbFile, "this is not a valid sqlite database file!!!".getBytes());

    // Opening a new store should recover silently
    SqliteStatsStore store2 = new SqliteStatsStore(_tempDir);
    store2.init(); // must not throw
    try {
      // After recovery, the store is empty
      assertNull(store2.getTableStats(TABLE_A));
      Map<String, Long> crcs = store2.getSegmentCrcs(TABLE_A);
      assertTrue(crcs.isEmpty());
    } finally {
      store2.close();
    }
  }

  /// The schema carries no migration path on purpose: rows here are derived from ZooKeeper and can
  /// always be re-collected, so a store written by a different schema version is discarded rather
  /// than migrated -- the same recovery path a corrupt file takes.
  @Test
  public void testSchemaVersionMismatchRebuilds()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, List.of(seg("seg1", 42L, 500L, 2000L, 0L, 100L, false)));
    _store.close();
    _store = null;

    Path dbFile = _tempDir.resolve("broker-stats.sqlite");
    try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath());
        Statement st = conn.createStatement()) {
      st.execute("PRAGMA user_version=9999");
    }

    try (SqliteStatsStore reopened = new SqliteStatsStore(_tempDir)) {
      reopened.init();
      assertTrue(reopened.getSegmentCrcs(TABLE_A).isEmpty(), "A foreign schema version must be rebuilt empty");
      assertNull(reopened.getTableStats(TABLE_A));
    }
  }

  // ---------------------------------------------------------------------------
  // Read pool sizing and borrow timeout
  // ---------------------------------------------------------------------------

  /// The pool is a concurrency bound, not a per-caller connection, so a store configured with a
  /// single reader must still serve every caller correctly: readers are borrowed and returned,
  /// never leaked and never handed to two callers at once.
  @Test
  public void testSingleReaderPoolStillServesEveryRead()
      throws Exception {
    Path dir = Files.createTempDirectory("sqlite-stats-pool-");
    try (SqliteStatsStore store = new SqliteStatsStore(dir, 1, 5000)) {
      store.init();
      store.upsertSegmentStats(TABLE_A, List.of(seg("seg1", 1L, 500L, 2000L, 0L, 100L, false)));

      // More reads than pooled readers: each must borrow, use and return the one connection.
      for (int i = 0; i < 8; i++) {
        TableStatistics stats = store.getTableStats(TABLE_A);
        assertNotNull(stats, "Read " + i + " got no statistics");
        assertEquals(stats.getRowCount(), 500L, "Read " + i + " saw the wrong row count");
      }
    } finally {
      deleteRecursively(dir);
    }
  }

  /// An exhausted pool must give up after the configured timeout rather than block a planner
  /// thread indefinitely, and must recover once readers come back.
  @Test
  public void testBorrowTimesOutWhenPoolIsExhausted()
      throws Exception {
    Path dir = Files.createTempDirectory("sqlite-stats-timeout-");
    long timeoutMs = 100;
    try (SqliteStatsStore store = new SqliteStatsStore(dir, 2, timeoutMs)) {
      store.init();
      store.upsertSegmentStats(TABLE_A, List.of(seg("seg1", 1L, 500L, 2000L, 0L, 100L, false)));

      // Hold every reader. Reflection rather than blocked threads: an empty pool is exactly the
      // state under test, and reproducing it with concurrent reads would race against how fast
      // SQLite answers.
      Deque<Object> held = drainReadPool(store);
      assertEquals(held.size(), 2, "Expected the configured number of pooled readers");
      try {
        long startNanos = System.nanoTime();
        StatsStoreException e = expectThrows(StatsStoreException.class, () -> store.getTableStats(TABLE_A));
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

        assertTrue(e.getMessage().contains("Timed out"), "Unexpected message: " + e.getMessage());
        assertTrue(elapsedMs >= timeoutMs,
            "Gave up after " + elapsedMs + "ms, before the configured " + timeoutMs + "ms");
      } finally {
        returnToReadPool(store, held);
      }

      // With the readers back, reads succeed again.
      TableStatistics recovered = store.getTableStats(TABLE_A);
      assertNotNull(recovered);
      assertEquals(recovered.getRowCount(), 500L);
    } finally {
      deleteRecursively(dir);
    }
  }

  private static Deque<Object> drainReadPool(SqliteStatsStore store)
      throws Exception {
    BlockingQueue<Object> pool = readPool(store);
    Deque<Object> held = new ArrayDeque<>();
    Object reader;
    while ((reader = pool.poll()) != null) {
      held.add(reader);
    }
    return held;
  }

  private static void returnToReadPool(SqliteStatsStore store, Deque<Object> held)
      throws Exception {
    BlockingQueue<Object> pool = readPool(store);
    for (Object reader : held) {
      pool.offer(reader);
    }
  }

  @SuppressWarnings("unchecked")
  private static BlockingQueue<Object> readPool(SqliteStatsStore store)
      throws Exception {
    java.lang.reflect.Field field = SqliteStatsStore.class.getDeclaredField("_readPool");
    field.setAccessible(true);
    return (BlockingQueue<Object>) field.get(store);
  }

  // ---------------------------------------------------------------------------
  // Factory helpers
  // ---------------------------------------------------------------------------

  private static SegmentStatsRow seg(String name, long crc, long docs, long sizeBytes,
      long startMs, long endMs, boolean consuming) {
    return new SegmentStatsRow(name, crc, docs, sizeBytes, startMs, endMs, consuming);
  }

  /// Recursively deletes a directory tree.
  private static void deleteRecursively(Path dir)
      throws IOException {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (var stream = Files.walk(dir)) {
      stream.sorted(java.util.Comparator.reverseOrder())
          .forEach(p -> {
            try {
              Files.deleteIfExists(p);
            } catch (IOException e) {
              // ignore
            }
          });
    }
  }
}
