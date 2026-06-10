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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for [SqliteStatsStore].
///
/// Each test method gets a fresh temporary directory and a new store instance.
public class SqliteStatsStoreTest {
  private static final String TABLE_A = "myTable_OFFLINE";
  private static final String TABLE_B = "otherTable_REALTIME";

  private Path _tempDir;
  private SqliteStatsStore _store;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("sqlite-stats-test-");
    _store = new SqliteStatsStore(_tempDir);
    _store.init();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    if (_store != null) {
      _store.close();
    }
    deleteRecursively(_tempDir);
  }

  // ---------------------------------------------------------------------------
  // Round-trip: upsert + getTableStats
  // ---------------------------------------------------------------------------

  @Test
  public void testRoundTripTableStats()
      throws Exception {
    List<SegmentStatsRow> rowsA = Arrays.asList(
        seg("seg1", 100L, 1000L, 5000L, 0L, 100L, false),
        seg("seg2", 200L, 2000L, 8000L, 100L, 200L, false)
    );
    List<SegmentStatsRow> rowsB = Arrays.asList(
        seg("seg3", 300L, 500L, 1000L, 0L, 50L, false)
    );

    _store.upsertSegmentStats(TABLE_A, rowsA);
    _store.upsertSegmentStats(TABLE_B, rowsB);

    TableStatistics statsA = _store.getTableStats(TABLE_A);
    assertNotNull(statsA);
    assertEquals(statsA.getRowCount(), 3000L);
    assertEquals(statsA.getTableSizeBytes(), 13000L);
    assertEquals(statsA.getRowCountConfidence(), StatConfidence.EXACT);
    assertEquals(statsA.getSizeConfidence(), StatConfidence.EXACT);

    TableStatistics statsB = _store.getTableStats(TABLE_B);
    assertNotNull(statsB);
    assertEquals(statsB.getRowCount(), 500L);
  }

  @Test
  public void testConsumingSegmentsExcludedFromTableStats()
      throws Exception {
    List<SegmentStatsRow> rows = Arrays.asList(
        seg("committed", 111L, 1000L, 5000L, 0L, 100L, false),
        seg("consuming", 222L, 999L, 1000L, 100L, 200L, true)
    );
    _store.upsertSegmentStats(TABLE_A, rows);

    TableStatistics stats = _store.getTableStats(TABLE_A);
    assertNotNull(stats);
    // Only the committed segment should be counted
    assertEquals(stats.getRowCount(), 1000L);
    assertEquals(stats.getTableSizeBytes(), 5000L);
  }

  @Test
  public void testGetTableStatsNullWhenEmpty()
      throws Exception {
    assertNull(_store.getTableStats(TABLE_A));
  }

  // ---------------------------------------------------------------------------
  // Upsert overwrite
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertOverwrite()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A,
        Collections.singletonList(seg("seg1", 100L, 1000L, 5000L, 0L, 100L, false)));

    // Overwrite with new values
    _store.upsertSegmentStats(TABLE_A,
        Collections.singletonList(seg("seg1", 999L, 2500L, 9000L, 0L, 100L, false)));

    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE_A);
    assertEquals(crcs.size(), 1);
    assertEquals(crcs.get("seg1").longValue(), 999L);

    TableStatistics stats = _store.getTableStats(TABLE_A);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 2500L);
  }

  // ---------------------------------------------------------------------------
  // removeSegments
  // ---------------------------------------------------------------------------

  @Test
  public void testRemoveSegments()
      throws Exception {
    List<SegmentStatsRow> rows = Arrays.asList(
        seg("seg1", 1L, 100L, 1000L, 0L, 10L, false),
        seg("seg2", 2L, 200L, 2000L, 10L, 20L, false)
    );
    _store.upsertSegmentStats(TABLE_A, rows);
    _store.upsertSegmentColumnStats(TABLE_A, Arrays.asList(
        col("seg1", "colA", 10L, "1", "9", true, 4.0, 0.0),
        col("seg2", "colA", 20L, "2", "8", true, 4.0, 0.0)
    ));

    _store.removeSegments(TABLE_A, Collections.singletonList("seg1"));

    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE_A);
    assertFalse(crcs.containsKey("seg1"));
    assertTrue(crcs.containsKey("seg2"));

    // Column stats for seg1 should also be removed
    ColumnStatistics colStats = _store.getColumnStats(TABLE_A, "colA");
    assertNotNull(colStats);
    // Only seg2's stats remain: ndv=20, min=2, max=8
    assertEquals(colStats.getNdv(), 20L);
  }

  // ---------------------------------------------------------------------------
  // Column stats aggregation
  // ---------------------------------------------------------------------------

  @Test
  public void testColumnStatsAggregation()
      throws Exception {
    // 3 segments with different stats; docs used for weighting
    List<SegmentStatsRow> segs = Arrays.asList(
        seg("s1", 1L, 100L, 1000L, 0L, 100L, false),
        seg("s2", 2L, 200L, 2000L, 100L, 200L, false),
        seg("s3", 3L, 300L, 3000L, 200L, 300L, false)
    );
    _store.upsertSegmentStats(TABLE_A, segs);

    // ndv: 10, 20, 30 → MAX = 30
    // min: "9", "10", "2" → numeric min = 2 (i.e. "2")
    // max: "9", "10", "20" → numeric max = 20 (i.e. "20")
    // minTrusted: true, false, true → AND = false
    // avgBytes: 4.0, 8.0, 2.0 (weighted by docs 100/200/300)
    //   weighted = (4*100 + 8*200 + 2*300) / 600 = (400+1600+600)/600 = 2600/600 ≈ 4.333...
    // nullFraction: 0.1, 0.2, 0.0 (weighted)
    //   weighted = (0.1*100 + 0.2*200 + 0.0*300) / 600 = (10+40+0)/600 = 50/600 ≈ 0.0833...
    List<SegmentColumnStatsRow> cols = Arrays.asList(
        col("s1", "colA", 10L, "9", "9", true, 4.0, 0.1),
        col("s2", "colA", 20L, "10", "10", false, 8.0, 0.2),
        col("s3", "colA", 30L, "2", "20", true, 2.0, 0.0)
    );
    _store.upsertSegmentColumnStats(TABLE_A, cols);

    ColumnStatistics cs = _store.getColumnStats(TABLE_A, "colA");
    assertNotNull(cs);

    assertEquals(cs.getNdv(), 30L);
    assertEquals(cs.getNdvConfidence(), StatConfidence.ESTIMATED);

    // Numeric comparison: min of "9","10","2" = 2 → Double(2.0)
    assertNotNull(cs.getMinValue());
    assertEquals(((Double) cs.getMinValue()).doubleValue(), 2.0, 0.001);

    // max of "9","10","20" = 20 → Double(20.0)
    assertNotNull(cs.getMaxValue());
    assertEquals(((Double) cs.getMaxValue()).doubleValue(), 20.0, 0.001);

    // minTrusted: s2 is false → overall false
    assertFalse(cs.isMinTrusted());

    // Weighted avgBytes ≈ 4.333
    assertEquals(cs.getAvgBytesPerValue(), 2600.0 / 600.0, 0.001);

    // Weighted nullFraction ≈ 0.0833
    assertEquals(cs.getNullFraction(), 50.0 / 600.0, 0.001);
  }

  @Test
  public void testNumericMinMaxOrdering()
      throws Exception {
    // "9" vs "10": lexically "9" > "10", numerically "9" < "10"
    _store.upsertSegmentStats(TABLE_A, Arrays.asList(
        seg("s1", 1L, 100L, 1000L, 0L, 100L, false),
        seg("s2", 2L, 100L, 1000L, 0L, 100L, false)
    ));
    _store.upsertSegmentColumnStats(TABLE_A, Arrays.asList(
        col("s1", "colA", 5L, "9", "9", true, 4.0, 0.0),
        col("s2", "colA", 5L, "10", "10", true, 4.0, 0.0)
    ));

    ColumnStatistics cs = _store.getColumnStats(TABLE_A, "colA");
    assertNotNull(cs);
    // Numeric min: 9 vs 10 → 9
    assertEquals(((Double) cs.getMinValue()).doubleValue(), 9.0, 0.001);
    // Numeric max: 9 vs 10 → 10
    assertEquals(((Double) cs.getMaxValue()).doubleValue(), 10.0, 0.001);
  }

  @Test
  public void testColumnStatsNullWhenNoRows()
      throws Exception {
    assertNull(_store.getColumnStats(TABLE_A, "colA"));
  }

  // ---------------------------------------------------------------------------
  // estimateRowsInTimeRange
  // ---------------------------------------------------------------------------

  @Test
  public void testEstimateRowsFullOverlap()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, Arrays.asList(
        seg("s1", 1L, 1000L, 0L, 0L, 100L, false),   // [0, 100)
        seg("s2", 2L, 2000L, 0L, 100L, 200L, false)  // [100, 200)
    ));

    // Query [0, 200) → both fully inside
    OptionalLong result = _store.estimateRowsInTimeRange(TABLE_A, 0L, 200L);
    assertTrue(result.isPresent());
    assertEquals(result.getAsLong(), 3000L);
  }

  @Test
  public void testEstimateRowsPartialOverlap()
      throws Exception {
    // Segment [0, 100), docs=1000
    _store.upsertSegmentStats(TABLE_A, Collections.singletonList(
        seg("s1", 1L, 1000L, 0L, 0L, 100L, false)
    ));

    // Query [25, 75) → 50% overlap → 500 rows
    OptionalLong result = _store.estimateRowsInTimeRange(TABLE_A, 25L, 75L);
    assertTrue(result.isPresent());
    assertEquals(result.getAsLong(), 500L);
  }

  @Test
  public void testEstimateRowsNoOverlap()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, Collections.singletonList(
        seg("s1", 1L, 1000L, 0L, 0L, 100L, false)
    ));

    // Query [200, 300) → no overlap
    OptionalLong result = _store.estimateRowsInTimeRange(TABLE_A, 200L, 300L);
    assertTrue(result.isPresent());
    assertEquals(result.getAsLong(), 0L);
  }

  @Test
  public void testEstimateRowsUnknownTimesSegment()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, Collections.singletonList(
        seg("s1", 1L, 1000L, 0L, -1L, -1L, false)
    ));

    // Unknown times → conservative, always include
    OptionalLong result = _store.estimateRowsInTimeRange(TABLE_A, 0L, 100L);
    assertTrue(result.isPresent());
    assertEquals(result.getAsLong(), 1000L);
  }

  @Test
  public void testEstimateRowsEmptyOptionalWhenNoRows()
      throws Exception {
    OptionalLong result = _store.estimateRowsInTimeRange(TABLE_A, 0L, 100L);
    assertFalse(result.isPresent());
  }

  // ---------------------------------------------------------------------------
  // Persistence across reopen
  // ---------------------------------------------------------------------------

  @Test
  public void testPersistenceAcrossReopen()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, Collections.singletonList(
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
    _store.upsertSegmentStats(TABLE_A, Collections.singletonList(
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

  // ---------------------------------------------------------------------------
  // purgeTable / purgeAll
  // ---------------------------------------------------------------------------

  @Test
  public void testPurgeTable()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, Collections.singletonList(
        seg("seg1", 1L, 100L, 1000L, 0L, 10L, false)
    ));
    _store.upsertSegmentStats(TABLE_B, Collections.singletonList(
        seg("seg2", 2L, 200L, 2000L, 0L, 10L, false)
    ));
    _store.upsertSegmentColumnStats(TABLE_A, Collections.singletonList(
        col("seg1", "colA", 5L, "1", "9", true, 4.0, 0.0)
    ));

    _store.purgeTable(TABLE_A);

    assertNull(_store.getTableStats(TABLE_A));
    assertNull(_store.getColumnStats(TABLE_A, "colA"));
    // TABLE_B should still be there
    assertNotNull(_store.getTableStats(TABLE_B));
  }

  @Test
  public void testPurgeAll()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, Collections.singletonList(
        seg("seg1", 1L, 100L, 1000L, 0L, 10L, false)
    ));
    _store.upsertSegmentStats(TABLE_B, Collections.singletonList(
        seg("seg2", 2L, 200L, 2000L, 0L, 10L, false)
    ));

    _store.purgeAll();

    assertNull(_store.getTableStats(TABLE_A));
    assertNull(_store.getTableStats(TABLE_B));
  }

  // ---------------------------------------------------------------------------
  // getSegmentCrcs
  // ---------------------------------------------------------------------------

  @Test
  public void testGetSegmentCrcsEmptyWhenNoData()
      throws Exception {
    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE_A);
    assertNotNull(crcs);
    assertTrue(crcs.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // Factory helpers
  // ---------------------------------------------------------------------------

  private static SegmentStatsRow seg(String name, long crc, long docs, long sizeBytes,
      long startMs, long endMs, boolean consuming) {
    return new SegmentStatsRow(name, crc, docs, sizeBytes, startMs, endMs, consuming);
  }

  private static SegmentColumnStatsRow col(String segName, String colName, long ndv,
      String minVal, String maxVal, boolean minTrusted, double avgBytes, double nullFrac) {
    return new SegmentColumnStatsRow(segName, colName, ndv, minVal, maxVal, minTrusted, avgBytes,
        nullFrac);
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
