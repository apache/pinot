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

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.ColumnValueType;
import org.apache.pinot.query.planner.spi.stats.SegmentColumnStatsRow;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.StatsAggregations;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Behavior every [StatsStore] implementation must share.
///
/// The optimizer must not change with the configured store, so these cases are defined once here
/// and run against each implementation by a subclass. Anything specific to one implementation
/// (durability, corruption recovery) belongs in that subclass instead.
public abstract class StatsStoreContractTest {
  protected static final String TABLE_A = "myTable_OFFLINE";
  protected static final String TABLE_B = "otherTable_REALTIME";

  protected StatsStore _store;

  /// Creates a fresh, un-initialized store for one test method.
  protected abstract StatsStore createStore()
      throws Exception;

  /// Releases anything the concrete store needed beyond [StatsStore#close()].
  protected void cleanUp()
      throws Exception {
  }

  @BeforeMethod
  public void setUp()
      throws Exception {
    _store = createStore();
    _store.init();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    if (_store != null) {
      _store.close();
    }
    cleanUp();
  }

  // ---------------------------------------------------------------------------
  // Round-trip: upsert + getTableStats
  // ---------------------------------------------------------------------------

  @Test
  public void testRoundTripTableStats()
      throws Exception {
    List<SegmentStatsRow> rowsA = List.of(
        seg("seg1", 100L, 1000L, 5000L, 0L, 100L, false),
        seg("seg2", 200L, 2000L, 8000L, 100L, 200L, false)
    );
    List<SegmentStatsRow> rowsB = List.of(
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
    List<SegmentStatsRow> rows = List.of(
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
        List.of(seg("seg1", 100L, 1000L, 5000L, 0L, 100L, false)));

    // Overwrite with new values
    _store.upsertSegmentStats(TABLE_A,
        List.of(seg("seg1", 999L, 2500L, 9000L, 0L, 100L, false)));

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
    List<SegmentStatsRow> rows = List.of(
        seg("seg1", 1L, 100L, 1000L, 0L, 10L, false),
        seg("seg2", 2L, 200L, 2000L, 10L, 20L, false)
    );
    _store.upsertSegmentStats(TABLE_A, rows);
    _store.upsertSegmentColumnStats(TABLE_A, List.of(
        col("seg1", "colA", 10L, "1", "9", true, 4.0, 0.0),
        col("seg2", "colA", 20L, "2", "8", true, 4.0, 0.0)
    ));

    _store.removeSegments(TABLE_A, List.of("seg1"));

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
    List<SegmentStatsRow> segs = List.of(
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
    List<SegmentColumnStatsRow> cols = List.of(
        col("s1", "colA", 10L, "9", "9", true, 4.0, 0.1),
        col("s2", "colA", 20L, "10", "10", false, 8.0, 0.2),
        col("s3", "colA", 30L, "2", "20", true, 2.0, 0.0)
    );
    _store.upsertSegmentColumnStats(TABLE_A, cols);

    ColumnStatistics cs = _store.getColumnStats(TABLE_A, "colA");
    assertNotNull(cs);

    assertEquals(cs.getNdv(), 30L);
    assertEquals(cs.getNdvConfidence(), StatConfidence.ESTIMATED);

    // Ordered as LONG (the recorded type), and returned as the Java type that ordering implies:
    // min of "9","10","2" is 2, max of "9","10","20" is 20.
    assertEquals(cs.getMinValue(), 2L);
    assertEquals(cs.getMaxValue(), 20L);

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
    _store.upsertSegmentStats(TABLE_A, List.of(
        seg("s1", 1L, 100L, 1000L, 0L, 100L, false),
        seg("s2", 2L, 100L, 1000L, 0L, 100L, false)
    ));
    _store.upsertSegmentColumnStats(TABLE_A, List.of(
        col("s1", "colA", 5L, "9", "9", true, 4.0, 0.0),
        col("s2", "colA", 5L, "10", "10", true, 4.0, 0.0)
    ));

    ColumnStatistics cs = _store.getColumnStats(TABLE_A, "colA");
    assertNotNull(cs);
    // "9" vs "10" orders numerically under LONG, where text order would put "10" first.
    assertEquals(cs.getMinValue(), 9L);
    assertEquals(cs.getMaxValue(), 10L);
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
    _store.upsertSegmentStats(TABLE_A, List.of(
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
    _store.upsertSegmentStats(TABLE_A, List.of(
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
    _store.upsertSegmentStats(TABLE_A, List.of(
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
    _store.upsertSegmentStats(TABLE_A, List.of(
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

  @Test
  public void testEstimateRowsEmptyOptionalWhenOnlyConsumingSegments()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, List.of(
        seg("s1", 1L, 1000L, 0L, 0L, 100L, true)
    ));

    // Consuming segments are excluded from time-range estimates; with no committed segments the
    // result must be empty ("no stats"), never of(0) ("provably zero rows in range").
    OptionalLong result = _store.estimateRowsInTimeRange(TABLE_A, 0L, 100L);
    assertFalse(result.isPresent());
  }

  @Test
  public void testEstimateRowsBoundaryAdjacency()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, List.of(
        seg("s1", 1L, 1000L, 0L, 100L, 200L, false)   // [100, 200)
    ));

    // Segment ending exactly at the range start → no overlap (half-open semantics)
    OptionalLong before = _store.estimateRowsInTimeRange(TABLE_A, 200L, 300L);
    assertTrue(before.isPresent());
    assertEquals(before.getAsLong(), 0L);

    // Segment starting exactly at the range end → no overlap
    OptionalLong after = _store.estimateRowsInTimeRange(TABLE_A, 0L, 100L);
    assertTrue(after.isPresent());
    assertEquals(after.getAsLong(), 0L);

    // Touching on both sides at once: range exactly equal to the segment → full overlap
    OptionalLong exact = _store.estimateRowsInTimeRange(TABLE_A, 100L, 200L);
    assertTrue(exact.isPresent());
    assertEquals(exact.getAsLong(), 1000L);
  }

  // ---------------------------------------------------------------------------
  // purgeTable / purgeAll
  // ---------------------------------------------------------------------------

  @Test
  public void testPurgeTable()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, List.of(
        seg("seg1", 1L, 100L, 1000L, 0L, 10L, false)
    ));
    _store.upsertSegmentStats(TABLE_B, List.of(
        seg("seg2", 2L, 200L, 2000L, 0L, 10L, false)
    ));
    _store.upsertSegmentColumnStats(TABLE_A, List.of(
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
    _store.upsertSegmentStats(TABLE_A, List.of(
        seg("seg1", 1L, 100L, 1000L, 0L, 10L, false)
    ));
    _store.upsertSegmentStats(TABLE_B, List.of(
        seg("seg2", 2L, 200L, 2000L, 0L, 10L, false)
    ));

    _store.purgeAll();

    assertNull(_store.getTableStats(TABLE_A));
    assertNull(_store.getTableStats(TABLE_B));
  }

  // ---------------------------------------------------------------------------
  // Consuming segments
  // ---------------------------------------------------------------------------

  /// Drives whether realtime row counts are trusted (see `LogicalTableStatsResolver`), so both
  /// stores must agree on it.
  @Test
  public void testHasConsumingSegments()
      throws Exception {
    assertFalse(_store.hasConsumingSegments(TABLE_A), "Unknown table has no consuming segments");

    _store.upsertSegmentStats(TABLE_A, List.of(seg("committed", 1L, 100L, 1000L, 0L, 10L, false)));
    assertFalse(_store.hasConsumingSegments(TABLE_A));

    _store.upsertSegmentStats(TABLE_A, List.of(seg("consuming", 2L, 50L, 500L, 10L, 20L, true)));
    assertTrue(_store.hasConsumingSegments(TABLE_A));

    _store.removeSegments(TABLE_A, List.of("consuming"));
    assertFalse(_store.hasConsumingSegments(TABLE_A), "Removing the consuming segment clears the flag");
  }

  /// A table with nothing but consuming segments has no trustworthy aggregate: reads must report
  /// "no statistics" rather than a zero, and the segment must still be reconcilable by crc.
  @Test
  public void testOnlyConsumingSegmentsYieldNoTableOrColumnStats()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, List.of(seg("consuming", 7L, 999L, 500L, 0L, 10L, true)));
    _store.upsertSegmentColumnStats(TABLE_A,
        List.of(col("consuming", "colA", 5L, "1", "9", true, 4.0, 0.0)));

    assertNull(_store.getTableStats(TABLE_A), "Consuming-only table has no committed rows to aggregate");
    assertNull(_store.getColumnStats(TABLE_A, "colA"), "A column row on a consuming segment does not count");
    assertTrue(_store.getSegmentCrcs(TABLE_A).containsKey("consuming"), "crcs still cover consuming segments");
  }

  /// A column row whose segment row is gone contributes nothing: it supplies the doc count used for
  /// weighting, so counting it would weight by a stale or absent value.
  @Test
  public void testColumnStatsIgnoreRowsWithoutASegment()
      throws Exception {
    _store.upsertSegmentColumnStats(TABLE_A,
        List.of(col("ghost", "colA", 5L, "1", "9", true, 4.0, 0.0)));

    assertNull(_store.getColumnStats(TABLE_A, "colA"));
  }

  // ---------------------------------------------------------------------------
  // Shared aggregation semantics
  // ---------------------------------------------------------------------------

  /// Pins the ordering rules both stores rely on. The type has to be recorded with the values:
  /// guessing "numeric if it parses" orders a STRING column numerically and rounds a LONG past
  /// 2^53 — inwards, which would exclude rows that exist.
  @Test
  public void testValueOrderingIsDrivenByTheRecordedType() {
    assertTrue(ColumnValueType.LONG.compare("9", "10") < 0, "LONG orders numerically");
    assertTrue(ColumnValueType.STRING.compare("9", "10") > 0, "STRING orders lexically");

    // 2^53 + 1 and 2^53 + 3 are indistinguishable as doubles; as LONG they are not.
    assertTrue(ColumnValueType.LONG.compare("9007199254740993", "9007199254740995") < 0,
        "LONG must not lose precision past 2^53");
    assertEquals(ColumnValueType.LONG.toComparable("9007199254740993"), 9007199254740993L);
    assertEquals(ColumnValueType.BIG_DECIMAL.toComparable("1.5"), new java.math.BigDecimal("1.5"));
    assertEquals(ColumnValueType.STRING.toComparable("abc"), "abc");
    assertNull(ColumnValueType.STRING.toComparable(null));

    assertEquals(StatsAggregations.minOf(null, "5", ColumnValueType.LONG), "5", "null means unknown");
    assertEquals(StatsAggregations.maxOf("5", null, ColumnValueType.LONG), "5");
    assertNull(StatsAggregations.minOf(null, null, ColumnValueType.LONG));
  }

  /// Without a recorded type there is no defined ordering, so the bounds must be reported
  /// untrusted rather than ordered on a guess — a consumer may prune on trusted bounds.
  @Test
  public void testUnknownValueTypeYieldsUntrustedBounds()
      throws Exception {
    _store.upsertSegmentStats(TABLE_A, List.of(seg("s1", 1L, 100L, 1000L, 0L, 10L, false)));
    _store.upsertSegmentColumnStats(TABLE_A,
        List.of(new SegmentColumnStatsRow("s1", "colA", 5L, "9", "10", true, 4.0, 0.0, null)));

    ColumnStatistics stats = _store.getColumnStats(TABLE_A, "colA");
    assertNotNull(stats);
    assertFalse(stats.isMinTrusted(), "Bounds with no recorded ordering must not be trusted");
  }

  /// Pins the per-segment overlap rules, including the cases a query never makes obvious.
  @Test
  public void testOverlapSemantics() {
    assertEquals(StatsAggregations.overlapRows(100L, -1L, -1L, 0L, 10L), 100L, "Unknown times count in full");
    assertEquals(StatsAggregations.overlapRows(100L, 10L, 20L, 0L, 100L), 100L, "Full containment");
    assertEquals(StatsAggregations.overlapRows(100L, 100L, 200L, 0L, 100L), 0L, "Half-open upper bound excludes");
    assertEquals(StatsAggregations.overlapRows(1000L, 0L, 100L, 25L, 75L), 500L, "Partial overlap interpolates");
    assertEquals(StatsAggregations.overlapRows(100L, 50L, 50L, 0L, 100L), 100L, "Zero-length segment in range");
    assertEquals(StatsAggregations.overlapRows(100L, 50L, 50L, 60L, 100L), 0L, "Zero-length segment out of range");
  }

  // ---------------------------------------------------------------------------
  // Enumerating stored tables
  // ---------------------------------------------------------------------------

  /// getTables() feeds a destructive purge, so both stores must agree on what "holds statistics"
  /// means -- including that a table whose segments are all gone is no longer reported.
  @Test
  public void testGetTablesReflectsStoredRows()
      throws Exception {
    assertTrue(_store.getTables().isEmpty(), "A fresh store holds nothing");

    _store.upsertSegmentStats(TABLE_A, List.of(seg("s1", 1L, 100L, 1000L, 0L, 10L, false)));
    _store.upsertSegmentStats(TABLE_B, List.of(seg("s2", 2L, 200L, 2000L, 0L, 10L, false)));
    assertEquals(_store.getTables(), Set.of(TABLE_A, TABLE_B));

    _store.removeSegments(TABLE_A, List.of("s1"));
    assertEquals(_store.getTables(), Set.of(TABLE_B), "A table with no rows left is not reported");

    _store.purgeTable(TABLE_B);
    assertTrue(_store.getTables().isEmpty());
  }

  // ---------------------------------------------------------------------------
  // Closed store
  // ---------------------------------------------------------------------------

  @Test
  public void testReadsAfterCloseAreRejected()
      throws Exception {
    _store.close();
    assertThrows(StatsStoreException.class, () -> _store.getTableStats(TABLE_A));
    assertThrows(StatsStoreException.class, () -> _store.getTables());
    _store = null;  // already closed; tearDown must not close it twice
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

  /// Existing cases use numeric min/max, so they record LONG; the untyped case is exercised
  /// explicitly by testUnknownValueTypeYieldsUntrustedBounds.
  private static SegmentColumnStatsRow col(String segName, String colName, long ndv,
      String minVal, String maxVal, boolean minTrusted, double avgBytes, double nullFrac) {
    return new SegmentColumnStatsRow(segName, colName, ndv, minVal, maxVal, minTrusted, avgBytes,
        nullFrac, ColumnValueType.LONG);
  }
}
