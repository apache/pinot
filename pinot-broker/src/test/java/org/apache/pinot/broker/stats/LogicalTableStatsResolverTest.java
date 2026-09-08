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
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for [LogicalTableStatsResolver].
///
/// Each test method gets a fresh temporary directory and a new [SqliteStatsStore] and
/// [LogicalTableStatsResolver]. Table configs and time boundaries are injected as simple
/// lambdas.
public class LogicalTableStatsResolverTest {

  private static final String RAW_TABLE = "myTable";
  private static final String OFFLINE_TABLE = "myTable_OFFLINE";
  private static final String REALTIME_TABLE = "myTable_REALTIME";

  private Path _tempDir;
  private SqliteStatsStore _store;
  private LogicalTableStatsResolver _resolver;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("logical-stats-resolver-test-");
    _store = new SqliteStatsStore(_tempDir);
    _store.init();
    _resolver = new LogicalTableStatsResolver(_store);
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
  // Offline-only raw lookup
  // ---------------------------------------------------------------------------

  @Test
  public void testRawLookupOfflineOnly()
      throws Exception {
    // Only offline segments exist
    insertSegments(OFFLINE_TABLE, List.of(
        row("seg1", 1L, 500L, 4096L, 0L, 100L, false),
        row("seg2", 2L, 300L, 2048L, 100L, 200L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats, "Stats must be present for offline-only raw lookup");
    assertEquals(stats.getRowCount(), 800L, "Row count should be sum of offline segments");
    assertEquals(stats.getTableSizeBytes(), 6144L);
    // Only offline → no ESTIMATED downgrade from hybrid merge
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  // ---------------------------------------------------------------------------
  // Realtime-only raw lookup
  // ---------------------------------------------------------------------------

  @Test
  public void testRawLookupRealtimeOnly()
      throws Exception {
    // Only realtime (committed) segments exist, no consuming
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 10L, 200L, 1024L, 50L, 150L, false),
        row("rtSeg2", 11L, 100L, 512L, 150L, 250L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats, "Stats must be present for realtime-only raw lookup");
    assertEquals(stats.getRowCount(), 300L);
    // No upsert, no consuming → EXACT
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  // ---------------------------------------------------------------------------
  // Suffixed physical lookups
  // ---------------------------------------------------------------------------

  @Test
  public void testSuffixedOfflineLookup()
      throws Exception {
    insertSegments(OFFLINE_TABLE, List.of(
        row("seg1", 1L, 1000L, 8192L, 0L, 200L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(OFFLINE_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 1000L);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  @Test
  public void testSuffixedRealtimeLookupNoAdjustment()
      throws Exception {
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false)
    ));
    // No table config provider → no upsert/dedup detection; no consuming
    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 400L);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  // ---------------------------------------------------------------------------
  // Hybrid with boundary: no double-count
  // ---------------------------------------------------------------------------

  @Test
  public void testHybridWithBoundary()
      throws Exception {
    // Offline segments: [0,100ms) and [100,200ms)
    insertSegments(OFFLINE_TABLE, List.of(
        row("offSeg1", 1L, 100L, 1024L, 0L, 100L, false),
        row("offSeg2", 2L, 200L, 2048L, 100L, 200L, false)
    ));
    // Realtime segments: [150ms, 300ms)
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 10L, 150L, 1024L, 150L, 300L, false)
    ));

    // Boundary at 150ms:
    //   offline contribution: rows whose segments overlap [MIN, 150) = seg1 (full 100) + seg2 (partial 100→150 = 50%)
    //   realtime contribution: rows whose segments overlap [150, MAX) = rtSeg1 (full 150)
    long boundaryMs = 150L;
    _resolver.setTimeBoundaryMsProvider(rawName -> RAW_TABLE.equals(rawName) ? boundaryMs : null);

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats);

    // Plain sum would be 100+200+150 = 450; with boundary it should be less (no double-count)
    // offline estimate for [MIN, 150): seg1 full=100, seg2 partial (100→150 out of 100→200 = 50%) = 100
    //   → offline = 200
    // realtime estimate for [150, MAX): rtSeg1 full = 150
    //   → merged = 200 + 150 = 350
    assertEquals(stats.getRowCount(), 350L,
        "Hybrid row count should avoid double-counting via boundary split");
    assertEquals(stats.getRowCountConfidence(), StatConfidence.ESTIMATED);

    // Plain sum of sizes is acceptable
    assertEquals(stats.getTableSizeBytes(), 1024L + 2048L + 1024L);
    assertEquals(stats.getSizeConfidence(), StatConfidence.ESTIMATED);
  }

  // ---------------------------------------------------------------------------
  // Hybrid without boundary: plain sum, ESTIMATED
  // ---------------------------------------------------------------------------

  @Test
  public void testHybridWithoutBoundary()
      throws Exception {
    insertSegments(OFFLINE_TABLE, List.of(
        row("offSeg1", 1L, 100L, 1024L, 0L, 100L, false)
    ));
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 10L, 200L, 2048L, 50L, 200L, false)
    ));
    // No boundary provider → plain sum
    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 300L, "Without boundary, plain sum should be used");
    assertEquals(stats.getRowCountConfidence(), StatConfidence.ESTIMATED,
        "Without boundary, confidence must be ESTIMATED");
  }

  // ---------------------------------------------------------------------------
  // Upsert realtime: LOW confidence (raw and suffixed)
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertRealtimeConfidenceLowSuffixed()
      throws Exception {
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 1L, 500L, 4096L, 0L, 100L, false)
    ));

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig upsertTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setUpsertConfig(upsertConfig).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? upsertTable : null);

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "Upsert realtime table must report LOW confidence");
    assertEquals(stats.getRowCount(), 500L);
  }

  @Test
  public void testUpsertRealtimeConfidenceLowRaw()
      throws Exception {
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 1L, 500L, 4096L, 0L, 100L, false)
    ));
    // No offline table stats

    TableConfig upsertTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setUpsertConfig(upsertTable(UpsertConfig.Mode.FULL)).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? upsertTable : null);

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "Upsert realtime raw lookup must report LOW confidence");
  }

  // ---------------------------------------------------------------------------
  // Dedup realtime: LOW confidence
  // ---------------------------------------------------------------------------

  @Test
  public void testDedupRealtimeConfidenceLow()
      throws Exception {
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 1L, 300L, 2048L, 0L, 100L, false)
    ));

    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setDedupEnabled(true);
    TableConfig dedupTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setDedupConfig(dedupConfig).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? dedupTable : null);

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "Dedup realtime table must report LOW confidence");
  }

  // ---------------------------------------------------------------------------
  // Consuming present: ESTIMATED confidence
  // ---------------------------------------------------------------------------

  @Test
  public void testConsumingSegmentDowngradesConfidence()
      throws Exception {
    // One committed + one consuming segment
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false),
        row("rtSeg2_consuming", 2L, -1L, -1L, 100L, -1L, true)
    ));

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    // Row count only includes committed rows (consuming excluded from sum by store)
    assertEquals(stats.getRowCount(), 400L);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.ESTIMATED,
        "Consuming segment present must downgrade confidence to ESTIMATED");
  }

  @Test
  public void testNoConsumingSegmentConfidenceExact()
      throws Exception {
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT,
        "No consuming segment: confidence should remain EXACT");
  }

  // ---------------------------------------------------------------------------
  // LOW wins over ESTIMATED (upsert + consuming)
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertPlusConsumingIsLow()
      throws Exception {
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false),
        row("rtSeg2_consuming", 2L, -1L, -1L, 100L, -1L, true)
    ));
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig upsertTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setUpsertConfig(upsertConfig).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? upsertTable : null);

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "LOW must win over ESTIMATED when upsert + consuming both apply");
  }

  // ---------------------------------------------------------------------------
  // estimateRowsInTimeRange: hybrid split at boundary
  // ---------------------------------------------------------------------------

  @Test
  public void testEstimateRowsInTimeRangeHybridSplit()
      throws Exception {
    // Offline: two segments, each 1000 rows, [0,100) and [100,200)
    insertSegments(OFFLINE_TABLE, List.of(
        row("offSeg1", 1L, 1000L, 0L, 0L, 100L, false),
        row("offSeg2", 2L, 1000L, 0L, 100L, 200L, false)
    ));
    // Realtime: one segment, 500 rows, [150,300)
    insertSegments(REALTIME_TABLE, List.of(
        row("rtSeg1", 10L, 500L, 0L, 150L, 300L, false)
    ));

    long boundaryMs = 150L;
    _resolver.setTimeBoundaryMsProvider(rawName -> RAW_TABLE.equals(rawName) ? boundaryMs : null);

    // Query range [0, 300) on hybrid
    // offline contribution for [0, min(300, 150)) = [0, 150):
    //   offSeg1 full [0,100) → 1000 rows
    //   offSeg2 partial [100,150) out of [100,200) → 50% of 1000 = 500 rows
    //   total offline = 1500
    // realtime contribution for [max(0,150), 300) = [150, 300):
    //   rtSeg1 full [150,300) → 500 rows
    // total = 2000
    OptionalLong estimate = _resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, 300L);
    assertTrue(estimate.isPresent());
    assertEquals(estimate.getAsLong(), 2000L,
        "Hybrid estimateRowsInTimeRange should split at boundary");
  }

  @Test
  public void testEstimateRowsInTimeRangePhysicalTable()
      throws Exception {
    insertSegments(OFFLINE_TABLE, List.of(
        row("seg1", 1L, 1000L, 0L, 0L, 100L, false),
        row("seg2", 2L, 1000L, 0L, 100L, 200L, false)
    ));

    // Query [50, 150) on offline: seg1 partial + seg2 partial
    // seg1 [0,100) ∩ [50,150) = [50,100), fraction = 50/100 = 0.5, rows = 500
    // seg2 [100,200) ∩ [50,150) = [100,150), fraction = 50/100 = 0.5, rows = 500
    OptionalLong estimate = _resolver.estimateRowsInTimeRange(OFFLINE_TABLE, 50L, 150L);
    assertTrue(estimate.isPresent());
    assertEquals(estimate.getAsLong(), 1000L);
  }

  // ---------------------------------------------------------------------------
  // Null returns when no data
  // ---------------------------------------------------------------------------

  @Test
  public void testGetTableStatsReturnsNullWhenNoData() {
    assertNull(_resolver.getTableStats(RAW_TABLE));
    assertNull(_resolver.getTableStats(OFFLINE_TABLE));
    assertNull(_resolver.getTableStats(REALTIME_TABLE));
  }

  @Test
  public void testEstimateRowsInTimeRangeEmptyWhenNoData() {
    assertFalse(_resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, Long.MAX_VALUE).isPresent());
    assertFalse(_resolver.estimateRowsInTimeRange(OFFLINE_TABLE, 0L, Long.MAX_VALUE).isPresent());
  }

  // ---------------------------------------------------------------------------
  // weakest() helper
  // ---------------------------------------------------------------------------

  @Test
  public void testWeakestHelper() {
    assertEquals(LogicalTableStatsResolver.weakest(StatConfidence.EXACT, StatConfidence.EXACT),
        StatConfidence.EXACT);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.EXACT, StatConfidence.ESTIMATED),
        StatConfidence.ESTIMATED);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.ESTIMATED, StatConfidence.LOW),
        StatConfidence.LOW);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.LOW, StatConfidence.UNKNOWN),
        StatConfidence.UNKNOWN);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.LOW, StatConfidence.ESTIMATED),
        StatConfidence.LOW);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void insertSegments(String tableNameWithType, List<SegmentStatsRow> rows)
      throws StatsStoreException {
    _store.upsertSegmentStats(tableNameWithType, rows);
  }

  private static SegmentStatsRow row(String segName, long crc, long totalDocs, long sizeBytes,
      long startMs, long endMs, boolean consuming) {
    return new SegmentStatsRow(segName, crc, totalDocs, sizeBytes, startMs, endMs, consuming);
  }

  private static UpsertConfig upsertTable(UpsertConfig.Mode mode) {
    return new UpsertConfig(mode);
  }

  private static void deleteRecursively(Path dir)
      throws IOException {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (var stream = Files.walk(dir)) {
      stream.sorted(Comparator.reverseOrder())
          .forEach(p -> {
            try {
              Files.deleteIfExists(p);
            } catch (IOException e) {
              // ignore
            }
          });
    }
  }

  // ---------------------------------------------------------------------------
  // A missing side of a hybrid table must read as unknown, never as zero rows
  // ---------------------------------------------------------------------------

  /// The realtime side of a hybrid table contributes nothing to a range that ends at the boundary,
  /// but that structural zero must not stand in for the offline side having no statistics at all.
  /// Reporting a confident 0 here is the worst possible input to the planner: it is not "no
  /// estimate", it is "this table is empty".
  @Test
  public void testRangeBelowBoundaryIsUnknownWhenOfflineHasNoStats()
      throws Exception {
    insertSegments(REALTIME_TABLE, List.of(row("rtSeg1", 10L, 150L, 1024L, 150L, 300L, false)));
    _resolver.setTimeBoundaryMsProvider(rawName -> RAW_TABLE.equals(rawName) ? 1000L : null);

    assertFalse(_resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, 500L).isPresent(),
        "Offline has no statistics, so the estimate is unknown -- not zero");
  }

  /// The mirror image: a range entirely above the boundary with no realtime statistics.
  @Test
  public void testRangeAboveBoundaryIsUnknownWhenRealtimeHasNoStats()
      throws Exception {
    insertSegments(OFFLINE_TABLE, List.of(row("seg1", 1L, 100L, 1024L, 0L, 100L, false)));
    _resolver.setTimeBoundaryMsProvider(rawName -> RAW_TABLE.equals(rawName) ? 1000L : null);

    assertFalse(_resolver.estimateRowsInTimeRange(RAW_TABLE, 2000L, 3000L).isPresent(),
        "Realtime has no statistics, so the estimate is unknown -- not zero");
  }

  /// When the side that can hold the range does have statistics, the structural zero from the
  /// other side is a real measurement and the estimate stands.
  @Test
  public void testRangeBelowBoundaryCountsOnlyOffline()
      throws Exception {
    insertSegments(OFFLINE_TABLE, List.of(row("seg1", 1L, 100L, 1024L, 0L, 100L, false)));
    insertSegments(REALTIME_TABLE, List.of(row("rtSeg1", 10L, 150L, 1024L, 1000L, 2000L, false)));
    _resolver.setTimeBoundaryMsProvider(rawName -> RAW_TABLE.equals(rawName) ? 1000L : null);

    OptionalLong estimate = _resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, 500L);
    assertTrue(estimate.isPresent());
    assertEquals(estimate.getAsLong(), 100L, "Realtime cannot serve rows below the boundary");
  }

  // ---------------------------------------------------------------------------
  // Nothing here may propagate onto the planning path
  // ---------------------------------------------------------------------------

  /// The providers are wired from broker startup and can fail during a startup race. A resolver
  /// that let that escape would fail query planning outright.
  @Test
  public void testThrowingProvidersDegradeInsteadOfPropagating()
      throws Exception {
    insertSegments(OFFLINE_TABLE, List.of(row("seg1", 1L, 100L, 1024L, 0L, 100L, false)));
    _resolver.setTimeBoundaryMsProvider(rawName -> {
      throw new IllegalStateException("boundary provider failed");
    });
    _resolver.setTableConfigProvider(tableNameWithType -> {
      throw new IllegalStateException("table config provider failed");
    });

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats, "A failing provider must cost precision, not the estimate");
    assertEquals(stats.getRowCount(), 100L);
    // The range estimate degrades to the no-boundary path rather than throwing.
    assertTrue(_resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, Long.MAX_VALUE).isPresent());
  }

  /// A store that fails every read leaves the resolver reporting "no statistics".
  @Test
  public void testStoreErrorsDegradeToNoStatistics()
      throws Exception {
    LogicalTableStatsResolver resolver = new LogicalTableStatsResolver(new UnreadableStatsStore());
    assertNull(resolver.getTableStats(RAW_TABLE));
    assertNull(resolver.getTableStats(OFFLINE_TABLE));
    assertFalse(resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, Long.MAX_VALUE).isPresent());
    assertFalse(resolver.estimateRowsInTimeRange(OFFLINE_TABLE, 0L, Long.MAX_VALUE).isPresent());
  }

  /// Opens cleanly, then fails every read.
  private static final class UnreadableStatsStore extends InMemoryStatsStore {
    @Override
    public TableStatistics getTableStats(String tableNameWithType)
        throws StatsStoreException {
      throw new StatsStoreException("getTableStats failed (stub)");
    }

    @Override
    public OptionalLong estimateRowsInTimeRange(String tableNameWithType, long startMs, long endMs)
        throws StatsStoreException {
      throw new StatsStoreException("estimateRowsInTimeRange failed (stub)");
    }

    @Override
    public boolean hasConsumingSegments(String tableNameWithType)
        throws StatsStoreException {
      throw new StatsStoreException("hasConsumingSegments failed (stub)");
    }
  }
}
