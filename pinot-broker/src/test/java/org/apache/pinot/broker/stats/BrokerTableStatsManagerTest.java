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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.broker.stats.BrokerTableStatsManager.TableStatsZkListener;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.SegmentColumnStatsRow;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for [BrokerTableStatsManager] and its inner [TableStatsZkListener].
///
/// Tests use a real [SqliteStatsStore] on a temporary directory; no mock store is
/// needed for behaviour tests. A minimal throwing-stub is used only for failure-isolation tests.
public class BrokerTableStatsManagerTest {

  private static final String TABLE = "myTable_OFFLINE";
  private static final String OTHER_TABLE = "otherTable_OFFLINE";
  private static final String TABLE_RT = "myTable_REALTIME";

  private Path _tempDir;
  private SqliteStatsStore _store;
  private BrokerTableStatsManager _manager;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("broker-stats-mgr-test-");
    _store = new SqliteStatsStore(_tempDir);
    _manager = new BrokerTableStatsManager(_store);
    _manager.init();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    if (_manager != null) {
      _manager.close();
    }
    deleteRecursively(_tempDir);
  }

  // ---------------------------------------------------------------------------
  // init: N segments — table stats reflect sums; consuming excluded
  // ---------------------------------------------------------------------------

  @Test
  public void testInitPopulatesStats()
      throws Exception {
    List<String> segments = List.of("seg1", "seg2", "seg3_consuming");
    List<ZNRecord> records = List.of(
        offlineRecord("seg1", 1L, 1000L, 4096L, 0L, 100L),
        offlineRecord("seg2", 2L, 2000L, 8192L, 100L, 200L),
        realtimeRecord("seg3_consuming", 3L, -1L, -1L, 200L, -1L, Status.IN_PROGRESS)
    );

    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null, segments, records);

    TableStatistics stats = _manager.getTableStats(TABLE);
    assertNotNull(stats, "Stats must be present after init");
    // Only seg1 and seg2 are committed; consuming excluded
    assertEquals(stats.getRowCount(), 3000L);
    assertEquals(stats.getTableSizeBytes(), 12288L);
  }

  @Test
  public void testInitNullZNRecordSkipped()
      throws Exception {
    List<String> segments = List.of("seg1", "seg2");
    // Arrays.asList, not List.of: the null is the point of this test and List.of rejects it.
    List<ZNRecord> records = Arrays.asList(
        offlineRecord("seg1", 1L, 500L, 2000L, 0L, 50L),
        null  // seg2 metadata missing
    );

    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null, segments, records);

    // Only seg1 should be in stats
    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE);
    assertEquals(crcs.size(), 1);
    assertTrue(crcs.containsKey("seg1"));
    assertFalse(crcs.containsKey("seg2"));
  }

  // ---------------------------------------------------------------------------
  // init: restart reconciliation
  // ---------------------------------------------------------------------------

  @Test
  public void testInitReconciliation()
      throws Exception {
    // Pre-populate store: stale segment (not in new online set), matching-crc, old-crc
    _store.upsertSegmentStats(TABLE, List.of(
        new SegmentStatsRow("stale_seg", 99L, 100L, 1000L, 0L, 10L, false),
        new SegmentStatsRow("matching_seg", 42L, 500L, 5000L, 0L, 50L, false),
        new SegmentStatsRow("changed_seg", 10L, 200L, 2000L, 0L, 20L, false)
    ));

    // New online set: matching_seg (same crc=42), changed_seg (new crc=11), new_seg
    List<String> segments = List.of("matching_seg", "changed_seg", "new_seg");
    List<ZNRecord> records = List.of(
        offlineRecord("matching_seg", 42L, 500L, 5000L, 0L, 50L),   // same crc — should NOT update
        offlineRecord("changed_seg", 11L, 300L, 3000L, 0L, 30L),    // changed crc — must update
        offlineRecord("new_seg", 77L, 100L, 1000L, 0L, 10L)          // new — must insert
    );

    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null, segments, records);

    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE);
    // stale_seg must be removed
    assertFalse(crcs.containsKey("stale_seg"), "Stale segment must be removed");
    // matching_seg unchanged
    assertEquals(crcs.get("matching_seg").longValue(), 42L);
    // changed_seg updated to new crc
    assertEquals(crcs.get("changed_seg").longValue(), 11L);
    // new_seg inserted
    assertEquals(crcs.get("new_seg").longValue(), 77L);

    // Table stats should reflect updated committed rows: 500 + 300 + 100 = 900
    TableStatistics stats = _manager.getTableStats(TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 900L);
  }

  // ---------------------------------------------------------------------------
  // onAssignmentChange: adds new segment, removes dropped one
  // ---------------------------------------------------------------------------

  @Test
  public void testOnAssignmentChangeAddsAndRemoves()
      throws Exception {
    // Use init() to prime both the store and the listener's in-memory segment set.
    List<String> initSegments = List.of("seg1");
    List<ZNRecord> initRecords = List.of(
        offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)
    );
    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null, initSegments, initRecords);

    // Assignment change: seg1 stays, seg2 comes online (pulled)
    Set<String> onlineSegments = Set.of("seg1", "seg2");
    List<String> pulledSegments = List.of("seg2");
    List<ZNRecord> pulledRecords = List.of(
        offlineRecord("seg2", 2L, 200L, 2000L, 10L, 20L)
    );

    listener.onAssignmentChange(null, null, onlineSegments, pulledSegments, pulledRecords);

    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE);
    assertTrue(crcs.containsKey("seg1"), "seg1 should remain");
    assertTrue(crcs.containsKey("seg2"), "seg2 should be added");
    assertEquals(crcs.size(), 2);

    // Now remove seg1 from the online set
    Set<String> updatedOnline = Set.of("seg2");
    listener.onAssignmentChange(null, null, updatedOnline, List.of(), List.of());

    Map<String, Long> crcs2 = _store.getSegmentCrcs(TABLE);
    assertFalse(crcs2.containsKey("seg1"), "seg1 should be removed");
    assertTrue(crcs2.containsKey("seg2"), "seg2 should remain");
  }

  // ---------------------------------------------------------------------------
  // refreshSegment: updates totalDocs
  // ---------------------------------------------------------------------------

  @Test
  public void testRefreshSegmentUpdatesTotalDocs()
      throws Exception {
    // Insert initial seg1
    _store.upsertSegmentStats(TABLE, List.of(
        new SegmentStatsRow("seg1", 1L, 100L, 1000L, 0L, 10L, false)
    ));

    // Refresh with new totalDocs
    ZNRecord updatedRecord = offlineRecord("seg1", 1L, 999L, 1000L, 0L, 10L);

    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.refreshSegment("seg1", updatedRecord);

    TableStatistics stats = _manager.getTableStats(TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 999L);
  }

  @Test
  public void testRefreshSegmentWithNullRemovesSegment()
      throws Exception {
    _store.upsertSegmentStats(TABLE, List.of(
        new SegmentStatsRow("seg1", 1L, 100L, 1000L, 0L, 10L, false)
    ));

    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.refreshSegment("seg1", null);

    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE);
    assertFalse(crcs.containsKey("seg1"), "Segment should be removed when record is null");
  }

  // ---------------------------------------------------------------------------
  // Failure isolation: listener methods must not throw when store throws
  // ---------------------------------------------------------------------------

  @Test
  public void testListenerDoesNotThrowOnStoreError() {
    StatsStore throwingStore = new ThrowingStatsStore();
    // Test the listener directly via TableStatsZkListener
    TableStatsZkListener listener = new TableStatsZkListener(TABLE, throwingStore, () -> { });

    List<String> segments = List.of("seg1");
    List<ZNRecord> records = List.of(
        offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)
    );

    // None of these must throw
    listener.init(null, null, segments, records);
    listener.onAssignmentChange(null, null, Set.of("seg1"), segments, records);
    listener.refreshSegment("seg1", records.get(0));
    listener.refreshSegment("seg1", null);
    // All passed if we reach this line
  }

  // ---------------------------------------------------------------------------
  // Manager read methods degrade gracefully when store errors
  // ---------------------------------------------------------------------------

  @Test
  public void testManagerReadsDegradeOnStoreError()
      throws Exception {
    // Opens cleanly, then fails every read -- so this goes through the real init() path.
    BrokerTableStatsManager mgr = new BrokerTableStatsManager(new ThrowingStatsStore(false));
    mgr.init();

    assertNull(mgr.getTableStats(TABLE), "Must return null on store read error");
    assertFalse(mgr.estimateRowsInTimeRange(TABLE, 0, 100).isPresent(),
        "Must return empty on store read error");
  }

  // ---------------------------------------------------------------------------
  // onRoutingRemoved purges stats
  // ---------------------------------------------------------------------------

  @Test
  public void testOnTableRemovedPurgesStats()
      throws Exception {
    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null,
        List.of("seg1"),
        List.of(offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)));

    assertNotNull(_manager.getTableStats(TABLE));
    // The routing manager invokes this on every listener when a table's routing entry is removed.
    listener.onRoutingRemoved();
    assertNull(_manager.getTableStats(TABLE), "Stats should be gone after onRoutingRemoved");
  }

  /// Builds an enabled manager over the given store.
  private static BrokerTableStatsManager enabledManagerOver(StatsStore store)
      throws Exception {
    BrokerTableStatsManager mgr = new BrokerTableStatsManager(store);
    mgr.init();
    return mgr;
  }

  // ---------------------------------------------------------------------------
  // Purging tables this broker no longer serves
  // ---------------------------------------------------------------------------

  /// A table dropped while the broker was down leaves rows no listener will ever revisit, because
  /// no routing entry is created for it again. The purge reclaims exactly those, and must not
  /// touch a table that is still served.
  @Test
  public void testPurgeTablesNoLongerServed()
      throws Exception {
    SegmentZkMetadataFetchListener served = _manager.createListener(TABLE);
    served.init(null, null, List.of("seg1"),
        List.of(offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)));
    SegmentZkMetadataFetchListener dropped = _manager.createListener(OTHER_TABLE);
    dropped.init(null, null, List.of("seg2"),
        List.of(offlineRecord("seg2", 2L, 200L, 2000L, 0L, 10L)));

    List<String> purged = _manager.purgeTablesNoLongerServed(TABLE::equals);

    assertEquals(purged, List.of(OTHER_TABLE), "Only the unserved table is purged");
    assertNotNull(_manager.getTableStats(TABLE), "A served table keeps its statistics");
    assertNull(_manager.getTableStats(OTHER_TABLE));
  }

  /// Purging must never be the reason a request fails: a store that cannot list its tables reports
  /// nothing purged rather than propagating.
  @Test
  public void testPurgeDegradesWhenTheStoreCannotListTables()
      throws Exception {
    try (BrokerTableStatsManager mgr = enabledManagerOver(new ThrowingStatsStore(false))) {
      assertEquals(mgr.purgeTablesNoLongerServed(t -> false), List.of());
    }
  }

  /// Disabled collection means there is nothing to purge and the store is never touched.
  @Test
  public void testPurgeIsANoOpWhenDisabled()
      throws Exception {
    try (BrokerTableStatsManager mgr = new BrokerTableStatsManager(new ThrowingStatsStore())) {
      assertEquals(mgr.purgeTablesNoLongerServed(t -> false), List.of());
    }
  }

  // ---------------------------------------------------------------------------
  // Consuming segment handling
  // ---------------------------------------------------------------------------

  @Test
  public void testConsumingSegmentExcludedFromTableStats()
      throws Exception {
    List<String> segments = List.of("committed", "consuming");
    List<ZNRecord> records = List.of(
        offlineRecord("committed", 1L, 1000L, 4000L, 0L, 100L),
        realtimeRecord("consuming", 2L, -1L, -1L, 100L, -1L, Status.IN_PROGRESS)
    );

    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE_RT);
    listener.init(null, null, segments, records);

    TableStatistics stats = _manager.getTableStats(TABLE_RT);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 1000L, "Only committed rows should count");
  }

  // ---------------------------------------------------------------------------
  // Factory helpers
  // ---------------------------------------------------------------------------

  /// Builds a ZNRecord simulating an offline (committed) segment with the given fields.
  private static ZNRecord offlineRecord(String segName, long crc, long totalDocs, long sizeBytes,
      long startMs, long endMs) {
    SegmentZKMetadata meta = new SegmentZKMetadata(segName);
    meta.setCrc(crc);
    if (totalDocs >= 0) {
      meta.setTotalDocs(totalDocs);
    }
    if (sizeBytes >= 0) {
      meta.setSizeInBytes(sizeBytes);
    }
    if (startMs > 0) {
      meta.setStartTime(startMs);
      meta.setTimeUnit(TimeUnit.MILLISECONDS);
    }
    if (endMs > 0) {
      meta.setEndTime(endMs);
      meta.setTimeUnit(TimeUnit.MILLISECONDS);
    }
    return meta.toZNRecord();
  }

  /// Builds a ZNRecord simulating a realtime segment with the given status.
  private static ZNRecord realtimeRecord(String segName, long crc, long totalDocs, long sizeBytes,
      long startMs, long endMs, Status status) {
    SegmentZKMetadata meta = new SegmentZKMetadata(segName);
    meta.setCrc(crc);
    if (totalDocs >= 0) {
      meta.setTotalDocs(totalDocs);
    }
    if (sizeBytes >= 0) {
      meta.setSizeInBytes(sizeBytes);
    }
    if (startMs > 0) {
      meta.setStartTime(startMs);
      meta.setTimeUnit(TimeUnit.MILLISECONDS);
    }
    if (endMs > 0) {
      meta.setEndTime(endMs);
      meta.setTimeUnit(TimeUnit.MILLISECONDS);
    }
    meta.setStatus(status);
    return meta.toZNRecord();
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

  // ---------------------------------------------------------------------------
  // Stub: store that always throws
  // ---------------------------------------------------------------------------

  /// Minimal [StatsStore] stub whose every method throws [StatsStoreException].
  /// Used to verify that listeners and the manager degrade gracefully.
  private static final class ThrowingStatsStore implements StatsStore {

    private final boolean _failInit;

    ThrowingStatsStore() {
      this(true);
    }

    /// @param failInit `false` models a store that opened cleanly but fails every subsequent
    ///     operation, which is what exercises the manager's read-degradation paths through the
    ///     real [BrokerTableStatsManager#init()] rather than around it.
    ThrowingStatsStore(boolean failInit) {
      _failInit = failInit;
    }

    @Override
    public void init()
        throws StatsStoreException {
      if (_failInit) {
        throw new StatsStoreException("init failed (stub)");
      }
    }

    @Override
    public void upsertSegmentStats(String tableNameWithType, List<SegmentStatsRow> rows)
        throws StatsStoreException {
      throw new StatsStoreException("upsert failed (stub)");
    }

    @Override
    public void upsertSegmentColumnStats(String tableNameWithType, List<SegmentColumnStatsRow> rows)
        throws StatsStoreException {
      throw new StatsStoreException("upsertCol failed (stub)");
    }

    @Override
    public void removeSegments(String tableNameWithType, Collection<String> segmentNames)
        throws StatsStoreException {
      throw new StatsStoreException("remove failed (stub)");
    }

    @Override
    public Map<String, Long> getSegmentCrcs(String tableNameWithType)
        throws StatsStoreException {
      throw new StatsStoreException("getCrcs failed (stub)");
    }

    @Override
    @Nullable
    public TableStatistics getTableStats(String tableNameWithType)
        throws StatsStoreException {
      throw new StatsStoreException("getTableStats failed (stub)");
    }

    @Override
    @Nullable
    public ColumnStatistics getColumnStats(String tableNameWithType, String columnName)
        throws StatsStoreException {
      throw new StatsStoreException("getColStats failed (stub)");
    }

    @Override
    public OptionalLong estimateRowsInTimeRange(String tableNameWithType, long startMs, long endMs)
        throws StatsStoreException {
      throw new StatsStoreException("estimateRows failed (stub)");
    }

    @Override
    public boolean hasConsumingSegments(String tableNameWithType)
        throws StatsStoreException {
      throw new StatsStoreException("hasConsumingSegments failed (stub)");
    }

    @Override
    public Set<String> getTables()
        throws StatsStoreException {
      throw new StatsStoreException("getTables failed (stub)");
    }

    @Override
    public void purgeTable(String tableNameWithType)
        throws StatsStoreException {
      throw new StatsStoreException("purgeTable failed (stub)");
    }

    @Override
    public void purgeAll()
        throws StatsStoreException {
      throw new StatsStoreException("purgeAll failed (stub)");
    }

    @Override
    public void close() {
    }
  }

  // ---------------------------------------------------------------------------
  // Recovery when the mirror and the store disagree
  // ---------------------------------------------------------------------------

  /// A store read that fails during init leaves the listener's in-memory mirror incomplete, so it
  /// cannot compute removals from it. The next assignment change must rebuild the mirror from the
  /// store and reclaim what is no longer online -- otherwise those rows leak until the broker
  /// restarts, with every test still green.
  @Test
  public void testFailedInitReconcilesOnTheNextAssignmentChange()
      throws Exception {
    FailFirstCrcStore store = new FailFirstCrcStore();
    try (BrokerTableStatsManager mgr = new BrokerTableStatsManager(store)) {
      mgr.init();
      SegmentZkMetadataFetchListener listener = mgr.createListener(TABLE);

      // init(): the CRC read fails, so the mirror is seeded from nothing even though both segments
      // are written to the store.
      listener.init(null, null, List.of("seg1", "seg2"),
          List.of(offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L),
              offlineRecord("seg2", 2L, 200L, 2000L, 0L, 10L)));
      assertEquals(store.getSegmentCrcs(TABLE).keySet(), Set.of("seg1", "seg2"));
      // One failed read from init(), plus the probe on the line above.
      assertEquals(store.getCrcCallCount(), 2, "init should have attempted exactly one CRC read");

      // seg2 goes offline. Without the reconcile the mirror would not know seg2 was ever stored,
      // so nothing would remove it.
      listener.onAssignmentChange(null, null, Set.of("seg1"), List.of(), List.of());
      assertEquals(store.getSegmentCrcs(TABLE).keySet(), Set.of("seg1"), "seg2 should have been reclaimed");

      // The flag is cleared, so later changes do not keep re-reading the store.
      int callsSoFar = store.getCrcCallCount();
      listener.onAssignmentChange(null, null, Set.of("seg1"), List.of(), List.of());
      assertEquals(store.getCrcCallCount(), callsSoFar, "The store should not be re-read once reconciled");
    }
  }

  /// An orphan purge that races a table's routing being rebuilt deletes rows underneath a listener
  /// that still believes they are present. Left alone the table would carry no statistics until the
  /// broker restarted, because a listener only removes segments its mirror knows about.
  @Test
  public void testPurgeRepairsAListenerItRacedAgainst()
      throws Exception {
    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null, List.of("seg1"),
        List.of(offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)));
    assertNotNull(_manager.getTableStats(TABLE));

    // The purge sees no routing (so it deletes), but the table is served by the time it re-checks.
    AtomicBoolean served = new AtomicBoolean(false);
    assertEquals(_manager.purgeTablesNoLongerServed(t -> served.getAndSet(true)), List.of(TABLE));
    assertNull(_manager.getTableStats(TABLE));

    // The listener was told its mirror is stale, so the next change rebuilds from the store rather
    // than trusting a mirror that still lists seg1.
    listener.onAssignmentChange(null, null, Set.of("seg1"), List.of("seg1"),
        List.of(offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)));
    TableStatistics stats = _manager.getTableStats(TABLE);
    assertNotNull(stats, "The table should carry statistics again");
    assertEquals(stats.getRowCount(), 100L);
  }

  // ---------------------------------------------------------------------------
  // Segment metadata that ZooKeeper does not carry
  // ---------------------------------------------------------------------------

  /// SegmentZKMetadata reports -1 for a field ZooKeeper does not hold, which is routine for older
  /// pushes. Storing that verbatim would SUBTRACT from the table's row count and size.
  @Test
  public void testCommittedSegmentWithUnknownDocsAndSizeContributesZero()
      throws Exception {
    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null, List.of("known", "unknown"),
        List.of(offlineRecord("known", 1L, 100L, 1000L, 0L, 10L),
            offlineRecord("unknown", 2L, -1L, -1L, 0L, 10L)));

    TableStatistics stats = _manager.getTableStats(TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 100L, "The unknown segment must contribute 0, not -1");
    assertEquals(stats.getTableSizeBytes(), 1000L, "The unknown segment must contribute 0, not -1");
  }

  /// Fails the first CRC read only, modelling a store that is briefly unreadable at init.
  private static final class FailFirstCrcStore extends InMemoryStatsStore {
    private final AtomicInteger _crcCalls = new AtomicInteger();

    @Override
    public Map<String, Long> getSegmentCrcs(String tableNameWithType)
        throws StatsStoreException {
      if (_crcCalls.getAndIncrement() == 0) {
        throw new StatsStoreException("getSegmentCrcs failed (stub)");
      }
      return super.getSegmentCrcs(tableNameWithType);
    }

    int getCrcCallCount() {
      return _crcCalls.get();
    }
  }
}
