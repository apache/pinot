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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.broker.stats.BrokerTableStatsManager.TableStatsZkListener;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for {@link BrokerTableStatsManager} and its inner
 * {@link TableStatsZkListener}.
 *
 * <p>Tests use a real {@link SqliteStatsStore} on a temporary directory; no mock store is
 * needed for behaviour tests. A minimal throwing-stub is used only for failure-isolation tests.
 */
public class BrokerTableStatsManagerTest {

  private static final String TABLE = "myTable_OFFLINE";
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
    List<String> segments = Arrays.asList("seg1", "seg2", "seg3_consuming");
    List<ZNRecord> records = Arrays.asList(
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
    List<String> segments = Arrays.asList("seg1", "seg2");
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
    _store.upsertSegmentStats(TABLE, Arrays.asList(
        new SegmentStatsRow("stale_seg", 99L, 100L, 1000L, 0L, 10L, false),
        new SegmentStatsRow("matching_seg", 42L, 500L, 5000L, 0L, 50L, false),
        new SegmentStatsRow("changed_seg", 10L, 200L, 2000L, 0L, 20L, false)
    ));

    // New online set: matching_seg (same crc=42), changed_seg (new crc=11), new_seg
    List<String> segments = Arrays.asList("matching_seg", "changed_seg", "new_seg");
    List<ZNRecord> records = Arrays.asList(
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
    List<String> initSegments = Collections.singletonList("seg1");
    List<ZNRecord> initRecords = Collections.singletonList(
        offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)
    );
    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null, initSegments, initRecords);

    // Assignment change: seg1 stays, seg2 comes online (pulled)
    Set<String> onlineSegments = Set.of("seg1", "seg2");
    List<String> pulledSegments = Collections.singletonList("seg2");
    List<ZNRecord> pulledRecords = Collections.singletonList(
        offlineRecord("seg2", 2L, 200L, 2000L, 10L, 20L)
    );

    listener.onAssignmentChange(null, null, onlineSegments, pulledSegments, pulledRecords);

    Map<String, Long> crcs = _store.getSegmentCrcs(TABLE);
    assertTrue(crcs.containsKey("seg1"), "seg1 should remain");
    assertTrue(crcs.containsKey("seg2"), "seg2 should be added");
    assertEquals(crcs.size(), 2);

    // Now remove seg1 from the online set
    Set<String> updatedOnline = Set.of("seg2");
    listener.onAssignmentChange(null, null, updatedOnline, Collections.emptyList(),
        Collections.emptyList());

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
    _store.upsertSegmentStats(TABLE, Collections.singletonList(
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
    _store.upsertSegmentStats(TABLE, Collections.singletonList(
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
    BrokerTableStatsManager mgr = new BrokerTableStatsManager(throwingStore);
    // Bypass init() — manager stays disabled; createListener returns no-op
    // We test the listener directly via TableStatsZkListener
    TableStatsZkListener listener = new TableStatsZkListener(TABLE, throwingStore);

    List<String> segments = Collections.singletonList("seg1");
    List<ZNRecord> records = Collections.singletonList(
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
    StatsStore throwingStore = new ThrowingStatsStore();
    BrokerTableStatsManager mgr = new BrokerTableStatsManager(throwingStore) {
      @Override
      public void init()
          throws StatsStoreException {
        // Force enable without calling store.init()
        // Simulate a store that opened OK but fails on reads
        try {
          java.lang.reflect.Field f = BrokerTableStatsManager.class.getDeclaredField("_enabled");
          f.setAccessible(true);
          f.set(this, true);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    };
    mgr.init();

    assertNull(mgr.getTableStats(TABLE), "Must return null on store read error");
    assertFalse(mgr.estimateRowsInTimeRange(TABLE, 0, 100).isPresent(),
        "Must return empty on store read error");
  }

  // ---------------------------------------------------------------------------
  // onTableRemoved purges stats
  // ---------------------------------------------------------------------------

  @Test
  public void testOnTableRemovedPurgesStats()
      throws Exception {
    SegmentZkMetadataFetchListener listener = _manager.createListener(TABLE);
    listener.init(null, null,
        Collections.singletonList("seg1"),
        Collections.singletonList(offlineRecord("seg1", 1L, 100L, 1000L, 0L, 10L)));

    assertNotNull(_manager.getTableStats(TABLE));
    _manager.onTableRemoved(TABLE);
    assertNull(_manager.getTableStats(TABLE), "Stats should be gone after onTableRemoved");
  }

  // ---------------------------------------------------------------------------
  // Consuming segment handling
  // ---------------------------------------------------------------------------

  @Test
  public void testConsumingSegmentExcludedFromTableStats()
      throws Exception {
    List<String> segments = Arrays.asList("committed", "consuming");
    List<ZNRecord> records = Arrays.asList(
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

  /**
   * Builds a ZNRecord simulating an offline (committed) segment with the given fields.
   */
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

  /**
   * Builds a ZNRecord simulating a realtime segment with the given status.
   */
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

  /** Recursively deletes a directory tree. */
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

  /**
   * Minimal {@link StatsStore} stub whose every method throws {@link StatsStoreException}.
   * Used to verify that listeners and the manager degrade gracefully.
   */
  private static final class ThrowingStatsStore implements StatsStore {

    @Override
    public void init()
        throws StatsStoreException {
      throw new StatsStoreException("init failed (stub)");
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
}
