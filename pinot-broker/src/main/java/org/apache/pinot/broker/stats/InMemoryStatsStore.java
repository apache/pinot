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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.SegmentColumnStatsRow;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.StatsAggregations;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;


/// Heap-resident [StatsStore], for deployments that do not want a database file on the broker
/// (read-only filesystems, ephemeral containers, tests).
///
/// ### What it trades away
/// Statistics do not survive a restart. For the base tier that costs little: the values are derived
/// from the ZooKeeper segment metadata the broker re-reads at startup anyway, so an empty store is
/// simply re-populated by the same listener callbacks — [#getSegmentCrcs] returns an empty map and
/// every segment is treated as new. Per-column statistics pulled from servers are the expensive
/// case, since those must be fetched again.
///
/// Table-level reads (row counts, consuming-segment detection) are served from a rollup that is
/// recomputed only after a write, so query planning does not scan the segment map. Time-range
/// estimates still scan the table's committed segments, since the answer depends on the requested
/// range.
///
/// Heap use is proportional to the number of segments (and stored columns) of the tables this
/// broker serves. The SQLite-backed store exists precisely to keep that off-heap, so prefer this
/// implementation only when the segment count is modest or a file is unacceptable.
///
/// ### Semantics
/// Identical to the SQLite-backed store, and deliberately expressed through the same
/// [StatsAggregations] helpers so the two cannot drift: consuming segments are excluded from
/// table-level, column-level and time-range reads but included by [#getSegmentCrcs]; a column row
/// only counts while its segment row exists; "no data" is reported as `null` /
/// [OptionalLong#empty()] and never as a zero estimate.
///
/// ### Thread-safety
/// Safe for concurrent reads with a single concurrent writer, as the interface requires. Reads
/// aggregate over concurrent maps and therefore observe a weakly-consistent view: a read racing a
/// multi-row upsert may see part of that batch. Unlike a SQLite transaction the batch is not
/// atomic, which is acceptable because every consumer treats these values as estimates.
public class InMemoryStatsStore implements StatsStore {

  /// table name → segment name → row.
  private final Map<String, Map<String, StoredSegment>> _segments = new ConcurrentHashMap<>();
  /// table name → segment name → column name → row.
  private final Map<String, Map<String, Map<String, SegmentColumnStatsRow>>> _columns = new ConcurrentHashMap<>();
  /// table name → monotonically increasing write counter, bumped after every mutation of that table.
  private final Map<String, AtomicLong> _versions = new ConcurrentHashMap<>();
  /// table name → table-level rollup, valid only while its version matches the table's counter.
  private final Map<String, CachedAggregate> _aggregates = new ConcurrentHashMap<>();

  private volatile boolean _closed;

  @Override
  public void init() {
    // Nothing to open: the maps are ready on construction.
  }

  @Override
  public void upsertSegmentStats(String tableNameWithType, List<SegmentStatsRow> rows)
      throws StatsStoreException {
    checkOpen();
    long now = System.currentTimeMillis();
    Map<String, StoredSegment> table = _segments.computeIfAbsent(tableNameWithType, k -> new ConcurrentHashMap<>());
    for (SegmentStatsRow row : rows) {
      table.put(row.segmentName(), new StoredSegment(row, now));
    }
    invalidate(tableNameWithType);
  }

  @Override
  public void upsertSegmentColumnStats(String tableNameWithType, List<SegmentColumnStatsRow> rows)
      throws StatsStoreException {
    checkOpen();
    Map<String, Map<String, SegmentColumnStatsRow>> table =
        _columns.computeIfAbsent(tableNameWithType, k -> new ConcurrentHashMap<>());
    for (SegmentColumnStatsRow row : rows) {
      table.computeIfAbsent(row.segmentName(), k -> new ConcurrentHashMap<>()).put(row.columnName(), row);
    }
  }

  @Override
  public void removeSegments(String tableNameWithType, Collection<String> segmentNames)
      throws StatsStoreException {
    checkOpen();
    Map<String, StoredSegment> segments = _segments.get(tableNameWithType);
    Map<String, Map<String, SegmentColumnStatsRow>> columns = _columns.get(tableNameWithType);
    for (String segmentName : segmentNames) {
      if (segments != null) {
        segments.remove(segmentName);
      }
      if (columns != null) {
        columns.remove(segmentName);
      }
    }
    // Drop emptied tables, so getTables() means "holds statistics" here exactly as it does in a
    // row-backed store -- it is the input to a destructive purge, so the two must not disagree.
    if (segments != null && segments.isEmpty()) {
      _segments.remove(tableNameWithType, segments);
    }
    if (columns != null && columns.isEmpty()) {
      _columns.remove(tableNameWithType, columns);
    }
    if (_segments.get(tableNameWithType) == null && _columns.get(tableNameWithType) == null) {
      // Nothing left to roll up; see purgeTable for why this removes instead of invalidating.
      _aggregates.remove(tableNameWithType);
      _versions.remove(tableNameWithType);
    } else {
      invalidate(tableNameWithType);
    }
  }

  @Override
  public Map<String, Long> getSegmentCrcs(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    Map<String, StoredSegment> segments = _segments.get(tableNameWithType);
    // Mutable and detached, like the SQLite store's result: the SPI does not promise immutability
    // and callers may adjust the map.
    if (segments == null) {
      return new HashMap<>();
    }
    // Includes consuming segments: reconciliation must not re-upsert them on every restart.
    Map<String, Long> crcs = new HashMap<>(segments.size());
    for (Map.Entry<String, StoredSegment> entry : segments.entrySet()) {
      crcs.put(entry.getKey(), entry.getValue()._row.crc());
    }
    return crcs;
  }

  @Override
  @Nullable
  public TableStatistics getTableStats(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    return aggregate(tableNameWithType)._stats;
  }

  @Override
  @Nullable
  public ColumnStatistics getColumnStats(String tableNameWithType, String columnName)
      throws StatsStoreException {
    checkOpen();
    Map<String, Map<String, SegmentColumnStatsRow>> columnsBySegment = _columns.get(tableNameWithType);
    Map<String, StoredSegment> segments = _segments.get(tableNameWithType);
    if (columnsBySegment == null || segments == null) {
      return null;
    }

    // Same fold as the SQLite store, through the shared accumulator, so the two cannot disagree.
    StatsAggregations.ColumnStatsAccumulator accumulator = new StatsAggregations.ColumnStatsAccumulator();
    for (Map.Entry<String, Map<String, SegmentColumnStatsRow>> entry : columnsBySegment.entrySet()) {
      SegmentColumnStatsRow column = entry.getValue().get(columnName);
      if (column == null) {
        continue;
      }
      // Mirrors the SQL join onto segment_stats: a column row without a live, committed segment row
      // contributes nothing, and the segment row supplies the doc count used for weighting.
      StoredSegment segment = segments.get(entry.getKey());
      if (segment == null || segment._row.consuming()) {
        continue;
      }
      accumulator.add(segment._row.totalDocs(), column);
    }
    return accumulator.isEmpty() ? null : accumulator.build(columnName);
  }

  @Override
  public OptionalLong estimateRowsInTimeRange(String tableNameWithType, long startMs, long endMs)
      throws StatsStoreException {
    checkOpen();
    Map<String, StoredSegment> segments = _segments.get(tableNameWithType);
    if (segments == null) {
      return OptionalLong.empty();
    }
    long totalRows = 0;
    boolean hasCommittedSegment = false;
    for (StoredSegment segment : segments.values()) {
      SegmentStatsRow row = segment._row;
      if (row.consuming()) {
        continue;
      }
      // Distinguishes "no overlapping segment" (a real estimate of 0) from "no statistics"
      // (empty), exactly as the SQL existence sentinel does.
      hasCommittedSegment = true;
      totalRows += StatsAggregations.overlapRows(row.totalDocs(), row.startTimeMs(), row.endTimeMs(),
          startMs, endMs);
    }
    return hasCommittedSegment ? OptionalLong.of(totalRows) : OptionalLong.empty();
  }

  @Override
  public Set<String> getTables()
      throws StatsStoreException {
    checkOpen();
    Set<String> tables = new HashSet<>(_segments.keySet());
    tables.addAll(_columns.keySet());
    return tables;
  }

  @Override
  public void purgeTable(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    _segments.remove(tableNameWithType);
    _columns.remove(tableNameWithType);
    // Remove rather than invalidate: invalidate() would re-create the version entry it just
    // dropped, so a broker that serves many tables over its lifetime would accumulate one entry
    // per table it ever saw.
    _aggregates.remove(tableNameWithType);
    _versions.remove(tableNameWithType);
  }

  @Override
  public void purgeAll()
      throws StatsStoreException {
    checkOpen();
    _segments.clear();
    _columns.clear();
    _aggregates.clear();
    _versions.clear();
  }

  @Override
  public boolean hasConsumingSegments(String tableNameWithType)
      throws StatsStoreException {
    checkOpen();
    return aggregate(tableNameWithType)._hasConsuming;
  }

  @Override
  public void close() {
    _closed = true;
    _segments.clear();
    _columns.clear();
    _aggregates.clear();
    _versions.clear();
  }

  /// Returns the table-level rollup, recomputing it only when the table changed since it was last
  /// computed. Query planning asks for these on every compile, so an O(#segments) scan per call
  /// would show up directly in planning latency; writes are comparatively rare.
  ///
  /// The version is read before the scan and re-checked after it, so a rollup computed from a state
  /// a concurrent writer has already replaced is used once but never cached.
  private CachedAggregate aggregate(String tableNameWithType) {
    AtomicLong version = _versions.computeIfAbsent(tableNameWithType, k -> new AtomicLong());
    long observed = version.get();
    CachedAggregate cached = _aggregates.get(tableNameWithType);
    if (cached != null && cached._version == observed) {
      return cached;
    }
    CachedAggregate computed = computeAggregate(tableNameWithType, observed);
    if (version.get() == observed) {
      _aggregates.put(tableNameWithType, computed);
    }
    return computed;
  }

  private CachedAggregate computeAggregate(String tableNameWithType, long version) {
    Map<String, StoredSegment> segments = _segments.get(tableNameWithType);
    if (segments == null) {
      return new CachedAggregate(version, null, false);
    }
    long totalDocs = 0;
    long sizeBytes = 0;
    long maxUpdatedAt = 0;
    int committed = 0;
    boolean hasConsuming = false;
    for (StoredSegment segment : segments.values()) {
      if (segment._row.consuming()) {
        hasConsuming = true;
        continue;
      }
      totalDocs += segment._row.totalDocs();
      sizeBytes += segment._row.sizeBytes();
      maxUpdatedAt = Math.max(maxUpdatedAt, segment._updatedAtMs);
      committed++;
    }
    if (committed == 0) {
      return new CachedAggregate(version, null, hasConsuming);
    }
    return new CachedAggregate(version, TableStatistics.builder()
        .rowCount(totalDocs, StatConfidence.EXACT)
        .tableSizeBytes(sizeBytes, StatConfidence.EXACT)
        .updatedAtMs(maxUpdatedAt)
        .build(), hasConsuming);
  }

  private void invalidate(String tableNameWithType) {
    _versions.computeIfAbsent(tableNameWithType, k -> new AtomicLong()).incrementAndGet();
  }

  private void checkOpen()
      throws StatsStoreException {
    if (_closed) {
      throw new StatsStoreException("InMemoryStatsStore is closed");
    }
  }

  /// A table-level rollup together with the write version it was computed from.
  private static final class CachedAggregate {
    private final long _version;
    @Nullable
    private final TableStatistics _stats;
    private final boolean _hasConsuming;

    CachedAggregate(long version, @Nullable TableStatistics stats, boolean hasConsuming) {
      _version = version;
      _stats = stats;
      _hasConsuming = hasConsuming;
    }
  }

  /// A stored segment row plus the wall-clock time it was written, which the SQLite schema keeps in
  /// its `updated_at_ms` column and exposes as [TableStatistics#getUpdatedAtMs()].
  private static final class StoredSegment {
    private final SegmentStatsRow _row;
    private final long _updatedAtMs;

    StoredSegment(SegmentStatsRow row, long updatedAtMs) {
      _row = row;
      _updatedAtMs = updatedAtMs;
    }
  }
}
