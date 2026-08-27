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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.query.planner.spi.stats.SegmentStatsRow;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Broker-wide singleton that owns one [StatsStore] and produces per-table
/// [SegmentZkMetadataFetchListener] instances that populate it.
///
/// ### Usage
/// 1. Construct with a pre-created (but not yet init()d) [StatsStore].
/// 1. Call [#init()] — on failure the manager disables itself; broker startup is not affected.
/// 1. For each table, call [#createListener(String)] and register the result on that
///    table's [org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher]
///    *before* the fetcher's own `init()` call.
/// 1. Per-table stats are purged automatically when the routing entry is removed (the listener
///    implements [SegmentZkMetadataFetchListener#onRoutingRemoved()]).
/// 1. Close the manager when the broker shuts down.
///
/// ### Thread-safety
/// Read methods ([#getTableStats], [#estimateRowsInTimeRange]) are safe for
/// concurrent access by any number of reader threads. [#createListener] and
/// the listener callbacks are invoked from the routing-manager's table-build thread; the store
/// itself handles single-writer / multi-reader concurrency internally.
///
/// ### Failure isolation
/// All [StatsStoreException] escapes are suppressed here — callers on the query path
/// will receive `null` / empty rather than a propagated exception.
public class BrokerTableStatsManager implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerTableStatsManager.class);

  /// Live listeners by table, so [#purgeTablesNoLongerServed] can repair one whose mirror it
  /// invalidated. Entries are replaced when a table's routing is rebuilt and dropped when its
  /// routing goes away.
  private final Map<String, TableStatsZkListener> _listeners = new ConcurrentHashMap<>();

  private final StatsStore _statsStore;
  private final LogicalTableStatsResolver _resolver;
  /// False when init() failed; all operations become no-ops in that state.
  private volatile boolean _enabled = false;

  /// Constructs a new manager backed by the given [StatsStore].
  /// The store must not have been opened yet; [#init()] will call [StatsStore#init()].
  ///
  /// @param statsStore backing store; owned by this manager
  public BrokerTableStatsManager(StatsStore statsStore) {
    _statsStore = statsStore;
    _resolver = new LogicalTableStatsResolver(statsStore);
  }

  /// Sets the provider used to look up the time boundary (epoch-milliseconds) for a raw table
  /// name. Call this after the routing manager is fully initialized.
  ///
  /// A `null` return value from `provider` means no boundary is available for
  /// that table, which causes the resolver to fall back to a plain sum of offline + realtime rows
  /// with [org.apache.pinot.query.planner.spi.stats.StatConfidence#ESTIMATED] confidence.
  ///
  /// @param provider function from raw table name → time boundary in epoch-milliseconds, nullable
  public void setTimeBoundaryMsProvider(@Nullable Function<String, Long> provider) {
    _resolver.setTimeBoundaryMsProvider(provider);
  }

  /// Sets the provider used to look up the [TableConfig] for a fully-qualified
  /// (type-suffixed) table name. Call this after the table cache is initialized.
  ///
  /// Required for upsert/dedup detection; without this, upsert/dedup tables will report
  /// [org.apache.pinot.query.planner.spi.stats.StatConfidence#EXACT] rather than
  /// [org.apache.pinot.query.planner.spi.stats.StatConfidence#LOW].
  ///
  /// @param provider function from suffixed table name → TableConfig, nullable
  public void setTableConfigProvider(@Nullable Function<String, TableConfig> provider) {
    _resolver.setTableConfigProvider(provider);
  }

  /// Opens the backing store. On failure, logs an error and sets the manager to disabled; the
  /// broker should still start normally.
  ///
  /// @throws StatsStoreException if the store cannot be opened (callers may log and ignore)
  public void init()
      throws StatsStoreException {
    _statsStore.init();
    _enabled = true;
    LOGGER.info("BrokerTableStatsManager initialized");
  }

  /// Creates a listener that will maintain stats for `tableNameWithType` in the backing
  /// store. Must be registered on the table's
  /// [org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher] before the
  /// fetcher is initialized.
  ///
  /// If the manager is disabled (init failed), returns a no-op listener.
  ///
  /// @param tableNameWithType fully-qualified table name (e.g. `myTable_OFFLINE`)
  /// @return a new listener instance for that table
  public SegmentZkMetadataFetchListener createListener(String tableNameWithType) {
    if (!_enabled) {
      return NoOpListener.INSTANCE;
    }
    TableStatsZkListener listener =
        new TableStatsZkListener(tableNameWithType, _statsStore, () -> _listeners.remove(tableNameWithType));
    // Kept so an orphan purge can repair a listener whose mirror it invalidated. A routing entry
    // that is rebuilt replaces the listener, so last-writer-wins is the behaviour we want.
    _listeners.put(tableNameWithType, listener);
    return listener;
  }

  /// Drops statistics for every stored table that `stillServed` rejects.
  ///
  /// Per-table cleanup is normally driven by [SegmentZkMetadataFetchListener#onRoutingRemoved()],
  /// which a broker can only observe while it is running. A table dropped while this broker was
  /// down leaves rows behind that nothing would ever revisit, because no routing entry — and so no
  /// listener — is created for it again.
  ///
  /// Only call this once routing has settled. Before then "no routing for T" also matches a table
  /// that has merely not loaded yet, and purging then discards column statistics that are expensive
  /// to re-fetch. That is why this is driven by an operator request rather than run at startup.
  ///
  /// Errors are logged and ignored: this is housekeeping, and failing it must not affect startup.
  ///
  /// @param stillServed returns `true` for a table this broker still has routing for
  /// @return the tables whose statistics were dropped, in the order they were dropped
  public List<String> purgeTablesNoLongerServed(Predicate<String> stillServed) {
    if (!_enabled) {
      return List.of();
    }
    Set<String> storedTables;
    try {
      storedTables = _statsStore.getTables();
    } catch (StatsStoreException e) {
      LOGGER.warn("Could not list stored tables to purge orphans: {}", e.getMessage());
      return List.of();
    }
    List<String> purged = new ArrayList<>();
    for (String tableNameWithType : storedTables) {
      if (stillServed.test(tableNameWithType)) {
        continue;
      }
      try {
        _statsStore.purgeTable(tableNameWithType);
        purged.add(tableNameWithType);
        LOGGER.info("Purged statistics for table no longer served by this broker: {}", tableNameWithType);
      } catch (StatsStoreException e) {
        LOGGER.warn("Failed to purge statistics for {}: {}", tableNameWithType, e.getMessage());
        continue;
      }
      // The liveness test and the purge are not atomic with respect to a routing build, which
      // publishes its routing entry only after its listener has already written this table's rows.
      // A table that became served in that window has just had those rows deleted underneath a
      // listener that believes they are still there -- and since a listener only ever removes
      // segments its mirror knows about, the loss would persist until the broker restarts. Re-test
      // and hand the listener back a clean slate instead.
      if (stillServed.test(tableNameWithType)) {
        TableStatsZkListener listener = _listeners.get(tableNameWithType);
        if (listener != null) {
          listener.requestFullReconcile();
          LOGGER.info("Table {} became served while its statistics were being purged; its listener "
              + "will rebuild from the store", tableNameWithType);
        }
      }
    }
    return purged;
  }

  /// Returns logical table statistics for the given table name, or `null` if unavailable.
  ///
  /// Accepts both suffixed physical names (`foo_OFFLINE` / `foo_REALTIME`) and raw
  /// logical names (`foo`):
  /// - Suffixed names: returns physical stats with per-type confidence adjustments (upsert,
  ///   dedup, consuming-segment detection).
  /// - Raw names: returns a logical hybrid view merging offline and realtime stats at the
  ///   time boundary; if no boundary is available, returns a plain sum with
  ///   [org.apache.pinot.query.planner.spi.stats.StatConfidence#ESTIMATED] confidence.
  ///
  /// Any store error is logged at WARN and `null` is returned.
  ///
  /// @param tableName raw table name or fully-qualified name with type suffix
  @Nullable
  public TableStatistics getTableStats(String tableName) {
    if (!_enabled) {
      return null;
    }
    return _resolver.getTableStats(tableName);
  }

  /// Returns an estimate of the number of rows in the given time range, or an empty optional if
  /// unavailable. Any store error is logged at WARN and an empty optional is returned.
  ///
  /// For hybrid (raw) table names, the estimate is split at the time boundary:
  /// offline rows are counted for `[startMs, boundary)` and realtime rows for
  /// `[boundary, endMs)`.
  ///
  /// @param tableName raw table name or fully-qualified name with type suffix
  /// @param startMs   inclusive range start in epoch milliseconds
  /// @param endMs     exclusive range end in epoch milliseconds
  public OptionalLong estimateRowsInTimeRange(String tableName, long startMs, long endMs) {
    if (!_enabled) {
      return OptionalLong.empty();
    }
    return _resolver.estimateRowsInTimeRange(tableName, startMs, endMs);
  }

  @Override
  public void close()
      throws IOException {
    // Disable before closing the store so that concurrent read calls on the query path
    // short-circuit cleanly without triggering WARN log spam from a closed store.
    _enabled = false;
    try {
      _statsStore.close();
    } catch (IOException e) {
      LOGGER.warn("Error closing StatsStore: {}", e.getMessage());
      throw e;
    }
  }

  // ---------------------------------------------------------------------------
  // Inner class: TableStatsZkListener
  // ---------------------------------------------------------------------------

  /// [SegmentZkMetadataFetchListener] that maintains segment-level statistics for a single
  /// table in a [StatsStore].
  ///
  /// ### Thread-safety
  /// Instances are called sequentially from the routing manager's per-table lock, so no
  /// additional synchronization is needed inside this class.
  ///
  /// ### Failure isolation
  /// All [StatsStoreException] are caught; errors are logged at WARN and the listener
  /// never throws back into the routing manager.
  static final class TableStatsZkListener implements SegmentZkMetadataFetchListener {
    private static final Logger LISTENER_LOGGER = LoggerFactory.getLogger(TableStatsZkListener.class);

    private final String _tableNameWithType;
    private final StatsStore _statsStore;
    /// Deregisters this listener from its manager once its routing entry is gone, so the registry
    /// does not retain one entry per table the broker has ever served.
    private final Runnable _onRemoved;
    /// In-memory mirror of the segments currently persisted in the store for this table.
    /// Maintained after [#init] so that [#onAssignmentChange] can compute
    /// removals without a full DB round-trip.
    ///
    /// Accessed only from the routing-manager's per-table lock — no additional
    /// synchronization needed.
    private final Set<String> _persistedSegments = new HashSet<>();
    /// Set when the mirror may disagree with the store, so the next assignment change re-reads the
    /// store to recompute removals. Volatile because an orphan purge sets it from an HTTP thread,
    /// unlike every other field here, which the routing-manager lock confines to one thread.
    private volatile boolean _needsFullReconcile;

    TableStatsZkListener(String tableNameWithType, StatsStore statsStore, Runnable onRemoved) {
      _tableNameWithType = tableNameWithType;
      _statsStore = statsStore;
      _onRemoved = onRemoved;
    }

    /// Declares this listener's mirror untrustworthy, so the next assignment change rebuilds it
    /// from the store. Callable from any thread.
    void requestFullReconcile() {
      _needsFullReconcile = true;
    }

    @Override
    public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
        List<ZNRecord> znRecords) {
      // Restart reconciliation: read stored CRCs, upsert changed/new, remove dropped segments.
      Map<String, Long> storedCrcs;
      try {
        storedCrcs = _statsStore.getSegmentCrcs(_tableNameWithType);
      } catch (StatsStoreException e) {
        // An empty map here would mean "the store holds nothing", so the stale-segment pass below
        // would find nothing to remove and later passes -- which work off _persistedSegments --
        // would never revisit those rows. Ask the next assignment change to reconcile instead.
        LISTENER_LOGGER.warn("Failed to read stored CRCs for {} during init; will upsert all segments and "
            + "reconcile on the next assignment change: {}", _tableNameWithType, e.getMessage());
        storedCrcs = Map.of();
        _needsFullReconcile = true;
      }
      // Seed from what the STORE holds, not from what this pass writes: a row we fail to touch is
      // still a row that must remain reclaimable.
      _persistedSegments.addAll(storedCrcs.keySet());

      int n = onlineSegments.size();
      List<SegmentStatsRow> toUpsert = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        String segment = onlineSegments.get(i);
        ZNRecord znRecord = znRecords.get(i);
        if (znRecord == null) {
          continue;
        }
        SegmentZKMetadata meta = new SegmentZKMetadata(znRecord);
        long crc = meta.getCrc();
        Long stored = storedCrcs.get(segment);
        if (stored != null && stored == crc) {
          // CRC matches — data is still valid, skip upsert but track as persisted
          _persistedSegments.add(segment);
          continue;
        }
        toUpsert.add(buildRow(segment, meta));
      }

      if (!toUpsert.isEmpty()) {
        try {
          _statsStore.upsertSegmentStats(_tableNameWithType, toUpsert);
          for (SegmentStatsRow row : toUpsert) {
            _persistedSegments.add(row.segmentName());
          }
        } catch (StatsStoreException e) {
          LISTENER_LOGGER.warn("Failed to upsert segment stats for {} during init: {}", _tableNameWithType,
              e.getMessage());
        }
      }

      // Remove persisted segments that are no longer online
      Set<String> onlineSet = Set.copyOf(onlineSegments);
      List<String> toRemove = new ArrayList<>();
      for (String persisted : storedCrcs.keySet()) {
        if (!onlineSet.contains(persisted)) {
          toRemove.add(persisted);
        }
      }
      if (!toRemove.isEmpty()) {
        try {
          _statsStore.removeSegments(_tableNameWithType, toRemove);
          _persistedSegments.removeAll(toRemove);
        } catch (StatsStoreException e) {
          // Keep them in the mirror: they are still in the store, so a later pass must retry.
          LISTENER_LOGGER.warn("Failed to remove stale segments for {} during init: {}", _tableNameWithType,
              e.getMessage());
        }
      }
    }

    @Override
    public void onAssignmentChange(IdealState idealState, ExternalView externalView,
        Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
      // Upsert newly-online segments
      int n = pulledSegments.size();
      if (n > 0) {
        List<SegmentStatsRow> toUpsert = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
          ZNRecord znRecord = znRecords.get(i);
          if (znRecord == null) {
            continue;
          }
          SegmentZKMetadata meta = new SegmentZKMetadata(znRecord);
          toUpsert.add(buildRow(pulledSegments.get(i), meta));
        }
        if (!toUpsert.isEmpty()) {
          try {
            _statsStore.upsertSegmentStats(_tableNameWithType, toUpsert);
            for (SegmentStatsRow row : toUpsert) {
              _persistedSegments.add(row.segmentName());
            }
          } catch (StatsStoreException e) {
            LISTENER_LOGGER.warn("Failed to upsert segment stats for {} on assignment change: {}",
                _tableNameWithType, e.getMessage());
          }
        }
      }

      // Remove persisted segments that are no longer online.
      // Use the in-memory mirror to avoid a full DB round-trip.
      if (_needsFullReconcile) {
        try {
          // Replace rather than merge: the store is the authority, and the mirror can be wrong in
          // both directions -- short of rows when init() could not read the store, and ahead of it
          // when an orphan purge raced this table's routing being rebuilt.
          Set<String> stored = _statsStore.getSegmentCrcs(_tableNameWithType).keySet();
          _persistedSegments.clear();
          _persistedSegments.addAll(stored);
          _needsFullReconcile = false;
        } catch (StatsStoreException e) {
          LISTENER_LOGGER.warn("Reconcile of stored segments for {} failed; will retry: {}", _tableNameWithType,
              e.getMessage());
        }
      }
      List<String> toRemove = new ArrayList<>();
      for (String persisted : _persistedSegments) {
        if (!onlineSegments.contains(persisted)) {
          toRemove.add(persisted);
        }
      }
      if (!toRemove.isEmpty()) {
        try {
          _statsStore.removeSegments(_tableNameWithType, toRemove);
          _persistedSegments.removeAll(toRemove);
        } catch (StatsStoreException e) {
          LISTENER_LOGGER.warn("Failed to remove dropped segments for {} on assignment change: {}",
              _tableNameWithType, e.getMessage());
        }
      }
    }

    @Override
    public void onRoutingRemoved() {
      _onRemoved.run();
      try {
        _statsStore.purgeTable(_tableNameWithType);
      } catch (StatsStoreException e) {
        LISTENER_LOGGER.warn("Failed to purge stats for removed table {}: {}", _tableNameWithType, e.getMessage());
      }
    }

    @Override
    public void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
      if (znRecord == null) {
        // Segment disappeared — remove from store
        try {
          _statsStore.removeSegments(_tableNameWithType, List.of(segment));
          _persistedSegments.remove(segment);
        } catch (StatsStoreException e) {
          LISTENER_LOGGER.warn("Failed to remove segment {} for {} on refresh: {}", segment,
              _tableNameWithType, e.getMessage());
        }
        return;
      }
      SegmentZKMetadata meta = new SegmentZKMetadata(znRecord);
      List<SegmentStatsRow> rows = List.of(buildRow(segment, meta));
      try {
        _statsStore.upsertSegmentStats(_tableNameWithType, rows);
        _persistedSegments.add(segment);
      } catch (StatsStoreException e) {
        LISTENER_LOGGER.warn("Failed to upsert segment {} for {} on refresh: {}", segment, _tableNameWithType,
            e.getMessage());
      }
    }

    /// Converts a [SegmentZKMetadata] into a [SegmentStatsRow].
    /// A segment is considered consuming when it is a realtime segment whose status is
    /// [Status#IN_PROGRESS].
    /// For non-consuming segments, negative totalDocs is stored as 0.
    private static SegmentStatsRow buildRow(String segmentName, SegmentZKMetadata meta) {
      boolean consuming = meta.getStatus() == Status.IN_PROGRESS;
      long totalDocs = meta.getTotalDocs();
      if (!consuming && totalDocs < 0) {
        LISTENER_LOGGER.debug("Segment {} has negative totalDocs ({}); storing 0", segmentName, totalDocs);
        totalDocs = 0;
      }
      // Size is -1 when unknown in ZK metadata; clamp so SUM(size_bytes) is not skewed downwards
      // by sentinel values.
      long sizeBytes = Math.max(meta.getSizeInBytes(), 0);
      return new SegmentStatsRow(segmentName, meta.getCrc(), totalDocs, sizeBytes,
          meta.getStartTimeMs(), meta.getEndTimeMs(), consuming);
    }
  }

  // ---------------------------------------------------------------------------
  // Inner class: NoOpListener
  // ---------------------------------------------------------------------------

  /// No-op listener returned when the manager is disabled.
  private static final class NoOpListener implements SegmentZkMetadataFetchListener {
    static final NoOpListener INSTANCE = new NoOpListener();

    private NoOpListener() {
    }

    @Override
    public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
        List<ZNRecord> znRecords) {
    }

    @Override
    public void onAssignmentChange(IdealState idealState, ExternalView externalView,
        Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    }

    @Override
    public void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    }
  }
}
