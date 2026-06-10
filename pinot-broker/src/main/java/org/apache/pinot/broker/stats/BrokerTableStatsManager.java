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
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker-wide singleton that owns one {@link StatsStore} and produces per-table
 * {@link SegmentZkMetadataFetchListener} instances that populate it.
 *
 * <h3>Usage</h3>
 * <ol>
 *   <li>Construct with a pre-created (but not yet init()d) {@link StatsStore}.</li>
 *   <li>Call {@link #init()} — on failure the manager disables itself; broker startup is not
 *       affected.</li>
 *   <li>For each table, call {@link #createListener(String)} and register the result on that
 *       table's {@link org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher}
 *       <em>before</em> the fetcher's own {@code init()} call.</li>
 *   <li>On table removal, call {@link #onTableRemoved(String)}.</li>
 *   <li>Close the manager when the broker shuts down.</li>
 * </ol>
 *
 * <h3>Thread-safety</h3>
 * <p>Read methods ({@link #getTableStats}, {@link #estimateRowsInTimeRange}) are safe for
 * concurrent access by any number of reader threads. {@link #createListener} and
 * {@link #onTableRemoved} are called from the routing-manager's table-build thread; the store
 * itself handles single-writer / multi-reader concurrency internally.
 *
 * <h3>Failure isolation</h3>
 * <p>All {@link StatsStoreException} escapes are suppressed here — callers on the query path
 * will receive {@code null} / empty rather than a propagated exception.
 */
public class BrokerTableStatsManager implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerTableStatsManager.class);

  private final StatsStore _statsStore;
  private final LogicalTableStatsResolver _resolver;
  /** False when init() failed; all operations become no-ops in that state. */
  private volatile boolean _enabled = false;

  /**
   * Constructs a new manager backed by the given {@link StatsStore}.
   * The store must not have been opened yet; {@link #init()} will call {@link StatsStore#init()}.
   *
   * @param statsStore backing store; owned by this manager
   */
  public BrokerTableStatsManager(StatsStore statsStore) {
    _statsStore = statsStore;
    _resolver = new LogicalTableStatsResolver(statsStore);
  }

  /**
   * Sets the provider used to look up the time boundary (epoch-milliseconds) for a raw table
   * name. Call this after the routing manager is fully initialized.
   *
   * <p>A {@code null} return value from {@code provider} means no boundary is available for
   * that table, which causes the resolver to fall back to a plain sum of offline + realtime rows
   * with {@link org.apache.pinot.query.planner.spi.stats.StatConfidence#ESTIMATED} confidence.
   *
   * @param provider function from raw table name → time boundary in epoch-milliseconds, nullable
   */
  public void setTimeBoundaryMsProvider(@Nullable Function<String, Long> provider) {
    _resolver.setTimeBoundaryMsProvider(provider);
  }

  /**
   * Sets the provider used to look up the {@link TableConfig} for a fully-qualified
   * (type-suffixed) table name. Call this after the table cache is initialized.
   *
   * <p>Required for upsert/dedup detection; without this, upsert/dedup tables will report
   * {@link org.apache.pinot.query.planner.spi.stats.StatConfidence#EXACT} rather than
   * {@link org.apache.pinot.query.planner.spi.stats.StatConfidence#LOW}.
   *
   * @param provider function from suffixed table name → TableConfig, nullable
   */
  public void setTableConfigProvider(@Nullable Function<String, TableConfig> provider) {
    _resolver.setTableConfigProvider(provider);
  }

  /**
   * Opens the backing store. On failure, logs an error and sets the manager to disabled; the
   * broker should still start normally.
   *
   * @throws StatsStoreException if the store cannot be opened (callers may log and ignore)
   */
  public void init()
      throws StatsStoreException {
    _statsStore.init();
    _enabled = true;
    LOGGER.info("BrokerTableStatsManager initialized");
  }

  /**
   * Creates a listener that will maintain stats for {@code tableNameWithType} in the backing
   * store. Must be registered on the table's
   * {@link org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher} before the
   * fetcher is initialized.
   *
   * <p>If the manager is disabled (init failed), returns a no-op listener.
   *
   * @param tableNameWithType fully-qualified table name (e.g. {@code myTable_OFFLINE})
   * @return a new listener instance for that table
   */
  public SegmentZkMetadataFetchListener createListener(String tableNameWithType) {
    if (!_enabled) {
      return NoOpListener.INSTANCE;
    }
    return new TableStatsZkListener(tableNameWithType, _statsStore);
  }

  /**
   * Removes all persisted stats for the given table. Called when the routing entry for a table
   * is removed. Any store error is logged at WARN and ignored.
   *
   * @param tableNameWithType fully-qualified table name
   */
  public void onTableRemoved(String tableNameWithType) {
    if (!_enabled) {
      return;
    }
    try {
      _statsStore.purgeTable(tableNameWithType);
    } catch (StatsStoreException e) {
      LOGGER.warn("Failed to purge stats for table {}: {}", tableNameWithType, e.getMessage());
    }
  }

  /**
   * Returns logical table statistics for the given table name, or {@code null} if unavailable.
   *
   * <p>Accepts both suffixed physical names ({@code foo_OFFLINE} / {@code foo_REALTIME}) and raw
   * logical names ({@code foo}):
   * <ul>
   *   <li>Suffixed names: returns physical stats with per-type confidence adjustments (upsert,
   *       dedup, consuming-segment detection).</li>
   *   <li>Raw names: returns a logical hybrid view merging offline and realtime stats at the
   *       time boundary; if no boundary is available, returns a plain sum with
   *       {@link org.apache.pinot.query.planner.spi.stats.StatConfidence#ESTIMATED}
   *       confidence.</li>
   * </ul>
   *
   * <p>Any store error is logged at WARN and {@code null} is returned.
   *
   * @param tableName raw table name or fully-qualified name with type suffix
   */
  @Nullable
  public TableStatistics getTableStats(String tableName) {
    if (!_enabled) {
      return null;
    }
    return _resolver.getTableStats(tableName);
  }

  /**
   * Returns an estimate of the number of rows in the given time range, or an empty optional if
   * unavailable. Any store error is logged at WARN and an empty optional is returned.
   *
   * <p>For hybrid (raw) table names, the estimate is split at the time boundary:
   * offline rows are counted for {@code [startMs, boundary)} and realtime rows for
   * {@code [boundary, endMs)}.
   *
   * @param tableName raw table name or fully-qualified name with type suffix
   * @param startMs   inclusive range start in epoch milliseconds
   * @param endMs     exclusive range end in epoch milliseconds
   */
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

  /**
   * {@link SegmentZkMetadataFetchListener} that maintains segment-level statistics for a single
   * table in a {@link StatsStore}.
   *
   * <h3>Thread-safety</h3>
   * <p>Instances are called sequentially from the routing manager's per-table lock, so no
   * additional synchronization is needed inside this class.
   *
   * <h3>Failure isolation</h3>
   * <p>All {@link StatsStoreException} are caught; errors are logged at WARN and the listener
   * never throws back into the routing manager.
   */
  static final class TableStatsZkListener implements SegmentZkMetadataFetchListener {
    private static final Logger LOG = LoggerFactory.getLogger(TableStatsZkListener.class);

    private final String _tableNameWithType;
    private final StatsStore _statsStore;
    /**
     * In-memory mirror of the segments currently persisted in the store for this table.
     * Maintained after {@link #init} so that {@link #onAssignmentChange} can compute
     * removals without a full DB round-trip.
     * <p>Accessed only from the routing-manager's per-table lock — no additional
     * synchronization needed.
     */
    private final Set<String> _persistedSegments = new HashSet<>();

    TableStatsZkListener(String tableNameWithType, StatsStore statsStore) {
      _tableNameWithType = tableNameWithType;
      _statsStore = statsStore;
    }

    @Override
    public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
        List<ZNRecord> znRecords) {
      // Restart reconciliation: read stored CRCs, upsert changed/new, remove dropped segments.
      Map<String, Long> storedCrcs;
      try {
        storedCrcs = _statsStore.getSegmentCrcs(_tableNameWithType);
      } catch (StatsStoreException e) {
        LOG.warn("Failed to read stored CRCs for {} during init; will upsert all segments: {}",
            _tableNameWithType, e.getMessage());
        storedCrcs = Map.of();
      }

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
            _persistedSegments.add(row.getSegmentName());
          }
        } catch (StatsStoreException e) {
          LOG.warn("Failed to upsert segment stats for {} during init: {}", _tableNameWithType,
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
          LOG.warn("Failed to remove stale segments for {} during init: {}", _tableNameWithType,
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
              _persistedSegments.add(row.getSegmentName());
            }
          } catch (StatsStoreException e) {
            LOG.warn("Failed to upsert segment stats for {} on assignment change: {}",
                _tableNameWithType, e.getMessage());
          }
        }
      }

      // Remove persisted segments that are no longer online.
      // Use the in-memory mirror to avoid a full DB round-trip.
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
          LOG.warn("Failed to remove dropped segments for {} on assignment change: {}",
              _tableNameWithType, e.getMessage());
        }
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
          LOG.warn("Failed to remove segment {} for {} on refresh: {}", segment,
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
        LOG.warn("Failed to upsert segment {} for {} on refresh: {}", segment, _tableNameWithType,
            e.getMessage());
      }
    }

    /**
     * Converts a {@link SegmentZKMetadata} into a {@link SegmentStatsRow}.
     * A segment is considered consuming when it is a realtime segment whose status is
     * {@link Status#IN_PROGRESS}.
     * For non-consuming segments, negative totalDocs is stored as 0.
     */
    private static SegmentStatsRow buildRow(String segmentName, SegmentZKMetadata meta) {
      boolean consuming = meta.getStatus() == Status.IN_PROGRESS;
      long totalDocs = meta.getTotalDocs();
      if (!consuming && totalDocs < 0) {
        LOG.debug("Segment {} has negative totalDocs ({}); storing 0", segmentName, totalDocs);
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

  /** No-op listener returned when the manager is disabled. */
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
