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
package org.apache.pinot.core.data.manager.offline;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/// Table data manager for OFFLINE table.
///
/// OFFLINE upsert tables have no consuming segment whose commit takes the upsert snapshot and removes the primary keys
/// out of metadata TTL, as REALTIME tables do. Instead, both are done in the background after a segment is added or
/// replaced, once all the segments of its partition assigned to this server are loaded. Each partition runs one such
/// task at a time, as a REALTIME partition does on its consuming thread.
@ThreadSafe
public class OfflineTableDataManager extends BaseTableDataManager {
  // States of the upsert snapshot task of a partition
  private static final int SNAPSHOT_IDLE = 0;
  private static final int SNAPSHOT_SCHEDULED = 1;
  private static final int SNAPSHOT_RERUN = 2;

  private final Map<Integer, AtomicInteger> _upsertSnapshotStates = new ConcurrentHashMap<>();

  @Override
  protected void doInit() {
    Pair<TableConfig, Schema> tableConfigAndSchema = getCachedTableConfigAndSchema();
    TableConfig tableConfig = tableConfigAndSchema.getLeft();
    Schema schema = tableConfigAndSchema.getRight();
    if (tableConfig.isUpsertEnabled()) {
      _tableUpsertMetadataManager =
          TableUpsertMetadataManagerFactory.create(_instanceDataManagerConfig.getUpsertConfig(), tableConfig, schema,
              this, _segmentOperationsThrottlerSet);
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    if (_tableUpsertMetadataManager != null) {
      _tableUpsertMetadataManager.stop();
    }
    releaseAndRemoveAllSegments();
    if (_tableUpsertMetadataManager != null) {
      try {
        _tableUpsertMetadataManager.close();
      } catch (IOException e) {
        _logger.warn("Caught exception while closing upsert metadata manager", e);
      }
    }
  }

  protected void doAddOnlineSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    IndexLoadingConfig indexLoadingConfig = getCachedIndexLoadingConfig().withSegmentTier(zkMetadata.getTier());
    handleUpsertPreload(zkMetadata, indexLoadingConfig);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager == null) {
      addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    } else {
      replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
    }
  }

  @Override
  protected void doAddSegment(ImmutableSegment immutableSegment, @Nullable SegmentZKMetadata zkMetadata) {
    if (isUpsertEnabled()) {
      handleUpsert(immutableSegment, zkMetadata);
      return;
    }
    super.doAddSegment(immutableSegment, zkMetadata);
  }

  /// Schedules the upsert snapshot and the removal of expired primary keys for the partition, and returns without
  /// waiting so the segment state transition is not slowed down. A partition has at most one task at a time: adds while
  /// it is scheduled or running make it run once more, so a burst of adds takes at most two runs.
  @Override
  protected void onUpsertSegmentAdded(int partitionId, PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    AtomicInteger state = _upsertSnapshotStates.computeIfAbsent(partitionId, k -> new AtomicInteger(SNAPSHOT_IDLE));
    if (state.getAndUpdate(s -> s == SNAPSHOT_IDLE ? SNAPSHOT_SCHEDULED : SNAPSHOT_RERUN) != SNAPSHOT_IDLE) {
      return;
    }
    try {
      _segmentRefreshExecutor.execute(() -> runUpsertSnapshotTask(partitionId, partitionUpsertMetadataManager, state));
    } catch (RejectedExecutionException e) {
      state.set(SNAPSHOT_IDLE);
      _logger.warn("Failed to schedule upsert snapshot for partition: {}, will retry on the next segment add",
          partitionId, e);
    }
  }

  private void runUpsertSnapshotTask(int partitionId, PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      AtomicInteger state) {
    do {
      // Clear a rerun request before the run, as this run covers the segments added before it
      state.set(SNAPSHOT_SCHEDULED);
      if (isPartitionLoaded(partitionId)) {
        takeSnapshotAndRemoveExpiredPrimaryKeys(partitionUpsertMetadataManager);
      } else {
        // Expected while the partition loads. If it keeps increasing, e.g. with a segment of the partition in ERROR
        // state, the partition takes no snapshot and keeps the primary keys out of metadata TTL.
        _serverMetrics.addMeteredTableValue(_tableNameWithType,
            ServerMeter.UPSERT_SNAPSHOT_SKIPPED_PARTITION_NOT_LOADED, 1L);
      }
    } while (!state.compareAndSet(SNAPSHOT_SCHEDULED, SNAPSHOT_IDLE));
  }

  /// Returns whether every segment of the partition assigned to this server is loaded, so that the snapshot and the
  /// metadata TTL see the whole partition, e.g. not while the server starts or a rebalance moves the partition here.
  /// Otherwise the run is skipped, and the last of those segments to load schedules the task again.
  @VisibleForTesting
  boolean isPartitionLoaded(int partitionId) {
    try {
      return isReady((snapshot, logUnavailableSegments) -> snapshot.isPartitionLoaded(partitionId,
          this::isPartitionMetadataInitialized, logUnavailableSegments));
    } catch (Exception e) {
      // Helix and its ZooKeeper client fail with several exception types. The next segment add checks again.
      _logger.warn("Failed to check whether partition: {} is loaded, skipping upsert snapshot", partitionId, e);
      return false;
    }
  }

  /// Takes the upsert snapshot and removes the expired primary keys in the same order as a REALTIME segment commit (see
  /// RealtimeSegmentDataManager). Both are no-op when snapshot or TTL is disabled.
  private void takeSnapshotAndRemoveExpiredPrimaryKeys(PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    try {
      if (partitionUpsertMetadataManager.getContext().getMetadataTTL() > 0) {
        // Take the snapshot first to persist the final state of the keys before they are removed for metadata TTL
        partitionUpsertMetadataManager.takeSnapshot();
        partitionUpsertMetadataManager.removeExpiredPrimaryKeys();
      } else {
        // Remove the deleted keys out of TTL first so that the snapshot reflects their removed valid docs right away,
        // instead of on the next segment upload
        partitionUpsertMetadataManager.removeExpiredPrimaryKeys();
        partitionUpsertMetadataManager.takeSnapshot();
      }
    } catch (Exception e) {
      _logger.warn("Caught exception while taking upsert snapshot or removing expired primary keys", e);
    }
  }

  @Override
  public void addConsumingSegment(String segmentName) {
    throw new UnsupportedOperationException("Cannot add CONSUMING segment to OFFLINE table");
  }
}
