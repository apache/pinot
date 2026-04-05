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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Table data manager for OFFLINE table.
 */
@ThreadSafe
public class OfflineTableDataManager extends BaseTableDataManager {

  private final BooleanSupplier _isServerReadyToServeQueries;
  private OfflineFreshnessTracker _freshnessTracker;

  public OfflineTableDataManager() {
    this(() -> true);
  }

  public OfflineTableDataManager(BooleanSupplier isServerReadyToServeQueries) {
    _isServerReadyToServeQueries = isServerReadyToServeQueries;
  }

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
    _freshnessTracker =
        new OfflineFreshnessTracker(_serverMetrics, _tableNameWithType, _isServerReadyToServeQueries);
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    if (_freshnessTracker != null) {
      _freshnessTracker.shutdown();
    }
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
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager == null) {
      addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    } else {
      replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
    }

    // Track freshness after segment is loaded
    if (_freshnessTracker != null) {
      trackSegmentFreshness(segmentName, zkMetadata);
    }
  }

  @Override
  protected void doOffloadSegment(String segmentName) {
    if (_freshnessTracker != null) {
      _freshnessTracker.segmentRemoved(segmentName);
    }
    super.doOffloadSegment(segmentName);
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment, @Nullable SegmentZKMetadata zkMetadata) {
    String segmentName = immutableSegment.getSegmentName();
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot add segment: %s to table: %s",
        segmentName, _tableNameWithType);
    if (isUpsertEnabled()) {
      handleUpsert(immutableSegment, zkMetadata);
      return;
    }
    super.addSegment(immutableSegment, zkMetadata);
  }

  @Override
  public void addConsumingSegment(String segmentName) {
    throw new UnsupportedOperationException("Cannot add CONSUMING segment to OFFLINE table");
  }

  private void trackSegmentFreshness(String segmentName, SegmentZKMetadata zkMetadata) {
    try {
      SegmentDataManager sdm = _segmentDataManagerMap.get(segmentName);
      if (sdm == null) {
        return;
      }
      SegmentMetadata meta = sdm.getSegment().getSegmentMetadata();
      TimeUnit timeUnit = meta.getTimeUnit();
      if (timeUnit == null) {
        return;
      }
      long endTimeMs = timeUnit.toMillis(meta.getEndTime());

      int partitionId;
      if (zkMetadata == null || zkMetadata.getPartitionMetadata() == null) {
        // No partition metadata — treat as non-partitioned
        partitionId = OfflineFreshnessTracker.NON_PARTITIONED_SENTINEL;
      } else {
        // getSegmentPartitionId returns null when partition ID is ambiguous (e.g. multi-partition segment)
        Integer derivedPartitionId = SegmentUtils.getSegmentPartitionId(zkMetadata, null);
        if (derivedPartitionId == null) {
          // Skip tracking for ambiguous segments to avoid polluting the non-partitioned bucket
          return;
        }
        partitionId = derivedPartitionId;
      }
      _freshnessTracker.segmentLoaded(segmentName, endTimeMs, partitionId);
    } catch (Exception e) {
      _logger.warn("Failed to track freshness for segment: {}", segmentName, e);
    }
  }
}
