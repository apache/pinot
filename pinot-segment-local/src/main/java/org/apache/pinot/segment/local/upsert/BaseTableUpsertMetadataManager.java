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
package org.apache.pinot.segment.local.upsert;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseTableUpsertMetadataManager implements TableUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTableUpsertMetadataManager.class);
  private TableConfig _tableConfig;
  private Schema _schema;
  private TableDataManager _tableDataManager;
  protected String _tableNameWithType;
  protected List<String> _primaryKeyColumns;
  protected List<String> _comparisonColumns;
  protected String _deleteRecordColumn;
  protected HashFunction _hashFunction;
  protected PartialUpsertHandler _partialUpsertHandler;
  protected boolean _enableSnapshot;
  protected double _metadataTTL;
  protected File _tableIndexDir;
  protected ServerMetrics _serverMetrics;
  private volatile boolean _isPreloading = false;

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager,
      ServerMetrics serverMetrics, HelixManager helixManager, @Nullable ExecutorService segmentPreloadExecutor) {
    _tableConfig = tableConfig;
    _schema = schema;
    _tableDataManager = tableDataManager;
    _tableNameWithType = tableConfig.getTableName();

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkArgument(upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE,
        "Upsert must be enabled for table: %s", _tableNameWithType);

    _primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(_primaryKeyColumns),
        "Primary key columns must be configured for upsert enabled table: %s", _tableNameWithType);

    _comparisonColumns = upsertConfig.getComparisonColumns();
    if (_comparisonColumns == null) {
      _comparisonColumns = Collections.singletonList(tableConfig.getValidationConfig().getTimeColumnName());
    }

    _deleteRecordColumn = upsertConfig.getDeleteRecordColumn();
    _hashFunction = upsertConfig.getHashFunction();

    if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
      Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();
      Preconditions.checkArgument(partialUpsertStrategies != null,
          "Partial-upsert strategies must be configured for partial-upsert enabled table: %s", _tableNameWithType);
      _partialUpsertHandler =
          new PartialUpsertHandler(schema, partialUpsertStrategies, upsertConfig.getDefaultPartialUpsertStrategy(),
              _comparisonColumns);
    }

    _enableSnapshot = upsertConfig.isEnableSnapshot();
    _metadataTTL = upsertConfig.getMetadataTTL();
    _tableIndexDir = tableDataManager.getTableDataDir();
    _serverMetrics = serverMetrics;
    if (_enableSnapshot && segmentPreloadExecutor != null && upsertConfig.isEnablePreload()) {
      // Preloading the segments with snapshots for fast upsert metadata recovery.
      // Note that there is an implicit waiting logic between the thread doing the segment preloading here and the
      // other helix threads about to process segment state transitions (e.g. taking segments from OFFLINE to ONLINE).
      // The thread doing the segment preloading here must complete before the other helix threads start to handle
      // segment state transitions. This is ensured implicitly because segment preloading happens here when
      // initializing this TableUpsertMetadataManager, which happens when initializing the TableDataManager, which
      // happens as the lambda of ConcurrentHashMap.computeIfAbsent() method, which ensures the waiting logic.
      try {
        _isPreloading = true;
        preloadSegments(helixManager, segmentPreloadExecutor);
      } catch (Exception e) {
        // Even if preloading fails, we should continue to complete the initialization, so that TableDataManager can be
        // created. Once TableDataManager is created, no more segment preloading would happen, and the normal segment
        // loading logic would be used. The segments not being preloaded successfully here would be loaded via the
        // normal segment loading logic, the one doing more costly checks on the upsert metadata.
        LOGGER.warn("Failed to preload segments from table: {}, skipping", _tableNameWithType, e);
        if (e instanceof InterruptedException) {
          // Restore the interrupted status in case the upper callers want to check.
          Thread.currentThread().interrupt();
        }
      } finally {
        _isPreloading = false;
      }
    }
  }

  /**
   * Get the ideal state and find segments assigned to current instance, then preload those with validDocIds snapshot.
   * Skip those without the snapshots and those whose crc has changed, as they will be handled by normal Helix state
   * transitions, which will proceed after the preloading phase fully completes.
   */
  private void preloadSegments(HelixManager helixManager, ExecutorService segmentPreloadExecutor)
      throws Exception {
    LOGGER.info("Preload segments from table: {} for fast upsert metadata recovery", _tableNameWithType);
    IdealState idealState = HelixHelper.getTableIdealState(helixManager, _tableNameWithType);
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    String instanceId = getInstanceId();
    IndexLoadingConfig indexLoadingConfig = createIndexLoadingConfig();
    List<Future<?>> futures = new ArrayList<>();
    for (String segmentName : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentName);
      String state = instanceStateMap.get(instanceId);
      if (!CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)) {
        LOGGER.info("Skip segment: {} as its ideal state: {} is not ONLINE", segmentName, state);
        continue;
      }
      futures.add(segmentPreloadExecutor.submit(() -> {
        try {
          preloadSegment(segmentName, indexLoadingConfig, propertyStore);
        } catch (Exception e) {
          LOGGER.warn("Failed to preload segment: {} from table: {}, skipping", segmentName, _tableNameWithType, e);
        }
      }));
    }
    try {
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      for (Future<?> f : futures) {
        if (!f.isDone()) {
          f.cancel(true);
        }
      }
    }
    LOGGER.info("Preloaded segments from table: {} for fast upsert metadata recovery", _tableNameWithType);
  }

  private String getInstanceId() {
    InstanceDataManagerConfig instanceDataManagerConfig =
        _tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig();
    return instanceDataManagerConfig.getInstanceId();
  }

  @VisibleForTesting
  protected IndexLoadingConfig createIndexLoadingConfig() {
    return new IndexLoadingConfig(_tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig(),
        _tableConfig, _schema);
  }

  private void preloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    LOGGER.info("Preload segment: {} from table: {}", segmentName, _tableNameWithType);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(propertyStore, _tableNameWithType, segmentName);
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for segment: %s, table: %s", segmentName,
        _tableNameWithType);
    File snapshotFile = getValidDocIdsSnapshotFile(segmentName, zkMetadata.getTier());
    if (!snapshotFile.exists()) {
      LOGGER.info("Skip segment: {} as no validDocIds snapshot at: {}", segmentName, snapshotFile);
      return;
    }
    preloadSegmentWithSnapshot(segmentName, indexLoadingConfig, zkMetadata);
    LOGGER.info("Preloaded segment: {} from table: {}", segmentName, _tableNameWithType);
  }

  @VisibleForTesting
  protected void preloadSegmentWithSnapshot(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata) {
    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    try {
      segmentLock.lock();
      // This method checks segment crc and if it has changed, the segment is not loaded.
      _tableDataManager.tryLoadExistingSegment(segmentName, indexLoadingConfig, zkMetadata);
    } finally {
      segmentLock.unlock();
    }
  }

  private File getValidDocIdsSnapshotFile(String segmentName, String segmentTier) {
    File indexDir = _tableDataManager.getSegmentDataDir(segmentName, segmentTier, _tableConfig);
    return new File(SegmentDirectoryPaths.findSegmentDirectory(indexDir), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
  }

  @Override
  public boolean isPreloading() {
    return _isPreloading;
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _partialUpsertHandler == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }
}
