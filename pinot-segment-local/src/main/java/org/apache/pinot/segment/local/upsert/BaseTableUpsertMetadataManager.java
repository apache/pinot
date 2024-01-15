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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
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

  protected String _tableNameWithType;
  protected TableDataManager _tableDataManager;
  protected HelixManager _helixManager;
  protected ExecutorService _segmentPreloadExecutor;
  protected UpsertContext _context;

  private volatile boolean _isPreloading = false;

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager, HelixManager helixManager,
      @Nullable ExecutorService segmentPreloadExecutor) {
    _tableNameWithType = tableConfig.getTableName();
    _tableDataManager = tableDataManager;
    _helixManager = helixManager;
    _segmentPreloadExecutor = segmentPreloadExecutor;

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkArgument(upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE,
        "Upsert must be enabled for table: %s", _tableNameWithType);

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(primaryKeyColumns),
        "Primary key columns must be configured for upsert enabled table: %s", _tableNameWithType);

    List<String> comparisonColumns = upsertConfig.getComparisonColumns();
    if (comparisonColumns == null) {
      comparisonColumns = Collections.singletonList(tableConfig.getValidationConfig().getTimeColumnName());
    }

    PartialUpsertHandler partialUpsertHandler = null;
    if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
      Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();
      Preconditions.checkArgument(partialUpsertStrategies != null,
          "Partial-upsert strategies must be configured for partial-upsert enabled table: %s", _tableNameWithType);
      partialUpsertHandler =
          new PartialUpsertHandler(schema, partialUpsertStrategies, upsertConfig.getDefaultPartialUpsertStrategy(),
              comparisonColumns);
    }

    String deleteRecordColumn = upsertConfig.getDeleteRecordColumn();
    HashFunction hashFunction = upsertConfig.getHashFunction();
    boolean enableSnapshot = upsertConfig.isEnableSnapshot();
    boolean enablePreload = upsertConfig.isEnablePreload();
    double metadataTTL = upsertConfig.getMetadataTTL();
    double deletedKeysTTL = upsertConfig.getDeletedKeysTTL();
    File tableIndexDir = tableDataManager.getTableDataDir();

    // Server level config honoured only when table level config is not set to true
    if (tableDataManager.getTableDataManagerConfig() != null
        && tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig() != null
        && tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getUpsertDefaultEnableSnapshot()
        != null && !enableSnapshot) {
      enableSnapshot = Boolean.parseBoolean(
          tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getUpsertDefaultEnableSnapshot());
    }

    // Server level config honoured only when table level config is not set to true
    if (tableDataManager.getTableDataManagerConfig() != null
        && tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig() != null
        && tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getUpsertDefaultEnablePreload()
        != null && !enablePreload) {
      enablePreload = Boolean.parseBoolean(
          tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig().getUpsertDefaultEnablePreload());
    }

    _context = new UpsertContext.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setPrimaryKeyColumns(primaryKeyColumns).setComparisonColumns(comparisonColumns)
        .setDeleteRecordColumn(deleteRecordColumn).setHashFunction(hashFunction)
        .setPartialUpsertHandler(partialUpsertHandler).setEnableSnapshot(enableSnapshot).setEnablePreload(enablePreload)
        .setMetadataTTL(metadataTTL).setDeletedKeysTTL(deletedKeysTTL).setTableIndexDir(tableIndexDir).build();
    LOGGER.info(
        "Initialized {} for table: {} with primary key columns: {}, comparison columns: {}, delete record column: {},"
            + " hash function: {}, upsert mode: {}, enable snapshot: {}, enable preload: {}, metadata TTL: {},"
            + " deleted Keys TTL: {}, table index dir: {}", getClass().getSimpleName(), _tableNameWithType,
        primaryKeyColumns, comparisonColumns, deleteRecordColumn, hashFunction, upsertConfig.getMode(), enableSnapshot,
        enablePreload, metadataTTL, deletedKeysTTL, tableIndexDir);

    initCustomVariables();

    if (enableSnapshot && enablePreload && segmentPreloadExecutor != null) {

      // Preloading the segments with snapshots for fast upsert metadata recovery.
      // Note that there is an implicit waiting logic between the thread doing the segment preloading here and the
      // other helix threads about to process segment state transitions (e.g. taking segments from OFFLINE to ONLINE).
      // The thread doing the segment preloading here must complete before the other helix threads start to handle
      // segment state transitions. This is ensured implicitly because segment preloading happens here when
      // initializing this TableUpsertMetadataManager, which happens when initializing the TableDataManager, which
      // happens as the lambda of ConcurrentHashMap.computeIfAbsent() method, which ensures the waiting logic.
      try {
        _isPreloading = true;
        preloadSegments();
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
   * Can be overridden to initialize custom variables after other variables are set but before preload starts. This is
   * needed because preload will load segments which might require these custom variables.
   */
  protected void initCustomVariables() {
  }

  /**
   * Get the ideal state and find segments assigned to current instance, then preload those with validDocIds snapshot.
   * Skip those without the snapshots and those whose crc has changed, as they will be handled by normal Helix state
   * transitions, which will proceed after the preloading phase fully completes.
   */
  private void preloadSegments()
      throws Exception {
    LOGGER.info("Preload segments from table: {} for fast upsert metadata recovery", _tableNameWithType);
    if (!onPreloadStart()) {
      return;
    }
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixManager.getHelixPropertyStore();
    String instanceId = getInstanceId();
    IndexLoadingConfig indexLoadingConfig = createIndexLoadingConfig();
    Map<String, Map<String, String>> segmentAssignment = getSegmentAssignment();
    List<Future<?>> futures = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      String state = instanceStateMap.get(instanceId);
      if (!CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)) {
        LOGGER.info("Skip segment: {} as its ideal state: {} is not ONLINE", segmentName, state);
        continue;
      }
      futures.add(_segmentPreloadExecutor.submit(() -> {
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
    onPreloadFinish();
    LOGGER.info("Preloaded segments from table: {} for fast upsert metadata recovery", _tableNameWithType);
  }

  /**
   * Can be overridden to perform operations before preload starts.
   *
   * @return whether to continue the preloading logic.
   */
  protected boolean onPreloadStart() {
    return true;
  }

  /**
   * Can be overridden to perform operations after preload is done.
   */
  protected void onPreloadFinish() {
  }

  @VisibleForTesting
  String getInstanceId() {
    InstanceDataManagerConfig instanceDataManagerConfig =
        _tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig();
    return instanceDataManagerConfig.getInstanceId();
  }

  @VisibleForTesting
  IndexLoadingConfig createIndexLoadingConfig() {
    return new IndexLoadingConfig(_tableDataManager.getTableDataManagerConfig().getInstanceDataManagerConfig(),
        _context.getTableConfig(), _context.getSchema());
  }

  @VisibleForTesting
  Map<String, Map<String, String>> getSegmentAssignment() {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, _tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", _tableNameWithType);
    return idealState.getRecord().getMapFields();
  }

  private void preloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    LOGGER.info("Preload segment: {} from table: {}", segmentName, _tableNameWithType);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(propertyStore, _tableNameWithType, segmentName);
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for segment: %s, table: %s", segmentName,
        _tableNameWithType);
    if (!hasValidDocIdsSnapshot(segmentName, zkMetadata.getTier())) {
      LOGGER.info("Skip segment: {} as no validDocIds snapshot exists", segmentName);
      return;
    }
    preloadSegmentWithSnapshot(segmentName, indexLoadingConfig, zkMetadata);
    LOGGER.info("Preloaded segment: {} from table: {}", segmentName, _tableNameWithType);
  }

  @VisibleForTesting
  void preloadSegmentWithSnapshot(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata) {
    // This method checks segment crc and if it has changed, the segment is not loaded. It might modify the file on
    // disk, but we don't need to take the segmentLock, because every segment from the current table is processed by
    // at most one thread from the preloading thread pool. HelixTaskExecutor task threads about to process segments
    // from the same table are blocked on ConcurrentHashMap lock as in HelixInstanceDataManager.addRealtimeSegment().
    // In fact, taking segmentLock during segment preloading phase could cause deadlock when HelixTaskExecutor
    // threads processing other tables have taken the same segmentLock as decided by the hash of table name and
    // segment name, i.e. due to hash collision.
    _tableDataManager.tryLoadExistingSegment(segmentName, indexLoadingConfig, zkMetadata);
  }

  private boolean hasValidDocIdsSnapshot(String segmentName, String segmentTier) {
    try {
      File indexDir = _tableDataManager.getSegmentDataDir(segmentName, segmentTier, _context.getTableConfig());
      File snapshotFile =
          new File(SegmentDirectoryPaths.findSegmentDirectory(indexDir), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
      return snapshotFile.exists();
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean isPreloading() {
    return _isPreloading;
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _context.getPartialUpsertHandler() == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }
}
