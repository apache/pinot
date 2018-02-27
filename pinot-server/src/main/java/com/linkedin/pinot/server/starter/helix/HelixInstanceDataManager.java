/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.starter.helix;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>HelixInstanceDataManager</code> is the instance data manager based on Helix.
 *
 * TODO: add locks and remove synchronized to add segments in parallel
 * TODO: move SegmentFetcherAndLoader into this class to make this the top level manager
 */
public class HelixInstanceDataManager implements InstanceDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManager.class);

  private final Map<String, TableDataManager> _tableDataManagerMap = new HashMap<>();

  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private String _instanceId;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private ServerMetrics _serverMetrics;
  private boolean _started = false;

  @Override
  public synchronized void init(Configuration config, ZkHelixPropertyStore<ZNRecord> propertyStore,
      ServerMetrics serverMetrics) {
    try {
      _instanceDataManagerConfig = new HelixInstanceDataManagerConfig(config);
      _instanceId = _instanceDataManagerConfig.getInstanceId();
      _propertyStore = propertyStore;
      _serverMetrics = serverMetrics;

      LOGGER.info("InstanceDataManager Config:" + _instanceDataManagerConfig.toString());
      File instanceDataDir = new File(_instanceDataManagerConfig.getInstanceDataDir());
      if (!instanceDataDir.exists()) {
        Preconditions.checkState(instanceDataDir.mkdirs());
      }
      File instanceSegmentTarDir = new File(_instanceDataManagerConfig.getInstanceSegmentTarDir());
      if (!instanceSegmentTarDir.exists()) {
        Preconditions.checkState(instanceSegmentTarDir.mkdirs());
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while initializing Helix instance data manager", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void start() {
    LOGGER.info("Starting Helix instance data manager");
    if (_started) {
      LOGGER.info("Helix instance data manager is already started");
      return;
    }

    // Nothing to be done

    _started = true;
    LOGGER.info("Finish starting Helix instance data manager");
  }

  @Override
  public synchronized void shutDown() {
    LOGGER.info("Shutting down Helix instance data manager");
    if (!_started) {
      LOGGER.info("Helix instance data manager is not running");
      return;
    }

    for (TableDataManager tableDataManager : getTableDataManagers()) {
      tableDataManager.shutDown();
    }

    _started = false;
    LOGGER.info("Finish shutting down Helix instance data manager");
  }

  @Override
  public synchronized void addOfflineSegment(@Nonnull String offlineTableName, @Nonnull String segmentName,
      @Nonnull File indexDir) throws Exception {
    LOGGER.info("Adding segment: {} to OFFLINE table: {}", segmentName, offlineTableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, offlineTableName);
    Preconditions.checkNotNull(tableConfig);
    if (!_tableDataManagerMap.containsKey(offlineTableName)) {
      addTable(offlineTableName, tableConfig);
    }

    _tableDataManagerMap.get(offlineTableName)
        .addSegment(indexDir, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig));
    LOGGER.info("Added segment: {} to OFFLINE table: {}", segmentName, offlineTableName);
  }

  @Override
  public synchronized void addRealtimeSegment(@Nonnull String realtimeTableName, @Nonnull String segmentName)
      throws Exception {
    LOGGER.info("Adding segment: {} to REALTIME table: {}", segmentName, realtimeTableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, realtimeTableName);
    Preconditions.checkNotNull(tableConfig);
    if (!_tableDataManagerMap.containsKey(realtimeTableName)) {
      addTable(realtimeTableName, tableConfig);
    }

    _tableDataManagerMap.get(realtimeTableName)
        .addSegment(segmentName, tableConfig, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig));
    LOGGER.info("Added segment: {} to REALTIME table: {}", segmentName, realtimeTableName);
  }

  private void addTable(@Nonnull String tableNameWithType, @Nonnull TableConfig tableConfig)
      throws ConfigurationException {
    LOGGER.info("Adding table: {}", tableNameWithType);
    TableDataManagerConfig tableDataManagerConfig =
        TableDataManagerConfig.getDefaultHelixTableDataManagerConfig(_instanceDataManagerConfig, tableNameWithType);
    tableDataManagerConfig.overrideConfigs(tableConfig);
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, _instanceId, _propertyStore,
            _serverMetrics);
    tableDataManager.start();
    _tableDataManagerMap.put(tableNameWithType, tableDataManager);
    LOGGER.info("Added table: {}", tableNameWithType);
  }

  @Override
  public synchronized void removeSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName)
      throws Exception {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      tableDataManager.removeSegment(segmentName);
    }
  }

  @Override
  public synchronized void reloadSegment(@Nonnull String tableNameWithType, @Nonnull String segmentName)
      throws Exception {
    SegmentMetadata segmentMetadata = getSegmentMetadata(tableNameWithType, segmentName);
    if (segmentMetadata == null) {
      LOGGER.warn("Cannot locate segment: {} in table: {]", segmentName, tableNameWithType);
      return;
    }

    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);

    Schema schema = null;
    // For OFFLINE table, try to get schema for default columns
    if (TableNameBuilder.OFFLINE.tableHasTypeSuffix(tableNameWithType)) {
      schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    }

    reloadSegment(tableNameWithType, segmentMetadata, tableConfig, schema);
  }

  @Override
  public synchronized void reloadAllSegments(@Nonnull String tableNameWithType) throws Exception {
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);

    Schema schema = null;
    // For OFFLINE table, try to get schema for default columns
    if (TableNameBuilder.OFFLINE.tableHasTypeSuffix(tableNameWithType)) {
      schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    }

    for (SegmentMetadata segmentMetadata : getAllSegmentsMetadata(tableNameWithType)) {
      reloadSegment(tableNameWithType, segmentMetadata, tableConfig, schema);
    }
  }

  private void reloadSegment(@Nonnull String tableNameWithType, @Nonnull SegmentMetadata segmentMetadata,
      @Nonnull TableConfig tableConfig, @Nullable Schema schema) throws Exception {
    String segmentName = segmentMetadata.getName();
    LOGGER.info("Reloading segment: {} in table: {}", segmentName, tableNameWithType);

    String indexDirString = segmentMetadata.getIndexDir();
    if (indexDirString == null) {
      LOGGER.info("Skip reloading REALTIME consuming segment: {} in table: {}", segmentName, tableNameWithType);
      return;
    }

    File indexDir = new File(indexDirString);
    Preconditions.checkState(indexDir.isDirectory(), "Index directory: %s is not a directory", indexDir);

    File parentFile = indexDir.getParentFile();
    File segmentBackupDir =
        new File(parentFile, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    try {
      // First rename index directory to segment backup directory so that original segment have all file descriptors
      // point to the segment backup directory to ensure original segment serves queries properly

      // Rename index directory to segment backup directory (atomic)
      Preconditions.checkState(indexDir.renameTo(segmentBackupDir),
          "Failed to rename index directory: %s to segment backup directory: %s", indexDir, segmentBackupDir);

      // Copy from segment backup directory back to index directory
      FileUtils.copyDirectory(segmentBackupDir, indexDir);

      // Load from index directory
      IndexSegment indexSegment =
          ColumnarSegmentLoader.load(indexDir, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig), schema);

      // Replace the old segment in memory
      _tableDataManagerMap.get(tableNameWithType).addSegment(indexSegment);

      // Rename segment backup directory to segment temporary directory (atomic)
      // The reason to first rename then delete is that, renaming is an atomic operation, but deleting is not. When we
      // rename the segment backup directory to segment temporary directory, we know the reload already succeeded, so
      // that we can safely delete the segment temporary directory
      File segmentTempDir = new File(parentFile, indexDir.getName() + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);
      Preconditions.checkState(segmentBackupDir.renameTo(segmentTempDir),
          "Failed to rename segment backup directory: %s to segment temporary directory: %s", segmentBackupDir,
          segmentTempDir);
      LOGGER.info("Reloaded segment: {} in table: {}", segmentName, tableNameWithType);

      // Delete segment temporary directory
      FileUtils.deleteDirectory(segmentTempDir);
    } finally {
      LoaderUtils.reloadFailureRecovery(indexDir);
    }
  }

  @Nonnull
  @Override
  public Collection<TableDataManager> getTableDataManagers() {
    return _tableDataManagerMap.values();
  }

  @Nullable
  @Override
  public TableDataManager getTableDataManager(String tableName) {
    return _tableDataManagerMap.get(tableName);
  }

  @Nonnull
  @Override
  public String getSegmentDataDirectory() {
    return _instanceDataManagerConfig.getInstanceDataDir();
  }

  @Nonnull
  @Override
  public String getSegmentFileDirectory() {
    return _instanceDataManagerConfig.getInstanceSegmentTarDir();
  }

  @Override
  public int getMaxParallelRefreshThreads() {
    return _instanceDataManagerConfig.getMaxParallelRefreshThreads();
  }

  @Nullable
  @Override
  public SegmentMetadata getSegmentMetadata(@Nonnull String tableNameWithType, @Nonnull String segmentName) {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      SegmentDataManager segmentDataManager = null;
      try {
        segmentDataManager = tableDataManager.acquireSegment(segmentName);
        if (segmentDataManager != null) {
          return segmentDataManager.getSegment().getSegmentMetadata();
        }
      } finally {
        if (segmentDataManager != null) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return null;
  }

  @Nonnull
  @Override
  public List<SegmentMetadata> getAllSegmentsMetadata(@Nonnull String tableNameWithType) {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      return Collections.emptyList();
    } else {
      ImmutableList<SegmentDataManager> segmentDataManagers = null;
      try {
        segmentDataManagers = tableDataManager.acquireAllSegments();
        List<SegmentMetadata> segmentsMetadata = new ArrayList<>(segmentDataManagers.size());
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          segmentsMetadata.add(segmentDataManager.getSegment().getSegmentMetadata());
        }
        return segmentsMetadata;
      } finally {
        if (segmentDataManagers != null) {
          for (SegmentDataManager segmentDataManager : segmentDataManagers) {
            tableDataManager.releaseSegment(segmentDataManager);
          }
        }
      }
    }
  }
}
