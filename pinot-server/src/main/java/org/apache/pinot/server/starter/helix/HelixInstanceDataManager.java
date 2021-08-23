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
package org.apache.pinot.server.starter.helix;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploader;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>HelixInstanceDataManager</code> is the instance data manager based on Helix.
 */
@ThreadSafe
public class HelixInstanceDataManager implements InstanceDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManager.class);

  private final ConcurrentHashMap<String, TableDataManager> _tableDataManagerMap = new ConcurrentHashMap<>();

  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private String _instanceId;
  private HelixManager _helixManager;
  private ServerMetrics _serverMetrics;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private String _authToken;
  private SegmentUploader _segmentUploader;

  // Fixed size LRU cache for storing last N errors on the instance.
  // Key is TableNameWithType-SegmentName pair.
  private LoadingCache<Pair<String, String>, SegmentErrorInfo> _errorCache;

  @Override
  public synchronized void init(PinotConfiguration config, HelixManager helixManager, ServerMetrics serverMetrics)
      throws ConfigurationException {
    LOGGER.info("Initializing Helix instance data manager");

    _instanceDataManagerConfig = new HelixInstanceDataManagerConfig(config);
    LOGGER.info("HelixInstanceDataManagerConfig: {}", _instanceDataManagerConfig);
    _instanceId = _instanceDataManagerConfig.getInstanceId();
    _helixManager = helixManager;
    _serverMetrics = serverMetrics;
    _authToken = config.getProperty(CommonConstants.Server.CONFIG_OF_AUTH_TOKEN);
    _segmentUploader = new PinotFSSegmentUploader(_instanceDataManagerConfig.getSegmentStoreUri(),
        PinotFSSegmentUploader.DEFAULT_SEGMENT_UPLOAD_TIMEOUT_MILLIS);

    File instanceDataDir = new File(_instanceDataManagerConfig.getInstanceDataDir());
    if (!instanceDataDir.exists()) {
      Preconditions.checkState(instanceDataDir.mkdirs());
    }
    File instanceSegmentTarDir = new File(_instanceDataManagerConfig.getInstanceSegmentTarDir());
    if (!instanceSegmentTarDir.exists()) {
      Preconditions.checkState(instanceSegmentTarDir.mkdirs());
    }

    // Initialize segment build time lease extender executor
    SegmentBuildTimeLeaseExtender.initExecutor();

    // Initialize the table data manager provider
    TableDataManagerProvider.init(_instanceDataManagerConfig);
    LOGGER.info("Initialized Helix instance data manager");

    // Initialize the error cache
    _errorCache = CacheBuilder.newBuilder().maximumSize(_instanceDataManagerConfig.getErrorCacheSize())
        .build(new CacheLoader<Pair<String, String>, SegmentErrorInfo>() {
          @Override
          public SegmentErrorInfo load(Pair<String, String> tableNameWithTypeSegmentNamePair) {
            // This cache is populated only via the put api.
            return null;
          }
        });
  }

  @Override
  public synchronized void start() {
    _propertyStore = _helixManager.getHelixPropertyStore();
    LOGGER.info("Helix instance data manager started");
  }

  @Override
  public synchronized void shutDown() {
    for (TableDataManager tableDataManager : _tableDataManagerMap.values()) {
      tableDataManager.shutDown();
    }
    SegmentBuildTimeLeaseExtender.shutdownExecutor();
    LOGGER.info("Helix instance data manager shut down");
  }

  @Override
  public void addOfflineSegment(String offlineTableName, String segmentName, File indexDir)
      throws Exception {
    LOGGER.info("Adding segment: {} to table: {}", segmentName, offlineTableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, offlineTableName);
    Preconditions.checkNotNull(tableConfig);
    _tableDataManagerMap.computeIfAbsent(offlineTableName, k -> createTableDataManager(k, tableConfig))
        .addSegment(indexDir, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig));
    LOGGER.info("Added segment: {} to table: {}", segmentName, offlineTableName);
  }

  @Override
  public void addRealtimeSegment(String realtimeTableName, String segmentName)
      throws Exception {
    LOGGER.info("Adding segment: {} to table: {}", segmentName, realtimeTableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, realtimeTableName);
    Preconditions.checkNotNull(tableConfig);
    _tableDataManagerMap.computeIfAbsent(realtimeTableName, k -> createTableDataManager(k, tableConfig))
        .addSegment(segmentName, tableConfig, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig));
    LOGGER.info("Added segment: {} to table: {}", segmentName, realtimeTableName);
  }

  private TableDataManager createTableDataManager(String tableNameWithType, TableConfig tableConfig) {
    LOGGER.info("Creating table data manager for table: {}", tableNameWithType);
    TableDataManagerConfig tableDataManagerConfig =
        TableDataManagerConfig.getDefaultHelixTableDataManagerConfig(_instanceDataManagerConfig, tableNameWithType);
    tableDataManagerConfig.overrideConfigs(tableConfig, _authToken);
    TableDataManager tableDataManager = TableDataManagerProvider
        .getTableDataManager(tableDataManagerConfig, _instanceId, _propertyStore, _serverMetrics, _helixManager,
            _errorCache);
    tableDataManager.start();
    LOGGER.info("Created table data manager for table: {}", tableNameWithType);
    return tableDataManager;
  }

  @Override
  public void removeSegment(String tableNameWithType, String segmentName) {
    LOGGER.info("Removing segment: {} from table: {}", segmentName, tableNameWithType);
    _tableDataManagerMap.computeIfPresent(tableNameWithType, (k, v) -> {
      v.removeSegment(segmentName);
      LOGGER.info("Removed segment: {} from table: {}", segmentName, k);
      if (v.getNumSegments() == 0) {
        v.shutDown();
        return null;
      } else {
        return v;
      }
    });
  }

  @Override
  public void reloadSegment(String tableNameWithType, String segmentName)
      throws Exception {
    LOGGER.info("Reloading single segment: {} in table: {}", segmentName, tableNameWithType);
    SegmentMetadata segmentMetadata = getSegmentMetadata(tableNameWithType, segmentName);
    if (segmentMetadata == null) {
      LOGGER.info("Segment metadata is null. Skip reloading segment: {} in table: {}", segmentName, tableNameWithType);
      return;
    }

    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);

    reloadSegment(tableNameWithType, segmentMetadata, tableConfig, schema);

    LOGGER.info("Reloaded single segment: {} in table: {}", segmentName, tableNameWithType);
  }

  @Override
  public void reloadAllSegments(String tableNameWithType)
      throws Exception {
    LOGGER.info("Reloading all segments in table: {}", tableNameWithType);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);

    for (SegmentMetadata segmentMetadata : getAllSegmentsMetadata(tableNameWithType)) {
      reloadSegment(tableNameWithType, segmentMetadata, tableConfig, schema);
    }

    LOGGER.info("Reloaded all segments in table: {}", tableNameWithType);
  }

  private void reloadSegment(String tableNameWithType, SegmentMetadata segmentMetadata, TableConfig tableConfig,
      @Nullable Schema schema)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    LOGGER.info("Reloading segment: {} in table: {}", segmentName, tableNameWithType);

    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      LOGGER.warn("Failed to find table data manager for table: {}, skipping reloading segment", tableNameWithType);
      return;
    }

    File indexDir = segmentMetadata.getIndexDir();
    if (indexDir == null) {
      if (!_instanceDataManagerConfig.shouldReloadConsumingSegment()) {
        LOGGER.info("Skip reloading REALTIME consuming segment: {} in table: {}", segmentName, tableNameWithType);
        return;
      }
      Preconditions.checkState(schema != null, "Failed to find schema for table: {}", tableNameWithType);
      LOGGER.info("Try reloading REALTIME consuming segment: {} in table: {}", segmentName, tableNameWithType);
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
      if (segmentDataManager != null) {
        try {
          MutableSegmentImpl mutableSegment = (MutableSegmentImpl) segmentDataManager.getSegment();
          mutableSegment.addExtraColumns(schema);
        } finally {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
      return;
    }

    Preconditions.checkState(indexDir.isDirectory(), "Index directory: %s is not a directory", indexDir);
    File parentFile = indexDir.getParentFile();
    File segmentBackupDir =
        new File(parentFile, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);

    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
    try {
      segmentLock.lock();

      // First rename index directory to segment backup directory so that original segment have all file descriptors
      // point to the segment backup directory to ensure original segment serves queries properly

      // Rename index directory to segment backup directory (atomic)
      Preconditions.checkState(indexDir.renameTo(segmentBackupDir),
          "Failed to rename index directory: %s to segment backup directory: %s", indexDir, segmentBackupDir);

      // Copy from segment backup directory back to index directory
      FileUtils.copyDirectory(segmentBackupDir, indexDir);

      // Load from index directory
      ImmutableSegment immutableSegment = ImmutableSegmentLoader
          .load(indexDir, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig), schema);

      // Replace the old segment in memory
      tableDataManager.addSegment(immutableSegment);

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
    } catch (Exception reloadFailureException) {
      try {
        LoaderUtils.reloadFailureRecovery(indexDir);
      } catch (Exception recoveryFailureException) {
        LOGGER.error("Failed to recover after reload failure", recoveryFailureException);
        reloadFailureException.addSuppressed(recoveryFailureException);
      }
      throw reloadFailureException;
    } finally {
      segmentLock.unlock();
    }
  }

  @Override
  public void addOrReplaceSegment(String tableNameWithType, String segmentName, boolean forceDownload)
      throws Exception {
    LOGGER.info("Adding or replacing segment: {} for table: {} downloadIsForced: {}", segmentName, tableNameWithType,
        forceDownload);

    // Get updated table config and segment metadata from Zookeeper.
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    Preconditions.checkNotNull(zkMetadata);

    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
    try {
      // Lock the segment to get its metadata, so that no other threads are modifying
      // the disk files of this segment.
      segmentLock.lock();
      SegmentMetadata localMetadata = getSegmentMetadata(tableNameWithType, segmentName);

      _tableDataManagerMap.computeIfAbsent(tableNameWithType, k -> createTableDataManager(k, tableConfig))
          .addOrReplaceSegment(segmentName, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig),
              localMetadata, zkMetadata, forceDownload);
      LOGGER.info("Added or replaced segment: {} of table: {}", segmentName, tableNameWithType);
    } finally {
      segmentLock.unlock();
    }
  }

  @Override
  public void addOrReplaceAllSegments(String tableNameWithType, boolean forceDownload)
      throws Exception {
    LOGGER.info("Adding or replacing all segments in table: {} downloadIsForced: {}", tableNameWithType, forceDownload);
    List<SegmentMetadata> segMds = getAllSegmentsMetadata(tableNameWithType);
    for (SegmentMetadata segMd : segMds) {
      addOrReplaceSegment(tableNameWithType, segMd.getName(), forceDownload);
    }
    LOGGER.info("Added or replaced all segments in table: {}", tableNameWithType);
  }

  @Override
  public Set<String> getAllTables() {
    return _tableDataManagerMap.keySet();
  }

  @Nullable
  @Override
  public TableDataManager getTableDataManager(String tableNameWithType) {
    return _tableDataManagerMap.get(tableNameWithType);
  }

  @Nullable
  @Override
  public SegmentMetadata getSegmentMetadata(String tableNameWithType, String segmentName) {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
      if (segmentDataManager == null) {
        return null;
      }
      try {
        return segmentDataManager.getSegment().getSegmentMetadata();
      } finally {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    return null;
  }

  @Override
  public List<SegmentMetadata> getAllSegmentsMetadata(String tableNameWithType) {
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      return Collections.emptyList();
    } else {
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        List<SegmentMetadata> segmentsMetadata = new ArrayList<>(segmentDataManagers.size());
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          segmentsMetadata.add(segmentDataManager.getSegment().getSegmentMetadata());
        }
        return segmentsMetadata;
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
  }

  @Override
  public File getSegmentDataDirectory(String tableNameWithType, String segmentName) {
    return TableDataManager
        .getSegmentDataDir(_instanceDataManagerConfig.getInstanceDataDir(), tableNameWithType, segmentName);
  }

  @Override
  public String getSegmentFileDirectory() {
    return _instanceDataManagerConfig.getInstanceSegmentTarDir();
  }

  @Override
  public int getMaxParallelRefreshThreads() {
    return _instanceDataManagerConfig.getMaxParallelRefreshThreads();
  }

  @Override
  public ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    return _propertyStore;
  }

  @Override
  public SegmentUploader getSegmentUploader() {
    return _segmentUploader;
  }
}
