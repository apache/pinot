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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.LogicalTableContext;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
import org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploader;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottler;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>HelixInstanceDataManager</code> is the instance data manager based on Helix.
 */
@ThreadSafe
public class HelixInstanceDataManager implements InstanceDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManager.class);

  private final Map<String, TableDataManager> _tableDataManagerMap = new ConcurrentHashMap<>();

  // A cache for the logical table context
  private static final String TABLE_CONFIG_PATH_PREFIX = "/CONFIGS/TABLE/";
  private static final String SCHEMA_PATH_PREFIX = "/SCHEMAS/";
  private static final String LOGICAL_TABLE_PARENT_PATH = "/LOGICAL/TABLE";
  private static final String LOGICAL_TABLE_PATH_PREFIX = "/LOGICAL/TABLE/";

  private final Map<String, LogicalTableConfig> _logicalTableConfigMap = new ConcurrentHashMap<>();
  private final Map<String, Schema> _schemaMap = new ConcurrentHashMap<>();
  private final Map<String, TableConfig> _tableConfigMap = new ConcurrentHashMap<>();
  private final Map<String, List<Pair<String, String>>> _tableNameToLogicalTableNamesMap = new ConcurrentHashMap<>();

  ZkTableConfigChangeListener _zkTableConfigChangeListener;
  ZkSchemaChangeListener _zkSchemaChangeListener;
  ZkLogicalTableConfigChangeListener _zkLogicalTableConfigChangeListener;

  // TODO: Consider making segment locks per table instead of per instance
  private final SegmentLocks _segmentLocks = new SegmentLocks();

  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private String _instanceId;
  private TableDataManagerProvider _tableDataManagerProvider;
  private HelixManager _helixManager;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private SegmentUploader _segmentUploader;
  private Supplier<Boolean> _isServerReadyToServeQueries = () -> false;

  // Fixed size LRU cache for storing last N errors on the instance.
  // Key is TableNameWithType-SegmentName pair.
  private Cache<Pair<String, String>, SegmentErrorInfo> _errorCache;
  // Cache for recently deleted tables to prevent re-creating them accidentally.
  // Key is table name with type, value is deletion time.
  protected Cache<String, Long> _recentlyDeletedTables;

  private SegmentReloadSemaphore _segmentReloadSemaphore;
  private ExecutorService _segmentReloadExecutor;

  @Nullable
  private ExecutorService _segmentPreloadExecutor;

  @Override
  public void setSupplierOfIsServerReadyToServeQueries(Supplier<Boolean> isServingQueries) {
    _isServerReadyToServeQueries = isServingQueries;
  }

  @Override
  public synchronized void init(PinotConfiguration config, HelixManager helixManager, ServerMetrics serverMetrics,
      @Nullable SegmentOperationsThrottler segmentOperationsThrottler)
      throws Exception {
    LOGGER.info("Initializing Helix instance data manager");

    _instanceDataManagerConfig = new HelixInstanceDataManagerConfig(config);
    LOGGER.info("HelixInstanceDataManagerConfig: {}", _instanceDataManagerConfig.getConfig());
    _instanceId = _instanceDataManagerConfig.getInstanceId();
    _helixManager = helixManager;
    String tableDataManagerProviderClass = _instanceDataManagerConfig.getTableDataManagerProviderClass();
    LOGGER.info("Initializing table data manager provider of class: {}", tableDataManagerProviderClass);
    _tableDataManagerProvider = PluginManager.get().createInstance(tableDataManagerProviderClass);
    _tableDataManagerProvider.init(_instanceDataManagerConfig, helixManager, _segmentLocks, segmentOperationsThrottler);
    _segmentUploader = new PinotFSSegmentUploader(_instanceDataManagerConfig.getSegmentStoreUri(),
        ServerSegmentCompletionProtocolHandler.getSegmentUploadRequestTimeoutMs(), serverMetrics);

    File instanceDataDir = new File(_instanceDataManagerConfig.getInstanceDataDir());
    initInstanceDataDir(instanceDataDir);

    File instanceSegmentTarDir = new File(_instanceDataManagerConfig.getInstanceSegmentTarDir());
    if (!instanceSegmentTarDir.exists()) {
      Preconditions.checkState(instanceSegmentTarDir.mkdirs());
    }

    // Initialize segment build time lease extender executor
    SegmentBuildTimeLeaseExtender.initExecutor();
    // Initialize a semaphore and a fixed thread pool to reload/refresh segments in parallel.
    int maxParallelRefreshThreads = getMaxParallelRefreshThreads();
    Preconditions.checkArgument(maxParallelRefreshThreads > 0,
        "'pinot.server.instance.max.parallel.refresh.threads' must be positive, got: " + maxParallelRefreshThreads);
    _segmentReloadSemaphore = new SegmentReloadSemaphore(maxParallelRefreshThreads);
    _segmentReloadExecutor = Executors.newFixedThreadPool(maxParallelRefreshThreads,
        new ThreadFactoryBuilder().setNameFormat("segment-reload-thread-%d").build());
    LOGGER.info("Created SegmentReloadExecutor with pool size: {}", maxParallelRefreshThreads);
    int maxSegmentPreloadThreads = _instanceDataManagerConfig.getMaxSegmentPreloadThreads();
    if (maxSegmentPreloadThreads > 0) {
      _segmentPreloadExecutor = Executors.newFixedThreadPool(maxSegmentPreloadThreads,
          new ThreadFactoryBuilder().setNameFormat("segment-preload-thread-%d").build());
      LOGGER.info("Created SegmentPreloadExecutor with pool size: {}", maxSegmentPreloadThreads);
    } else {
      LOGGER.info("SegmentPreloadExecutor was not created with pool size: {}", maxSegmentPreloadThreads);
    }
    LOGGER.info("Initialized Helix instance data manager");

    // Initialize the error cache and recently deleted tables cache
    _errorCache = CacheBuilder.newBuilder().maximumSize(_instanceDataManagerConfig.getErrorCacheSize()).build();
    _recentlyDeletedTables = CacheBuilder.newBuilder()
        .expireAfterWrite(_instanceDataManagerConfig.getDeletedTablesCacheTtlMinutes(), TimeUnit.MINUTES).build();
  }

  private void initInstanceDataDir(File instanceDataDir) {
    if (!instanceDataDir.exists()) {
      Preconditions.checkState(instanceDataDir.mkdirs(), "Failed to create instance data dir: %s", instanceDataDir);
    } else {
      // Clean up the _tmp (consuming segments) dir and empty table data dir
      File[] tableDataDirs = instanceDataDir.listFiles((dir, name) -> TableNameBuilder.isTableResource(name));
      if (tableDataDirs != null) {
        for (File tableDataDir : tableDataDirs) {
          File resourceTempDir = new File(tableDataDir, RealtimeSegmentDataManager.RESOURCE_TEMP_DIR_NAME);
          try {
            FileUtils.deleteDirectory(resourceTempDir);
          } catch (IOException e) {
            LOGGER.error("Failed to delete temporary resource dir: {}, continue with error", resourceTempDir, e);
          }
          try {
            if (FileUtils.isEmptyDirectory(tableDataDir)) {
              FileUtils.deleteDirectory(tableDataDir);
            }
          } catch (IOException e) {
            LOGGER.error("Failed to delete empty table data dir: {}, continue with error", tableDataDir, e);
          }
        }
      }
    }
  }

  @Override
  public List<File> getConsumerDirPaths() {
    List<File> consumerDirs = new ArrayList<>();
    for (TableDataManager tableDataManager : _tableDataManagerMap.values()) {
      if (tableDataManager instanceof RealtimeTableDataManager) {
        File consumerDir = ((RealtimeTableDataManager) tableDataManager).getConsumerDirPath();
        consumerDirs.add(consumerDir);
      }
    }
    return consumerDirs;
  }

  @Override
  public String getInstanceDataDir() {
    return _instanceDataManagerConfig.getInstanceDataDir();
  }

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public synchronized void start() {
    _propertyStore = _helixManager.getHelixPropertyStore();

    _zkTableConfigChangeListener = new ZkTableConfigChangeListener();
    _zkSchemaChangeListener = new ZkSchemaChangeListener();
    _zkLogicalTableConfigChangeListener = new ZkLogicalTableConfigChangeListener();

    // Add child listeners to the property store for logical table config changes
    _propertyStore.subscribeChildChanges(LOGICAL_TABLE_PARENT_PATH, _zkLogicalTableConfigChangeListener);

    LOGGER.info("Helix instance data manager started");
  }

  @Override
  public synchronized void shutDown() {
    _segmentReloadExecutor.shutdownNow();
    if (_segmentPreloadExecutor != null) {
      _segmentPreloadExecutor.shutdownNow();
    }
    if (!_tableDataManagerMap.isEmpty()) {
      int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), _tableDataManagerMap.size());
      ExecutorService stopExecutorService = Executors.newFixedThreadPool(numThreads);
      for (TableDataManager tableDataManager : _tableDataManagerMap.values()) {
        stopExecutorService.submit(tableDataManager::shutDown);
      }
      stopExecutorService.shutdown();
      try {
        // Wait at most 10 minutes before exiting this method.
        if (!stopExecutorService.awaitTermination(10, TimeUnit.MINUTES)) {
          stopExecutorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        stopExecutorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    SegmentBuildTimeLeaseExtender.shutdownExecutor();
    _propertyStore.unsubscribeChildChanges(LOGICAL_TABLE_PARENT_PATH, _zkLogicalTableConfigChangeListener);
    LOGGER.info("Helix instance data manager shut down");
  }

  @Override
  public void deleteTable(String tableNameWithType, long deletionTimeMs)
      throws Exception {
    AtomicReference<TableDataManager> tableDataManagerRef = new AtomicReference<>();
    _tableDataManagerMap.computeIfPresent(tableNameWithType, (k, v) -> {
      _recentlyDeletedTables.put(k, deletionTimeMs);
      tableDataManagerRef.set(v);
      return null;
    });
    TableDataManager tableDataManager = tableDataManagerRef.get();
    if (tableDataManager == null) {
      LOGGER.warn("Failed to find table data manager for table: {}, skip deleting the table", tableNameWithType);
      return;
    }
    LOGGER.info("Shutting down table data manager for table: {}", tableNameWithType);
    tableDataManager.shutDown();
    LOGGER.info("Finished shutting down table data manager for table: {}", tableNameWithType);
  }

  @Override
  public void addOnlineSegment(String tableNameWithType, String segmentName)
      throws Exception {
    _tableDataManagerMap.computeIfAbsent(tableNameWithType, this::createTableDataManager).addOnlineSegment(segmentName);
  }

  @Override
  public void addConsumingSegment(String realtimeTableName, String segmentName)
      throws Exception {
    _tableDataManagerMap.computeIfAbsent(realtimeTableName, this::createTableDataManager)
        .addConsumingSegment(segmentName);
  }

  private TableDataManager createTableDataManager(String tableNameWithType) {
    LOGGER.info("Creating table data manager for table: {}", tableNameWithType);
    TableConfig tableConfig;
    Long tableDeleteTimeMs = _recentlyDeletedTables.getIfPresent(tableNameWithType);
    if (tableDeleteTimeMs != null) {
      long currentTimeMs = System.currentTimeMillis();
      LOGGER.info("Table: {} was recently deleted (deleted {}ms ago), checking table config creation timestamp",
          tableNameWithType, currentTimeMs - tableDeleteTimeMs);
      Pair<TableConfig, Stat> tableConfigAndStat =
          ZKMetadataProvider.getTableConfigWithStat(_propertyStore, tableNameWithType);
      Preconditions.checkState(tableConfigAndStat != null, "Failed to find table config for table: %s",
          tableNameWithType);
      tableConfig = tableConfigAndStat.getLeft();
      long tableCreationTimeMs = tableConfigAndStat.getRight().getCtime();
      Preconditions.checkState(tableCreationTimeMs > tableDeleteTimeMs,
          "Table: %s was recently deleted (deleted %dms ago) but the table config was created before that (created "
              + "%dms ago)", tableNameWithType, currentTimeMs - tableDeleteTimeMs, currentTimeMs - tableCreationTimeMs);
      _recentlyDeletedTables.invalidate(tableNameWithType);
    } else {
      tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
      Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);
    }
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableNameWithType);
    TimestampIndexUtils.applyTimestampIndex(tableConfig, schema);
    TableDataManager tableDataManager =
        _tableDataManagerProvider.getTableDataManager(tableConfig, schema, _segmentReloadSemaphore,
            _segmentReloadExecutor, _segmentPreloadExecutor, _errorCache, _isServerReadyToServeQueries);
    tableDataManager.start();
    LOGGER.info("Created table data manager for table: {}", tableNameWithType);
    return tableDataManager;
  }

  @Override
  public void replaceSegment(String tableNameWithType, String segmentName)
      throws Exception {
    LOGGER.info("Replacing segment: {} in table: {}", segmentName, tableNameWithType);
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      tableDataManager.replaceSegment(segmentName);
    } else {
      LOGGER.warn("Failed to find data manager for table: {}, skipping replacing segment: {}", tableNameWithType,
          segmentName);
    }
  }

  @Override
  public void offloadSegment(String tableNameWithType, String segmentName)
      throws Exception {
    LOGGER.info("Offloading segment: {} from table: {}", segmentName, tableNameWithType);
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      tableDataManager.offloadSegment(segmentName);
    } else {
      if (_recentlyDeletedTables.getIfPresent(tableNameWithType) != null) {
        LOGGER.info("Table: {} was recently deleted, skipping offloading segment: {}", tableNameWithType, segmentName);
      } else {
        LOGGER.warn("Failed to find data manager for table: {}, skipping offloading segment: {}", tableNameWithType,
            segmentName);
      }
    }
  }

  @Override
  public void deleteSegment(String tableNameWithType, String segmentName)
      throws Exception {
    LOGGER.info("Deleting segment: {} from table: {}", segmentName, tableNameWithType);
    // Segment deletion is handled at instance level because table data manager might not exist. Acquire the lock here.
    Lock segmentLock = _segmentLocks.getLock(tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      // Check if the segment is still loaded, if so, offload it first.
      // This might happen when the server disconnected from ZK and reconnected, and the segment is still loaded.
      // TODO: Consider using table data manager to delete the segment. This will allow the table data manager to clean
      //       up the segment data on all tiers. Note that table data manager might have not been created, and table
      //       config might have been deleted at this point.
      TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
      if (tableDataManager != null && tableDataManager.hasSegment(segmentName)) {
        LOGGER.warn("Segment: {} from table: {} is still loaded, offloading it first", segmentName, tableNameWithType);
        tableDataManager.offloadSegment(segmentName);
      }
      // Clean up the segment data on default tier unconditionally.
      File segmentDir = getSegmentDataDirectory(tableNameWithType, segmentName);
      if (segmentDir.exists()) {
        FileUtils.deleteQuietly(segmentDir);
        LOGGER.info("Deleted segment directory {} on default tier", segmentDir);
      }
      // We might clean up further more with the specific segment loader. But note that table data manager might have
      // not been created, and table config might have been deleted at this point.
      SegmentDirectoryLoader segmentLoader = SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(
          _instanceDataManagerConfig.getSegmentDirectoryLoader());
      if (segmentLoader != null) {
        LOGGER.info("Deleting segment: {} further with segment loader: {}", segmentName,
            _instanceDataManagerConfig.getSegmentDirectoryLoader());
        SegmentDirectoryLoaderContext ctx = new SegmentDirectoryLoaderContext.Builder().setSegmentName(segmentName)
            .setTableDataDir(_instanceDataManagerConfig.getInstanceDataDir() + "/" + tableNameWithType).build();
        segmentLoader.delete(ctx);
      }
      LOGGER.info("Deleted segment: {} from table: {}", segmentName, tableNameWithType);
    } finally {
      segmentLock.unlock();
    }
  }

  @Override
  public void reloadSegment(String tableNameWithType, String segmentName, boolean forceDownload)
      throws Exception {
    LOGGER.info("Reloading segment: {} in table: {}", segmentName, tableNameWithType);
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      tableDataManager.reloadSegment(segmentName, forceDownload);
    } else {
      LOGGER.warn("Failed to find data manager for table: {}, skipping reloading segment: {}", tableNameWithType,
          segmentName);
    }
  }

  @Override
  public void reloadAllSegments(String tableNameWithType, boolean forceDownload)
      throws Exception {
    LOGGER.info("Reloading all segments in table: {}", tableNameWithType);
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      tableDataManager.reloadAllSegments(forceDownload);
    } else {
      LOGGER.warn("Failed to find data manager for table: {}, skipping reloading all segments", tableNameWithType);
    }
  }

  @Override
  public void reloadSegments(String tableNameWithType, List<String> segmentNames, boolean forceDownload)
      throws Exception {
    LOGGER.info("Reloading segments: {} in table: {}", segmentNames, tableNameWithType);
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      tableDataManager.reloadSegments(segmentNames, forceDownload);
    } else {
      LOGGER.warn("Failed to find data manager for table: {}, skipping reloading segments: {}", tableNameWithType,
          segmentNames);
    }
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

  /**
   * Assemble the path to segment dir directly, when table mgr object is not
   * created for the given table yet.
   */
  @Override
  public File getSegmentDataDirectory(String tableNameWithType, String segmentName) {
    return new File(new File(_instanceDataManagerConfig.getInstanceDataDir(), tableNameWithType), segmentName);
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

  @Override
  public void forceCommit(String tableNameWithType, Set<String> segmentNames) {
    Preconditions.checkArgument(TableNameBuilder.isRealtimeTableResource(tableNameWithType), String.format(
        "Force commit is only supported for segments of realtime tables - table name: %s segment names: %s",
        tableNameWithType, segmentNames));
    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager != null) {
      segmentNames.forEach(segName -> {
        SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segName);
        if (segmentDataManager != null) {
          try {
            if (segmentDataManager instanceof RealtimeSegmentDataManager) {
              ((RealtimeSegmentDataManager) segmentDataManager).forceCommit();
            }
          } finally {
            tableDataManager.releaseSegment(segmentDataManager);
          }
        }
      });
    }
  }

  // TODO: LogicalTableContext has to be cached. https://github.com/apache/pinot/issues/15859
  @Nullable
  @Override
  public LogicalTableContext getLogicalTableContext(String logicalTableName) {
    Schema schema = _schemaMap.get(logicalTableName);
    if (schema == null) {
      LOGGER.warn("Failed to find schema for logical table: {}, skipping", logicalTableName);
      return null;
    }
    LogicalTableConfig logicalTableConfig = _logicalTableConfigMap.get(logicalTableName);
    if (logicalTableConfig == null) {
      LOGGER.warn("Failed to find logical table config for logical table: {}, skipping", logicalTableName);
      return null;
    }

    TableConfig offlineTableConfig = null;
    if (logicalTableConfig.getRefOfflineTableName() != null) {
      offlineTableConfig = _tableConfigMap.get(logicalTableConfig.getRefOfflineTableName());
      if (offlineTableConfig == null) {
        LOGGER.warn("Failed to find offline table config for logical table: {}, skipping", logicalTableName);
        return null;
      }
    }

    TableConfig realtimeTableConfig = null;
    if (logicalTableConfig.getRefRealtimeTableName() != null) {
      realtimeTableConfig = _tableConfigMap.get(logicalTableConfig.getRefRealtimeTableName());
      if (realtimeTableConfig == null) {
        LOGGER.warn("Failed to find realtime table config for logical table: {}, skipping", logicalTableName);
        return null;
      }
    }
    return new LogicalTableContext(logicalTableConfig, schema, offlineTableConfig, realtimeTableConfig);
  }

  private class ZkTableConfigChangeListener implements IZkDataListener {

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          TableConfig tableConfig = TableConfigUtils.fromZNRecord(znRecord);
          _tableConfigMap.put(tableConfig.getTableName(), tableConfig);
        } catch (Exception e) {
          LOGGER.error("Caught exception while refreshing table config for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // no-op, table config should not be deleted while referenced in the logical table config
    }
  }

  private class ZkSchemaChangeListener implements IZkDataListener {

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          Schema schema = SchemaUtils.fromZNRecord(znRecord);
          _schemaMap.put(schema.getSchemaName(), schema);
        } catch (Exception e) {
          LOGGER.error("Caught exception while refreshing schema for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // no-op, schema should not be deleted before the logical table config
    }
  }

  private class ZkLogicalTableConfigChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> logicalTableNames) {
      if (CollectionUtils.isEmpty(logicalTableNames)) {
        return;
      }

      // Only process new added logical tables. Changed/removed logical tables are handled by other callbacks.
      List<String> pathsToAdd = new ArrayList<>();
      for (String logicalTableName : logicalTableNames) {
        if (!_logicalTableConfigMap.containsKey(logicalTableName)) {
          pathsToAdd.add(LOGICAL_TABLE_PATH_PREFIX + logicalTableName);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addLogicalTableConfigs(pathsToAdd);
      }
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        updateLogicalTableConfig((ZNRecord) data);
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // NOTE: The path here is the absolute ZK path instead of the relative path to the property store.
      String logicalTableName = path.substring(path.lastIndexOf('/') + 1);
      removeLogicalTableConfig(logicalTableName);
    }

    private synchronized void addLogicalTableConfigs(List<String> pathsToAdd) {
      for (String path : pathsToAdd) {
        ZNRecord znRecord = _propertyStore.get(path, null, AccessOption.PERSISTENT);
        if (znRecord != null) {
          try {
            LogicalTableConfig logicalTableConfig = LogicalTableConfigUtils.fromZNRecord(znRecord);
            String logicalTableName = logicalTableConfig.getTableName();
            _logicalTableConfigMap.put(logicalTableName, logicalTableConfig);

            if (logicalTableConfig.getRefOfflineTableName() != null) {
              addTableConfig(logicalTableConfig.getRefOfflineTableName(), logicalTableName);
            }
            if (logicalTableConfig.getRefRealtimeTableName() != null) {
              addTableConfig(logicalTableConfig.getRefRealtimeTableName(), logicalTableName);
            }

            addSchema(logicalTableName);
            String logicalTableConfigPath = LOGICAL_TABLE_PATH_PREFIX + logicalTableName;
            getPropertyStore().subscribeDataChanges(logicalTableConfigPath, _zkLogicalTableConfigChangeListener);
          } catch (Exception e) {
            LOGGER.error("Caught exception while refreshing logical table config for ZNRecord: {}", znRecord.getId(),
                e);
          }
        }
      }
    }

    private synchronized void addTableConfig(String tableName, String logicalTableName) {
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableName);
      Preconditions.checkArgument(tableConfig != null, "Failed to find table config for table: %s", tableName);
      _tableNameToLogicalTableNamesMap.computeIfAbsent(tableName, k -> new ArrayList<>())
          .add(Pair.of(tableName, logicalTableName));
      _tableConfigMap.put(tableName, tableConfig);
      String path = TABLE_CONFIG_PATH_PREFIX + tableName;
      getPropertyStore().subscribeDataChanges(path, _zkTableConfigChangeListener);
    }

    private synchronized void addSchema(String logicalTableName) {
      Schema schema = ZKMetadataProvider.getSchema(getPropertyStore(), logicalTableName);
      Preconditions.checkArgument(schema != null,
          "Failed to find schema for logical table: %s", logicalTableName);
      _schemaMap.put(schema.getSchemaName(), schema);
      String schemaPath = SCHEMA_PATH_PREFIX + schema.getSchemaName();
      getPropertyStore().subscribeDataChanges(schemaPath, _zkSchemaChangeListener);
    }

    private synchronized void updateLogicalTableConfig(ZNRecord znRecord) {
      try {
        LogicalTableConfig logicalTableConfig = LogicalTableConfigUtils.fromZNRecord(znRecord);
        String logicalTableName = logicalTableConfig.getTableName();
        LogicalTableConfig oldLogicalTableConfig = _logicalTableConfigMap.put(logicalTableName, logicalTableConfig);
        Preconditions.checkArgument(oldLogicalTableConfig != null,
            "Logical table config for logical table: %s should have been created before", logicalTableName);

        // Remove the old table configs from the table config map
        if (oldLogicalTableConfig.getRefOfflineTableName() != null
            && !oldLogicalTableConfig.getRefOfflineTableName().equals(logicalTableConfig.getRefOfflineTableName())) {
          removeTableConfig(oldLogicalTableConfig.getRefOfflineTableName(), logicalTableName);
        }
        if (oldLogicalTableConfig.getRefRealtimeTableName() != null
            && !oldLogicalTableConfig.getRefRealtimeTableName().equals(logicalTableConfig.getRefRealtimeTableName())) {
          removeTableConfig(oldLogicalTableConfig.getRefRealtimeTableName(), logicalTableName);
        }

        // Add the new table configs to the table config map
        if (logicalTableConfig.getRefOfflineTableName() != null
            && !logicalTableConfig.getRefOfflineTableName().equals(oldLogicalTableConfig.getRefOfflineTableName())) {
          addTableConfig(logicalTableConfig.getRefOfflineTableName(), logicalTableName);
        }
        if (logicalTableConfig.getRefRealtimeTableName() != null
            && !logicalTableConfig.getRefRealtimeTableName().equals(oldLogicalTableConfig.getRefRealtimeTableName())) {
          addTableConfig(logicalTableConfig.getRefRealtimeTableName(), logicalTableName);
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while refreshing logical table for ZNRecord: {}", znRecord.getId(), e);
      }
    }

    private synchronized void removeLogicalTableConfig(String logicalTableName) {
      LogicalTableConfig logicalTableConfig = _logicalTableConfigMap.remove(logicalTableName);
      if (logicalTableConfig != null) {
        // Remove the table configs from the table config map
        String offlineTableName = logicalTableConfig.getRefOfflineTableName();
        String realtimeTableName = logicalTableConfig.getRefRealtimeTableName();
        if (offlineTableName != null) {
          removeTableConfig(offlineTableName, offlineTableName);
        }
        if (realtimeTableName != null) {
          removeTableConfig(realtimeTableName, logicalTableName);
        }
        // remove schema
        removeSchema(logicalTableConfig);
        // Unsubscribe from the logical table config path
        String logicalTableConfigPath = LOGICAL_TABLE_PATH_PREFIX + logicalTableName;
        getPropertyStore().unsubscribeDataChanges(logicalTableConfigPath, _zkLogicalTableConfigChangeListener);
      }
    }

    private synchronized void removeTableConfig(String tableName, String logicalTableName) {
      _tableNameToLogicalTableNamesMap.computeIfPresent(tableName, (k, v) -> {
        v.remove(Pair.of(tableName, logicalTableName));
        return v;
      });

      if (_tableNameToLogicalTableNamesMap.getOrDefault(tableName, List.of()).isEmpty()) {
        _tableNameToLogicalTableNamesMap.remove(tableName);
        _tableConfigMap.remove(tableName);
        String path = TABLE_CONFIG_PATH_PREFIX + tableName;
        getPropertyStore().unsubscribeDataChanges(path, _zkTableConfigChangeListener);
      }
    }

    private synchronized void removeSchema(LogicalTableConfig logicalTableConfig) {
      String schemaName = logicalTableConfig.getTableName();
      _schemaMap.remove(schemaName);
      String schemaPath = SCHEMA_PATH_PREFIX + schemaName;
      getPropertyStore().unsubscribeDataChanges(schemaPath, _zkSchemaChangeListener);
    }
  }
}
