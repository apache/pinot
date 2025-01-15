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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
import org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploader;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.core.util.SegmentRefreshSemaphore;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>HelixInstanceDataManager</code> is the instance data manager based on Helix.
 */
@ThreadSafe
public class HelixInstanceDataManager implements InstanceDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixInstanceDataManager.class);
  private static final int FORCE_COMMIT_STATUS_CHECK_INTERVAL_MS = 15000;
  private static final RetryPolicy DEFAULT_RETRY_POLICY =
      RetryPolicies.fixedDelayRetryPolicy(10, FORCE_COMMIT_STATUS_CHECK_INTERVAL_MS);

  private final ConcurrentHashMap<String, TableDataManager> _tableDataManagerMap = new ConcurrentHashMap<>();
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

  private ExecutorService _segmentRefreshExecutor;
  private ExecutorService _segmentPreloadExecutor;

  @Override
  public void setSupplierOfIsServerReadyToServeQueries(Supplier<Boolean> isServingQueries) {
    _isServerReadyToServeQueries = isServingQueries;
  }

  @Override
  public synchronized void init(PinotConfiguration config, HelixManager helixManager, ServerMetrics serverMetrics)
      throws Exception {
    LOGGER.info("Initializing Helix instance data manager");

    _instanceDataManagerConfig = new HelixInstanceDataManagerConfig(config);
    LOGGER.info("HelixInstanceDataManagerConfig: {}", _instanceDataManagerConfig.getConfig());
    _instanceId = _instanceDataManagerConfig.getInstanceId();
    _helixManager = helixManager;
    String tableDataManagerProviderClass = _instanceDataManagerConfig.getTableDataManagerProviderClass();
    LOGGER.info("Initializing table data manager provider of class: {}", tableDataManagerProviderClass);
    _tableDataManagerProvider = PluginManager.get().createInstance(tableDataManagerProviderClass);
    _tableDataManagerProvider.init(_instanceDataManagerConfig, helixManager, _segmentLocks);
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
    // Initialize a fixed thread pool to reload/refresh segments in parallel. The getMaxParallelRefreshThreads() is
    // used to initialize a segment refresh semaphore to limit the parallelism, so create a pool of same size.
    int poolSize = getMaxParallelRefreshThreads();
    Preconditions.checkArgument(poolSize > 0,
        "SegmentRefreshExecutor requires a positive pool size but got: " + poolSize);
    _segmentRefreshExecutor = Executors.newFixedThreadPool(poolSize,
        new ThreadFactoryBuilder().setNameFormat("segment-refresh-thread-%d").build());
    LOGGER.info("Created SegmentRefreshExecutor with pool size: {}", poolSize);
    poolSize = _instanceDataManagerConfig.getMaxSegmentPreloadThreads();
    if (poolSize > 0) {
      _segmentPreloadExecutor = Executors.newFixedThreadPool(poolSize,
          new ThreadFactoryBuilder().setNameFormat("segment-preload-thread-%d").build());
      LOGGER.info("Created SegmentPreloadExecutor with pool size: {}", poolSize);
    } else {
      LOGGER.info("SegmentPreloadExecutor was not created with pool size: {}", poolSize);
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
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public synchronized void start() {
    _propertyStore = _helixManager.getHelixPropertyStore();
    LOGGER.info("Helix instance data manager started");
  }

  @Override
  public synchronized void shutDown() {
    _segmentRefreshExecutor.shutdownNow();
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
    TableDataManager tableDataManager =
        _tableDataManagerProvider.getTableDataManager(tableConfig, _segmentPreloadExecutor, _errorCache,
            _isServerReadyToServeQueries);
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

  // TODO: Move reload handling logic to table data manager
  @Override
  public void reloadSegment(String tableNameWithType, String segmentName, boolean forceDownload)
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

    reloadSegmentWithMetadata(tableNameWithType, segmentMetadata, tableConfig, schema, forceDownload);

    LOGGER.info("Reloaded single segment: {} in table: {}", segmentName, tableNameWithType);
  }

  // TODO: Move reload handling logic to table data manager
  @Override
  public void reloadAllSegments(String tableNameWithType, boolean forceDownload,
      SegmentRefreshSemaphore segmentRefreshSemaphore)
      throws Exception {
    LOGGER.info("Reloading all segments in table: {}", tableNameWithType);
    List<SegmentMetadata> segmentsMetadata = getAllSegmentsMetadata(tableNameWithType);
    reloadSegmentsWithMetadata(tableNameWithType, segmentsMetadata, forceDownload, segmentRefreshSemaphore);
  }

  // TODO: Move reload handling logic to table data manager
  @Override
  public void reloadSegments(String tableNameWithType, List<String> segmentNames, boolean forceDownload,
      SegmentRefreshSemaphore segmentRefreshSemaphore)
      throws Exception {
    LOGGER.info("Reloading multiple segments: {} in table: {}", segmentNames, tableNameWithType);

    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      LOGGER.warn("Failed to find table data manager for table: {}, skipping reloading segments: {}", tableNameWithType,
          segmentNames);
      return;
    }
    List<String> missingSegments = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireSegments(segmentNames, missingSegments);
    if (!missingSegments.isEmpty()) {
      LOGGER.warn("Failed to get segment data manager for segments: {} of table: {}, skipping reloading them",
          missingSegments, tableDataManager);
    }
    List<SegmentMetadata> segmentsMetadata = new ArrayList<>(segmentDataManagers.size());
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        segmentsMetadata.add(segmentDataManager.getSegment().getSegmentMetadata());
      }
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    reloadSegmentsWithMetadata(tableNameWithType, segmentsMetadata, forceDownload, segmentRefreshSemaphore);
  }

  private void reloadSegmentsWithMetadata(String tableNameWithType, List<SegmentMetadata> segmentsMetadata,
      boolean forceDownload, SegmentRefreshSemaphore segmentRefreshSemaphore)
      throws Exception {
    long startTime = System.currentTimeMillis();
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    List<String> failedSegments = new ArrayList<>();
    final AtomicReference<Exception> sampleException = new AtomicReference<>();
    //calling thread hasn't acquired any permit so we don't reload any segments using it.
    CompletableFuture.allOf(segmentsMetadata.stream().map(segmentMetadata -> CompletableFuture.runAsync(() -> {
      String segmentName = segmentMetadata.getName();
      try {
        segmentRefreshSemaphore.acquireSema(segmentMetadata.getName(), LOGGER);
        try {
          reloadSegmentWithMetadata(tableNameWithType, segmentMetadata, tableConfig, schema, forceDownload);
        } finally {
          segmentRefreshSemaphore.releaseSema();
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while reloading segment: {} in table: {}", segmentName, tableNameWithType, e);
        failedSegments.add(segmentName);
        sampleException.set(e);
      }
    }, _segmentRefreshExecutor)).toArray(CompletableFuture[]::new)).get();
    if (sampleException.get() != null) {
      throw new RuntimeException(
          String.format("Failed to reload %d/%d segments: %s in table: %s", failedSegments.size(),
              segmentsMetadata.size(), failedSegments, tableNameWithType), sampleException.get());
    }
    LOGGER.info("Reloaded segments with metadata in table: {}. Duration: {}", tableNameWithType,
        (System.currentTimeMillis() - startTime));
  }

  private void reloadSegmentWithMetadata(String tableNameWithType, SegmentMetadata segmentMetadata,
      TableConfig tableConfig, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    LOGGER.info("Reloading segment: {} in table: {} with forceDownload: {}", segmentName, tableNameWithType,
        forceDownload);

    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      LOGGER.warn("Failed to find table data manager for table: {}, skipping reloading segment", tableNameWithType);
      return;
    }

    if (segmentMetadata.isMutableSegment()) {
      // Use force commit to reload consuming segment
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
      if (segmentDataManager == null) {
        LOGGER.warn("Failed to find segment data manager for table: {}, segment: {}, skipping reloading segment",
            tableNameWithType, segmentName);
        return;
      }
      try {
        if (!_instanceDataManagerConfig.shouldReloadConsumingSegment()) {
          LOGGER.warn("Skip reloading consuming segment: {} in table: {} as configured", segmentName,
              tableNameWithType);
          return;
        }
        if (segmentDataManager instanceof RealtimeSegmentDataManager) {
          LOGGER.info("Reloading (force committing) consuming segment: {} in table: {}", segmentName,
              tableNameWithType);
          ((RealtimeSegmentDataManager) segmentDataManager).forceCommit();
        }
        return;
      } finally {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig, schema);
    indexLoadingConfig.setErrorOnColumnBuildFailure(true);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for segment: %s of table: %s", segmentName,
        tableNameWithType);
    tableDataManager.reloadSegment(segmentName, indexLoadingConfig, zkMetadata, segmentMetadata, schema, forceDownload);
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
}
