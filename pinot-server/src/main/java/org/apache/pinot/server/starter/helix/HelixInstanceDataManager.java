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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.TableDataManagerProvider;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploader;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.core.util.SegmentRefreshSemaphore;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
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
  private SegmentUploader _segmentUploader;
  private Supplier<Boolean> _isServerReadyToServeQueries = () -> false;
  private long _externalViewDroppedMaxWaitMs;
  private long _externalViewDroppedCheckInternalMs;

  // Fixed size LRU cache for storing last N errors on the instance.
  // Key is TableNameWithType-SegmentName pair.
  private LoadingCache<Pair<String, String>, SegmentErrorInfo> _errorCache;
  private ExecutorService _segmentRefreshExecutor;
  private ExecutorService _segmentPreloadExecutor;

  @Override
  public void setSupplierOfIsServerReadyToServeQueries(Supplier<Boolean> isServingQueries) {
    _isServerReadyToServeQueries = isServingQueries;
  }

  @Override
  public synchronized void init(PinotConfiguration config, HelixManager helixManager, ServerMetrics serverMetrics)
      throws ConfigurationException {
    LOGGER.info("Initializing Helix instance data manager");

    _instanceDataManagerConfig = new HelixInstanceDataManagerConfig(config);
    LOGGER.info("HelixInstanceDataManagerConfig: {}", _instanceDataManagerConfig);
    _instanceId = _instanceDataManagerConfig.getInstanceId();
    _helixManager = helixManager;
    _serverMetrics = serverMetrics;
    _segmentUploader = new PinotFSSegmentUploader(_instanceDataManagerConfig.getSegmentStoreUri(),
        ServerSegmentCompletionProtocolHandler.getSegmentUploadRequestTimeoutMs(), _serverMetrics);

    _externalViewDroppedMaxWaitMs = _instanceDataManagerConfig.getExternalViewDroppedMaxWaitMs();
    _externalViewDroppedCheckInternalMs = _instanceDataManagerConfig.getExternalViewDroppedCheckIntervalMs();

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
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", offlineTableName);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableConfig);
    _tableDataManagerMap.computeIfAbsent(offlineTableName, k -> createTableDataManager(k, tableConfig))
        .addSegment(indexDir, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig, schema));
    LOGGER.info("Added segment: {} to table: {}", segmentName, offlineTableName);
  }

  @Override
  public void addRealtimeSegment(String realtimeTableName, String segmentName)
      throws Exception {
    LOGGER.info("Adding segment: {} to table: {}", segmentName, realtimeTableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, realtimeTableName);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", realtimeTableName);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableConfig);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", realtimeTableName);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, realtimeTableName, segmentName);
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for segment: %s, table: %s", segmentName,
        realtimeTableName);
    _tableDataManagerMap.computeIfAbsent(realtimeTableName, k -> createTableDataManager(k, tableConfig))
        .addSegment(segmentName, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig, schema), zkMetadata);
    LOGGER.info("Added segment: {} to table: {}", segmentName, realtimeTableName);
  }

  private TableDataManager createTableDataManager(String tableNameWithType, TableConfig tableConfig) {
    LOGGER.info("Creating table data manager for table: {}", tableNameWithType);
    TableDataManagerConfig tableDataManagerConfig = new TableDataManagerConfig(_instanceDataManagerConfig, tableConfig);
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, _instanceId, _propertyStore,
            _serverMetrics, _helixManager, _segmentPreloadExecutor, _errorCache, _isServerReadyToServeQueries);
    tableDataManager.start();
    LOGGER.info("Created table data manager for table: {}", tableNameWithType);
    return tableDataManager;
  }

  @Override
  public void deleteTable(String tableNameWithType)
      throws Exception {
    // Wait externalview to converge
    long endTimeMs = System.currentTimeMillis() + _externalViewDroppedMaxWaitMs;
    do {
      ExternalView externalView = _helixManager.getHelixDataAccessor()
          .getProperty(_helixManager.getHelixDataAccessor().keyBuilder().externalView(tableNameWithType));
      if (externalView == null) {
        LOGGER.info("ExternalView converged for the table to delete: {}", tableNameWithType);
        _tableDataManagerMap.compute(tableNameWithType, (k, v) -> {
          if (v != null) {
            v.shutDown();
            LOGGER.info("Removed table: {}", tableNameWithType);
          } else {
            LOGGER.warn("Failed to find table data manager for table: {}, skip removing the table", tableNameWithType);
          }
          return null;
        });
        return;
      }
      Thread.sleep(_externalViewDroppedCheckInternalMs);
    } while (System.currentTimeMillis() < endTimeMs);
    throw new TimeoutException(
        "Timeout while waiting for ExternalView to converge for the table to delete: " + tableNameWithType);
  }

  @Override
  public void offloadSegment(String tableNameWithType, String segmentName) {
    LOGGER.info("Removing segment: {} from table: {}", segmentName, tableNameWithType);
    _tableDataManagerMap.computeIfPresent(tableNameWithType, (k, v) -> {
      v.removeSegment(segmentName);
      LOGGER.info("Removed segment: {} from table: {}", segmentName, k);
      return v;
    });
  }

  @Override
  public void deleteSegment(String tableNameWithType, String segmentName)
      throws Exception {
    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
    try {
      segmentLock.lock();

      // Clean up the segment data on default tier unconditionally.
      File segmentDir = getSegmentDataDirectory(tableNameWithType, segmentName);
      if (segmentDir.exists()) {
        FileUtils.deleteQuietly(segmentDir);
        LOGGER.info("Deleted segment directory {} on default tier", segmentDir);
      }
      // We might clean up further more with the specific segment loader. But note that tableDataManager object or
      // even the TableConfig might not be present any more at this point.
      SegmentDirectoryLoader segmentLoader = SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(
          _instanceDataManagerConfig.getSegmentDirectoryLoader());
      if (segmentLoader != null) {
        LOGGER.info("Deleting segment: {} further with segment loader: {}", segmentName,
            _instanceDataManagerConfig.getSegmentDirectoryLoader());
        SegmentDirectoryLoaderContext ctx = new SegmentDirectoryLoaderContext.Builder().setSegmentName(segmentName)
            .setTableDataDir(_instanceDataManagerConfig.getInstanceDataDir() + "/" + tableNameWithType).build();
        segmentLoader.delete(ctx);
      }
    } finally {
      segmentLock.unlock();
    }
  }

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

  @Override
  public void reloadAllSegments(String tableNameWithType, boolean forceDownload,
      SegmentRefreshSemaphore segmentRefreshSemaphore)
      throws Exception {
    LOGGER.info("Reloading all segments in table: {}", tableNameWithType);
    List<SegmentMetadata> segmentsMetadata = getAllSegmentsMetadata(tableNameWithType);
    reloadSegmentsWithMetadata(tableNameWithType, segmentsMetadata, forceDownload, segmentRefreshSemaphore);
  }

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
        // TODO: Support force committing HLC consuming segment
        if (!(segmentDataManager instanceof LLRealtimeSegmentDataManager)) {
          LOGGER.warn("Cannot reload non-LLC consuming segment: {} in table: {}", segmentName, tableNameWithType);
          return;
        }
        LOGGER.info("Reloading (force committing) LLC consuming segment: {} in table: {}", segmentName,
            tableNameWithType);
        ((LLRealtimeSegmentDataManager) segmentDataManager).forceCommit();
        return;
      } finally {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }

    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    Preconditions.checkNotNull(zkMetadata);

    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
    try {
      segmentLock.lock();

      // Reloads an existing segment, and the local segment metadata is existing as asserted above.
      tableDataManager.reloadSegment(segmentName,
          new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig, schema), zkMetadata, segmentMetadata, schema,
          forceDownload);
      LOGGER.info("Reloaded segment: {} of table: {}", segmentName, tableNameWithType);
    } finally {
      segmentLock.unlock();
    }
  }

  @Override
  public void addOrReplaceSegment(String tableNameWithType, String segmentName)
      throws Exception {
    LOGGER.info("Adding or replacing segment: {} for table: {}", segmentName, tableNameWithType);

    // Get updated table config, schema and segment metadata from Zookeeper.
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableConfig);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for segment: %s, table: %s", segmentName,
        tableNameWithType);

    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
    try {
      segmentLock.lock();

      // But if table mgr is not created or the segment is not loaded yet, the localMetadata
      // is set to null. Then, addOrReplaceSegment method will load the segment accordingly.
      SegmentMetadata localMetadata = getSegmentMetadata(tableNameWithType, segmentName);

      _tableDataManagerMap.computeIfAbsent(tableNameWithType, k -> createTableDataManager(k, tableConfig))
          .addOrReplaceSegment(segmentName, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig, schema),
              zkMetadata, localMetadata);
      LOGGER.info("Added or replaced segment: {} of table: {}", segmentName, tableNameWithType);
    } finally {
      segmentLock.unlock();
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
            if (segmentDataManager instanceof LLRealtimeSegmentDataManager) {
              LLRealtimeSegmentDataManager llSegmentDataManager = (LLRealtimeSegmentDataManager) segmentDataManager;
              llSegmentDataManager.forceCommit();
            }
          } finally {
            tableDataManager.releaseSegment(segmentDataManager);
          }
        }
      });
    }
  }
}
