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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.File;
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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.ConfigurationException;
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
import org.apache.pinot.core.data.manager.realtime.SegmentBuildTimeLeaseExtender;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.core.util.SegmentRefreshSemaphore;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
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
  // TODO: Make this configurable
  private static final long EXTERNAL_VIEW_DROPPED_MAX_WAIT_MS = 20 * 60_000L; // 20 minutes
  private static final long EXTERNAL_VIEW_CHECK_INTERVAL_MS = 1_000L; // 1 second

  private final ConcurrentHashMap<String, TableDataManager> _tableDataManagerMap = new ConcurrentHashMap<>();

  private HelixInstanceDataManagerConfig _instanceDataManagerConfig;
  private String _instanceId;
  private HelixManager _helixManager;
  private ServerMetrics _serverMetrics;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
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
    tableDataManagerConfig.overrideConfigs(tableConfig);
    TableDataManager tableDataManager =
        TableDataManagerProvider.getTableDataManager(tableDataManagerConfig, _instanceId, _propertyStore,
            _serverMetrics, _helixManager, _errorCache);
    tableDataManager.start();
    LOGGER.info("Created table data manager for table: {}", tableNameWithType);
    return tableDataManager;
  }

  @Override
  public void deleteTable(String tableNameWithType)
      throws Exception {
    // Wait externalview to converge
    long endTimeMs = System.currentTimeMillis() + EXTERNAL_VIEW_DROPPED_MAX_WAIT_MS;
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
      Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
    } while (System.currentTimeMillis() < endTimeMs);
    throw new TimeoutException(
        "Timeout while waiting for ExternalView to converge for the table to delete: " + tableNameWithType);
  }

  @Override
  public void removeSegment(String tableNameWithType, String segmentName) {
    LOGGER.info("Removing segment: {} from table: {}", segmentName, tableNameWithType);
    _tableDataManagerMap.computeIfPresent(tableNameWithType, (k, v) -> {
      v.removeSegment(segmentName);
      LOGGER.info("Removed segment: {} from table: {}", segmentName, k);
      return v;
    });
  }

  @Override
  public void reloadSegment(String tableNameWithType, String segmentName, boolean forceDownload)
      throws Exception {
    LOGGER.info("Reloading single segment: {} in table: {}", segmentName, tableNameWithType);
    SegmentMetadata segmentMetadata = getSegmentMetadata(tableNameWithType, segmentName);
    if (segmentMetadata == null) {
      LOGGER.info("Segment metadata is null. Skip reloading segment: {} in table: {}", segmentName,
          tableNameWithType);
      return;
    }

    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);

    reloadSegment(tableNameWithType, segmentMetadata, tableConfig, schema, forceDownload);

    LOGGER.info("Reloaded single segment: {} in table: {}", segmentName, tableNameWithType);
  }

  @Override
  public void reloadAllSegments(String tableNameWithType, boolean forceDownload,
      SegmentRefreshSemaphore segmentRefreshSemaphore)
      throws Exception {
    LOGGER.info("Reloading all segments in table: {}", tableNameWithType);
    long startTime = System.currentTimeMillis();
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableNameWithType);
    List<String> failedSegments = new ArrayList<>();
    List<SegmentMetadata> segmentsMetadata = getAllSegmentsMetadata(tableNameWithType);
    ExecutorService workers = Executors.newCachedThreadPool();
    final AtomicReference<Exception> sampleException = new AtomicReference<>();
    //calling thread hasn't acquired any permit so we don't reload any segments using it.
    CompletableFuture.allOf(segmentsMetadata.stream().map(segmentMetadata -> CompletableFuture.runAsync(() -> {
      String segmentName = segmentMetadata.getName();
      try {
        segmentRefreshSemaphore.acquireSema(segmentMetadata.getName(), LOGGER);
        try {
          reloadSegment(tableNameWithType, segmentMetadata, tableConfig, schema, forceDownload);
        } finally {
          segmentRefreshSemaphore.releaseSema();
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while reloading segment: {} in table: {}", segmentName, tableNameWithType, e);
        failedSegments.add(segmentName);
        sampleException.set(e);
      }
    }, workers)).toArray(CompletableFuture[]::new)).get();

    workers.shutdownNow();

    if (sampleException.get() != null) {
      throw new RuntimeException(
          String.format("Failed to reload %d/%d segments: %s in table: %s", failedSegments.size(),
              segmentsMetadata.size(), failedSegments, tableNameWithType), sampleException.get());
    }
    LOGGER.info("Reloaded all segments in table: {}. Duration: {}", tableNameWithType,
        (System.currentTimeMillis() - startTime));
  }

  private void reloadSegment(String tableNameWithType, SegmentMetadata segmentMetadata, TableConfig tableConfig,
      @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    String segmentName = segmentMetadata.getName();
    LOGGER.info("Reloading segment: {} in table: {} with forceDownload: {}", segmentName, tableNameWithType,
        forceDownload);

    TableDataManager tableDataManager = _tableDataManagerMap.get(tableNameWithType);
    if (tableDataManager == null) {
      LOGGER.warn("Failed to find table data manager for table: {}, skipping reloading segment", tableNameWithType);
      return;
    }

    File indexDir = segmentMetadata.getIndexDir();
    if (indexDir == null) {
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
      if (segmentDataManager == null) {
        return;
      }
      try {
        if (reloadMutableSegment(tableNameWithType, segmentName, segmentDataManager, schema)) {
          // A mutable segment has been found and reloaded.
          segmentDataManager.setLoadTimeMs(System.currentTimeMillis());
          return;
        }
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
      tableDataManager.reloadSegment(segmentName, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig),
          zkMetadata, segmentMetadata, schema, forceDownload);
      LOGGER.info("Reloaded segment: {} of table: {}", segmentName, tableNameWithType);
    } finally {
      segmentLock.unlock();
    }
  }

  /**
   * Try to reload a mutable segment.
   * @return true if the segment is mutable and loaded; false if the segment is immutable.
   */
  @VisibleForTesting
  boolean reloadMutableSegment(String tableNameWithType, String segmentName,
      SegmentDataManager segmentDataManager, @Nullable Schema schema) {
    IndexSegment segment = segmentDataManager.getSegment();
    if (segment instanceof ImmutableSegment) {
      LOGGER.info("Found an immutable segment: {} in table: {}", segmentName, tableNameWithType);
      return false;
    }
    // Found a mutable/consuming segment from REALTIME table.
    if (!_instanceDataManagerConfig.shouldReloadConsumingSegment()) {
      LOGGER.info("Skip reloading REALTIME consuming segment: {} in table: {}", segmentName, tableNameWithType);
      return true;
    }
    LOGGER.info("Reloading REALTIME consuming segment: {} in table: {}", segmentName, tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for table: {}", tableNameWithType);
    MutableSegmentImpl mutableSegment = (MutableSegmentImpl) segment;
    mutableSegment.addExtraColumns(schema);
    return true;
  }

  @Override
  public void addOrReplaceSegment(String tableNameWithType, String segmentName)
      throws Exception {
    LOGGER.info("Adding or replacing segment: {} for table: {}", segmentName, tableNameWithType);

    // Get updated table config and segment metadata from Zookeeper.
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    Preconditions.checkNotNull(tableConfig);
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
    Preconditions.checkNotNull(zkMetadata);

    // This method might modify the file on disk. Use segment lock to prevent race condition
    Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
    try {
      segmentLock.lock();

      // But if table mgr is not created or the segment is not loaded yet, the localMetadata
      // is set to null. Then, addOrReplaceSegment method will load the segment accordingly.
      SegmentMetadata localMetadata = getSegmentMetadata(tableNameWithType, segmentName);

      _tableDataManagerMap.computeIfAbsent(tableNameWithType, k -> createTableDataManager(k, tableConfig))
          .addOrReplaceSegment(segmentName, new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig), zkMetadata,
              localMetadata);
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
    Preconditions.checkArgument(TableNameBuilder.isRealtimeTableResource(tableNameWithType), String
        .format("Force commit is only supported for segments of realtime tables - table name: %s segment names: %s",
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
