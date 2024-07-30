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
package org.apache.pinot.core.data.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseTableDataManager implements TableDataManager {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTableDataManager.class);

  protected final ConcurrentHashMap<String, SegmentDataManager> _segmentDataManagerMap = new ConcurrentHashMap<>();
  protected final ServerMetrics _serverMetrics = ServerMetrics.get();

  protected InstanceDataManagerConfig _instanceDataManagerConfig;
  protected String _instanceId;
  protected HelixManager _helixManager;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected SegmentLocks _segmentLocks;
  protected TableConfig _tableConfig;
  protected String _tableNameWithType;
  protected String _tableDataDir;
  protected File _indexDir;
  protected File _resourceTmpDir;
  protected Logger _logger;
  protected ExecutorService _segmentPreloadExecutor;
  protected AuthProvider _authProvider;
  protected String _peerDownloadScheme;
  protected long _streamSegmentDownloadUntarRateLimitBytesPerSec;
  protected boolean _isStreamSegmentDownloadUntar;
  // Semaphore to restrict the maximum number of parallel segment downloads for a table
  private Semaphore _segmentDownloadSemaphore;

  // Fixed size LRU cache with TableName - SegmentName pair as key, and segment related
  // errors as the value.
  protected LoadingCache<Pair<String, String>, SegmentErrorInfo> _errorCache;
  // Cache used for identifying segments which could not be acquired since they were recently deleted.
  protected Cache<String, String> _recentlyDeletedSegments;

  protected volatile boolean _shutDown;

  @Override
  public void init(InstanceDataManagerConfig instanceDataManagerConfig, HelixManager helixManager,
      SegmentLocks segmentLocks, TableConfig tableConfig, @Nullable ExecutorService segmentPreloadExecutor,
      @Nullable LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache) {
    LOGGER.info("Initializing table data manager for table: {}", tableConfig.getTableName());

    _instanceDataManagerConfig = instanceDataManagerConfig;
    _instanceId = instanceDataManagerConfig.getInstanceId();
    _tableConfig = tableConfig;
    _segmentLocks = segmentLocks;
    _helixManager = helixManager;
    _propertyStore = helixManager.getHelixPropertyStore();
    _segmentPreloadExecutor = segmentPreloadExecutor;
    _authProvider = AuthProviderUtils.extractAuthProvider(_instanceDataManagerConfig.getAuthConfig(), null);

    _tableNameWithType = tableConfig.getTableName();
    _tableDataDir = _instanceDataManagerConfig.getInstanceDataDir() + File.separator + _tableNameWithType;
    _indexDir = new File(_tableDataDir);
    if (!_indexDir.exists()) {
      Preconditions.checkState(_indexDir.mkdirs(), "Unable to create index directory at %s. "
          + "Please check for available space and write-permissions for this directory.", _indexDir);
    }
    _resourceTmpDir = new File(_indexDir, "tmp");
    // This is meant to cleanup temp resources from TableDataManager. But other code using this same
    // directory will have those deleted as well.
    FileUtils.deleteQuietly(_resourceTmpDir);
    if (!_resourceTmpDir.exists()) {
      Preconditions.checkState(_resourceTmpDir.mkdirs(), "Unable to create temp resources directory at %s. "
          + "Please check for available space and write-permissions for this directory.", _resourceTmpDir);
    }
    _errorCache = errorCache;
    _recentlyDeletedSegments =
        CacheBuilder.newBuilder().maximumSize(instanceDataManagerConfig.getDeletedSegmentsCacheSize())
            .expireAfterWrite(instanceDataManagerConfig.getDeletedSegmentsCacheTtlMinutes(), TimeUnit.MINUTES).build();

    _peerDownloadScheme = tableConfig.getValidationConfig().getPeerSegmentDownloadScheme();
    if (_peerDownloadScheme == null) {
      _peerDownloadScheme = instanceDataManagerConfig.getSegmentPeerDownloadScheme();
    }
    if (_peerDownloadScheme != null) {
      _peerDownloadScheme = _peerDownloadScheme.toLowerCase();
      Preconditions.checkState(
          CommonConstants.HTTP_PROTOCOL.equals(_peerDownloadScheme) || CommonConstants.HTTPS_PROTOCOL.equals(
              _peerDownloadScheme), "Unsupported peer download scheme: %s for table: %s", _peerDownloadScheme,
          _tableNameWithType);
    }

    _streamSegmentDownloadUntarRateLimitBytesPerSec =
        instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit();
    _isStreamSegmentDownloadUntar = instanceDataManagerConfig.isStreamSegmentDownloadUntar();
    if (_isStreamSegmentDownloadUntar) {
      LOGGER.info("Using streamed download-untar for segment download! "
              + "The rate limit interval for streamed download-untar is {} bytes/s",
          _streamSegmentDownloadUntarRateLimitBytesPerSec);
    }
    int maxParallelSegmentDownloads = instanceDataManagerConfig.getMaxParallelSegmentDownloads();
    if (maxParallelSegmentDownloads > 0) {
      LOGGER.info(
          "Construct segment download semaphore for Table: {}. Maximum number of parallel segment downloads: {}",
          _tableNameWithType, maxParallelSegmentDownloads);
      _segmentDownloadSemaphore = new Semaphore(maxParallelSegmentDownloads, true);
    } else {
      _segmentDownloadSemaphore = null;
    }
    _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + getClass().getSimpleName());

    doInit();

    _logger.info("Initialized table data manager with data directory: {}", _tableDataDir);
  }

  protected abstract void doInit();

  @Override
  public String getInstanceId() {
    return _instanceId;
  }

  @Override
  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  @Override
  public synchronized void start() {
    _logger.info("Starting table data manager");
    doStart();
    _logger.info("Started table data manager");
  }

  protected abstract void doStart();

  @Override
  public synchronized void shutDown() {
    if (_shutDown) {
      _logger.warn("Table data manager is already shut down");
      return;
    }
    _logger.info("Shutting down table data manager");
    _shutDown = true;
    doShutdown();
    _logger.info("Shut down table data manager");
  }

  protected abstract void doShutdown();

  /**
   * Releases and removes all segments tracked by the table data manager.
   */
  protected void releaseAndRemoveAllSegments() {
    List<SegmentDataManager> segmentDataManagers;
    synchronized (_segmentDataManagerMap) {
      segmentDataManagers = new ArrayList<>(_segmentDataManagerMap.values());
      _segmentDataManagerMap.clear();
    }
    if (!segmentDataManagers.isEmpty()) {
      int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), segmentDataManagers.size());
      ExecutorService stopExecutorService = Executors.newFixedThreadPool(numThreads);
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        stopExecutorService.submit(() -> {
          segmentDataManager.offload();
          releaseSegment(segmentDataManager);
        });
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
  }

  @Override
  public boolean isShutDown() {
    return _shutDown;
  }

  @Override
  public Lock getSegmentLock(String segmentName) {
    return _segmentLocks.getLock(_tableNameWithType, segmentName);
  }

  @Override
  public boolean hasSegment(String segmentName) {
    return _segmentDataManagerMap.containsKey(segmentName);
  }

  /**
   * {@inheritDoc}
   * <p>If one segment already exists with the same name, replaces it with the new one.
   * <p>Ensures that reference count of the old segment (if replaced) is reduced by 1, so that the last user of the old
   * segment (or the calling thread, if there are none) remove the segment.
   * <p>The new segment is added with reference count of 1, so that is never removed until a drop command comes through.
   *
   * @param immutableSegment Immutable segment to add
   */
  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    Preconditions.checkState(!_shutDown, "Table data manager is already shut down, cannot add segment: %s to table: %s",
        segmentName, _tableNameWithType);
    _logger.info("Adding immutable segment: {}", segmentName);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);

    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    SegmentDataManager oldSegmentManager = registerSegment(segmentName, newSegmentManager);
    if (oldSegmentManager == null) {
      _logger.info("Added new immutable segment: {}", segmentName);
    } else {
      _logger.info("Replaced immutable segment: {}", segmentName);
      oldSegmentManager.offload();
      releaseSegment(oldSegmentManager);
    }
  }

  @Override
  public void addOnlineSegment(String segmentName)
      throws Exception {
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot add ONLINE segment: %s to table: %s", segmentName,
        _tableNameWithType);
    _logger.info("Adding ONLINE segment: {}", segmentName);
    Lock segmentLock = getSegmentLock(segmentName);
    segmentLock.lock();
    try {
      doAddOnlineSegment(segmentName);
    } catch (Exception e) {
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while adding ONLINE segment", e));
      throw e;
    } finally {
      segmentLock.unlock();
    }
  }

  protected abstract void doAddOnlineSegment(String segmentName)
      throws Exception;

  @Override
  public SegmentZKMetadata fetchZKMetadata(String segmentName) {
    SegmentZKMetadata zkMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, _tableNameWithType, segmentName);
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for segment: %s of table: %s", segmentName,
        _tableNameWithType);
    return zkMetadata;
  }

  @Override
  public Pair<TableConfig, Schema> fetchTableConfigAndSchema() {
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, _tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", _tableNameWithType);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, tableConfig);
    // NOTE: Schema is mandatory for REALTIME table.
    if (tableConfig.getTableType() == TableType.REALTIME) {
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _tableNameWithType);
    }
    return Pair.of(tableConfig, schema);
  }

  @Override
  public IndexLoadingConfig getIndexLoadingConfig(TableConfig tableConfig, @Nullable Schema schema) {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_instanceDataManagerConfig, tableConfig, schema);
    indexLoadingConfig.setTableDataDir(_tableDataDir);
    indexLoadingConfig.setInstanceTierConfigs(_instanceDataManagerConfig.getTierConfigs());
    return indexLoadingConfig;
  }

  @Override
  public void addNewOnlineSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    _logger.info("Adding new ONLINE segment: {}", zkMetadata.getSegmentName());
    if (!tryLoadExistingSegment(zkMetadata, indexLoadingConfig)) {
      downloadAndLoadSegment(zkMetadata, indexLoadingConfig);
    }
  }

  /**
   * Replaces an already loaded segment in a table if the segment has been overridden in the deep store (CRC mismatch).
   */
  protected void replaceSegmentIfCrcMismatch(SegmentDataManager segmentDataManager, SegmentZKMetadata zkMetadata,
      IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    String segmentName = segmentDataManager.getSegmentName();
    Preconditions.checkState(segmentDataManager instanceof ImmutableSegmentDataManager,
        "Cannot replace CONSUMING segment: %s in table: %s", segmentName, _tableNameWithType);
    SegmentMetadata localMetadata = segmentDataManager.getSegment().getSegmentMetadata();
    if (hasSameCRC(zkMetadata, localMetadata)) {
      _logger.info("Segment: {} has CRC: {} same as before, not replacing it", segmentName, localMetadata.getCrc());
      return;
    }
    _logger.info("Replacing segment: {} because its CRC has changed from: {} to: {}", segmentName,
        localMetadata.getCrc(), zkMetadata.getCrc());
    downloadAndLoadSegment(zkMetadata, indexLoadingConfig);
    _logger.info("Replaced segment: {} with new CRC: {}", segmentName, zkMetadata.getCrc());
  }

  @Override
  public void downloadAndLoadSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    String segmentName = zkMetadata.getSegmentName();
    _logger.info("Downloading and loading segment: {}", segmentName);
    File indexDir = downloadSegment(zkMetadata);
    addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig));
    _logger.info("Downloaded and loaded segment: {} with CRC: {} on tier: {}", segmentName, zkMetadata.getCrc(),
        TierConfigUtils.normalizeTierName(zkMetadata.getTier()));
  }

  @Override
  public void replaceSegment(String segmentName)
      throws Exception {
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot replace segment: %s in table: %s", segmentName,
        _tableNameWithType);
    _logger.info("Replacing segment: {}", segmentName);
    Lock segmentLock = getSegmentLock(segmentName);
    segmentLock.lock();
    try {
      doReplaceSegment(segmentName);
    } catch (Exception e) {
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while replacing segment", e));
      throw e;
    } finally {
      segmentLock.unlock();
    }
  }

  protected void doReplaceSegment(String segmentName)
      throws Exception {
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager != null) {
      SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
      IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
      indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
      replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
    } else {
      _logger.warn("Failed to find segment: {}, skipping replacing it", segmentName);
    }
  }

  @Override
  public void offloadSegment(String segmentName) {
    // NOTE: Do not throw exception when data manager has been shut down. This is regular flow when a table is deleted.
    if (_shutDown) {
      _logger.info("Table data manager is already shut down, skipping offloading segment: {}", segmentName);
      return;
    }
    _logger.info("Offloading segment: {}", segmentName);
    Lock segmentLock = getSegmentLock(segmentName);
    segmentLock.lock();
    try {
      doOffloadSegment(segmentName);
    } catch (Exception e) {
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while offloading segment", e));
      throw e;
    } finally {
      segmentLock.unlock();
    }
  }

  @Override
  public void offloadSegmentUnsafe(String segmentName) {
    if (_shutDown) {
      _logger.info("Table data manager is already shut down, skipping offloading segment: {} unsafe", segmentName);
      return;
    }
    _logger.info("Offloading segment: {} unsafe", segmentName);
    try {
      doOffloadSegment(segmentName);
    } catch (Exception e) {
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while offloading segment unsafe", e));
      throw e;
    }
  }

  protected void doOffloadSegment(String segmentName) {
    SegmentDataManager segmentDataManager = unregisterSegment(segmentName);
    if (segmentDataManager != null) {
      segmentDataManager.offload();
      releaseSegment(segmentDataManager);
      _logger.info("Offloaded segment: {}", segmentName);
    } else {
      _logger.warn("Failed to find segment: {}, skipping offloading it", segmentName);
    }
  }

  /**
   * Returns true if the given segment has been deleted recently. The time range is determined by
   * {@link InstanceDataManagerConfig#getDeletedSegmentsCacheTtlMinutes()}.
   */
  @Override
  public boolean isSegmentDeletedRecently(String segmentName) {
    return _recentlyDeletedSegments.getIfPresent(segmentName) != null;
  }

  @Override
  public List<SegmentDataManager> acquireAllSegments() {
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (SegmentDataManager segmentDataManager : _segmentDataManagerMap.values()) {
      if (segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
      }
    }
    return segmentDataManagers;
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentNames, List<String> missingSegments) {
    return acquireSegments(segmentNames, null, missingSegments);
  }

  @Override
  public List<SegmentDataManager> acquireSegments(List<String> segmentNames,
      @Nullable List<String> optionalSegmentNames, List<String> missingSegments) {
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (String segmentName : segmentNames) {
      SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
      if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
      } else {
        missingSegments.add(segmentName);
      }
    }
    if (optionalSegmentNames != null) {
      for (String segmentName : optionalSegmentNames) {
        SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
        // Optional segments are not counted to missing segments that are reported back in query exception.
        if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
          segmentDataManagers.add(segmentDataManager);
        }
      }
    }
    return segmentDataManagers;
  }

  @Nullable
  @Override
  public SegmentDataManager acquireSegment(String segmentName) {
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
      return segmentDataManager;
    } else {
      return null;
    }
  }

  @Override
  public void releaseSegment(SegmentDataManager segmentDataManager) {
    if (segmentDataManager.decreaseReferenceCount()) {
      closeSegment(segmentDataManager);
    }
  }

  private void closeSegment(SegmentDataManager segmentDataManager) {
    String segmentName = segmentDataManager.getSegmentName();
    _logger.info("Closing segment: {}", segmentName);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, -1L);
    _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_SEGMENT_COUNT, 1L);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        -segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs());
    segmentDataManager.destroy();
    _logger.info("Closed segment: {}", segmentName);
  }

  @Override
  public int getNumSegments() {
    return _segmentDataManagerMap.size();
  }

  @Override
  public String getTableName() {
    return _tableNameWithType;
  }

  @Override
  public File getTableDataDir() {
    return _indexDir;
  }

  @Override
  public HelixManager getHelixManager() {
    return _helixManager;
  }

  @Override
  public ExecutorService getSegmentPreloadExecutor() {
    return _segmentPreloadExecutor;
  }

  @Override
  public void addSegmentError(String segmentName, SegmentErrorInfo segmentErrorInfo) {
    if (_errorCache != null) {
      _errorCache.put(Pair.of(_tableNameWithType, segmentName), segmentErrorInfo);
    }
  }

  @Override
  public Map<String, SegmentErrorInfo> getSegmentErrors() {
    if (_errorCache != null) {
      // Filter out entries that match the table name
      Map<String, SegmentErrorInfo> segmentErrors = new HashMap<>();
      for (Map.Entry<Pair<String, String>, SegmentErrorInfo> entry : _errorCache.asMap().entrySet()) {
        Pair<String, String> tableSegmentPair = entry.getKey();
        if (tableSegmentPair.getLeft().equals(_tableNameWithType)) {
          segmentErrors.put(tableSegmentPair.getRight(), entry.getValue());
        }
      }
      return segmentErrors;
    } else {
      return Map.of();
    }
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    List<SegmentContext> segmentContexts = new ArrayList<>(selectedSegments.size());
    selectedSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    return segmentContexts;
  }

  @Override
  public void reloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      SegmentMetadata localMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot reload segment: %s of table: %s", segmentName,
        _tableNameWithType);
    _logger.info("Reloading segment: {}", segmentName);
    String segmentTier = getSegmentCurrentTier(segmentName);
    indexLoadingConfig.setSegmentTier(segmentTier);
    indexLoadingConfig.setTableDataDir(_tableDataDir);
    indexLoadingConfig.setInstanceTierConfigs(_instanceDataManagerConfig.getTierConfigs());
    File indexDir = getSegmentDataDir(segmentName, segmentTier, indexLoadingConfig.getTableConfig());
    Lock segmentLock = getSegmentLock(segmentName);
    segmentLock.lock();
    try {
      // Download segment from deep store if CRC changes or forced to download;
      // otherwise, copy backup directory back to the original index directory.
      // And then continue to load the segment from the index directory.
      boolean shouldDownload = forceDownload || !hasSameCRC(zkMetadata, localMetadata);
      if (shouldDownload) {
        // Create backup directory to handle failure of segment reloading.
        createBackup(indexDir);
        if (forceDownload) {
          _logger.info("Force downloading segment: {}", segmentName);
        } else {
          _logger.info("Downloading segment: {} because its CRC has changed from: {} to: {}", segmentName,
              localMetadata.getCrc(), zkMetadata.getCrc());
        }
        indexDir = downloadSegment(zkMetadata);
      } else {
        _logger.info("Reloading existing segment: {} on tier: {}", segmentName,
            TierConfigUtils.normalizeTierName(segmentTier));
        SegmentDirectory segmentDirectory =
            initSegmentDirectory(segmentName, String.valueOf(zkMetadata.getCrc()), indexLoadingConfig);
        // We should first try to reuse existing segment directory
        if (canReuseExistingDirectoryForReload(zkMetadata, segmentTier, segmentDirectory, indexLoadingConfig, schema)) {
          _logger.info("Reloading segment: {} using existing segment directory as no reprocessing needed", segmentName);
          // No reprocessing needed, reuse the same segment
          ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDirectory, indexLoadingConfig, schema);
          addSegment(segment);
          return;
        }
        // Create backup directory to handle failure of segment reloading.
        createBackup(indexDir);
        // The indexDir is empty after calling createBackup, as it's renamed to a backup directory.
        // The SegmentDirectory should initialize accordingly. Like for SegmentLocalFSDirectory, it
        // doesn't load anything from an empty indexDir, but gets the info to complete the copyTo.
        try {
          segmentDirectory.copyTo(indexDir);
        } finally {
          segmentDirectory.close();
        }
      }

      // Load from indexDir and replace the old segment in memory. What's inside indexDir
      // may come from SegmentDirectory.copyTo() or the segment downloaded from deep store.
      indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
      _logger.info("Loading segment: {} from indexDir: {} to tier: {}", segmentName, indexDir,
          TierConfigUtils.normalizeTierName(zkMetadata.getTier()));
      ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, schema);
      addSegment(segment);

      // Remove backup directory to mark the completion of segment reloading.
      removeBackup(indexDir);
    } catch (Exception reloadFailureException) {
      try {
        LoaderUtils.reloadFailureRecovery(indexDir);
      } catch (Exception recoveryFailureException) {
        _logger.error("Failed to recover segment: {} after reload failure", segmentName, recoveryFailureException);
        reloadFailureException.addSuppressed(recoveryFailureException);
      }
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while reloading segment",
              reloadFailureException));
      throw reloadFailureException;
    } finally {
      segmentLock.unlock();
    }
    _logger.info("Reloaded segment: {}", segmentName);
  }

  private boolean canReuseExistingDirectoryForReload(SegmentZKMetadata segmentZKMetadata, String currentSegmentTier,
      SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig, Schema schema)
      throws Exception {
    SegmentDirectoryLoader segmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    return !segmentDirectoryLoader.needsTierMigration(segmentZKMetadata.getTier(), currentSegmentTier)
        && !ImmutableSegmentLoader.needPreprocess(segmentDirectory, indexLoadingConfig, schema);
  }

  /**
   * _segmentDataManagerMap is used for fetching segments that need to be queried. If a new segment is created,
   * calling this method ensures that all queries in the future can use the new segment. This method may replace an
   * existing segment with the same name.
   */
  @Nullable
  protected SegmentDataManager registerSegment(String segmentName, SegmentDataManager segmentDataManager) {
    SegmentDataManager oldSegmentDataManager;
    synchronized (_segmentDataManagerMap) {
      oldSegmentDataManager = _segmentDataManagerMap.put(segmentName, segmentDataManager);
    }
    _recentlyDeletedSegments.invalidate(segmentName);
    return oldSegmentDataManager;
  }

  /**
   * De-registering a segment ensures that no query uses the given segment until a segment with that name is
   * re-registered. There may be scenarios where the broker thinks that a segment is available even though it has
   * been de-registered in the servers (either due to manual deletion or retention). In such cases, acquireSegments
   * will mark those segments as missingSegments. The caller can use {@link #isSegmentDeletedRecently(String)} to
   * identify this scenario.
   */
  @Nullable
  protected SegmentDataManager unregisterSegment(String segmentName) {
    _recentlyDeletedSegments.put(segmentName, segmentName);
    synchronized (_segmentDataManagerMap) {
      return _segmentDataManagerMap.remove(segmentName);
    }
  }

  /**
   * Downloads an immutable segment into the index directory.
   * Segment can be downloaded from deep store or from peer servers. Downloaded segment might be compressed or
   * encrypted, and this method takes care of decompressing and decrypting the segment.
   */
  protected File downloadSegment(SegmentZKMetadata zkMetadata)
      throws Exception {
    String segmentName = zkMetadata.getSegmentName();
    String downloadUrl = zkMetadata.getDownloadUrl();
    Preconditions.checkState(downloadUrl != null,
        "Failed to find download URL in ZK metadata for segment: %s of table: %s", segmentName, _tableNameWithType);
    try {
      if (!CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD.equals(downloadUrl)) {
        try {
          return downloadSegmentFromDeepStore(zkMetadata);
        } catch (Exception e) {
          if (_peerDownloadScheme != null) {
            _logger.warn("Caught exception while downloading segment: {} from: {}, trying to download from peers",
                segmentName, downloadUrl, e);
            return downloadSegmentFromPeers(zkMetadata);
          } else {
            throw e;
          }
        }
      } else {
        return downloadSegmentFromPeers(zkMetadata);
      }
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_FAILURES, 1);
      throw e;
    }
  }

  private File downloadSegmentFromDeepStore(SegmentZKMetadata zkMetadata)
      throws Exception {
    String segmentName = zkMetadata.getSegmentName();
    String downloadUrl = zkMetadata.getDownloadUrl();
    _logger.info("Downloading segment: {} from: {}", segmentName, downloadUrl);
    File tempRootDir = getTmpSegmentDataDir("tmp-" + segmentName + "-" + UUID.randomUUID());
    if (_segmentDownloadSemaphore != null) {
      long startTime = System.currentTimeMillis();
      _logger.info("Acquiring segment download semaphore for segment: {}, queue-length: {} ", segmentName,
          _segmentDownloadSemaphore.getQueueLength());
      _segmentDownloadSemaphore.acquire();
      _logger.info("Acquired segment download semaphore for segment: {} (lock-time={}ms, queue-length={}).",
          segmentName, System.currentTimeMillis() - startTime, _segmentDownloadSemaphore.getQueueLength());
    }
    try {
      File untarredSegmentDir;
      if (_isStreamSegmentDownloadUntar && zkMetadata.getCrypterName() == null) {
        _logger.info("Downloading segment: {} using streamed download-untar with maxStreamRateInByte: {}", segmentName,
            _streamSegmentDownloadUntarRateLimitBytesPerSec);
        AtomicInteger failedAttempts = new AtomicInteger(0);
        try {
          untarredSegmentDir = SegmentFetcherFactory.fetchAndStreamUntarToLocal(downloadUrl, tempRootDir,
              _streamSegmentDownloadUntarRateLimitBytesPerSec, failedAttempts);
          _logger.info("Downloaded and untarred segment: {} from: {}, failed attempts: {}", segmentName, downloadUrl,
              failedAttempts.get());
        } finally {
          _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.SEGMENT_STREAMED_DOWNLOAD_UNTAR_FAILURES,
              failedAttempts.get());
        }
      } else {
        File segmentTarFile = new File(tempRootDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
        SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(downloadUrl, segmentTarFile, zkMetadata.getCrypterName());
        _logger.info("Downloaded tarred segment: {} from: {} to: {}, file length: {}", segmentName, downloadUrl,
            segmentTarFile, segmentTarFile.length());
        untarredSegmentDir = untarSegment(segmentName, segmentTarFile, tempRootDir);
      }
      File indexDir = moveSegment(segmentName, untarredSegmentDir);
      _logger.info("Downloaded segment: {} from: {} to: {}", segmentName, downloadUrl, indexDir);
      return indexDir;
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_FROM_REMOTE_FAILURES, 1);
      throw e;
    } finally {
      if (_segmentDownloadSemaphore != null) {
        _segmentDownloadSemaphore.release();
      }
      FileUtils.deleteQuietly(tempRootDir);
    }
  }

  private File downloadSegmentFromPeers(SegmentZKMetadata zkMetadata)
      throws Exception {
    String segmentName = zkMetadata.getSegmentName();
    Preconditions.checkState(_peerDownloadScheme != null, "Peer download is not enabled for table: %s",
        _tableNameWithType);
    _logger.info("Downloading segment: {} from peers", segmentName);
    File tempRootDir = getTmpSegmentDataDir("tmp-" + segmentName + "-" + UUID.randomUUID());
    File segmentTarFile = new File(tempRootDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    try {
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(segmentName, _peerDownloadScheme, () -> {
        List<URI> peerServerURIs =
            PeerServerSegmentFinder.getPeerServerURIs(_helixManager, _tableNameWithType, segmentName,
                _peerDownloadScheme);
        Collections.shuffle(peerServerURIs);
        return peerServerURIs;
      }, segmentTarFile, zkMetadata.getCrypterName());
      _logger.info("Downloaded tarred segment: {} from peers to: {}, file length: {}", segmentName, segmentTarFile,
          segmentTarFile.length());
      File indexDir = untarAndMoveSegment(segmentName, segmentTarFile, tempRootDir);
      _logger.info("Downloaded segment: {} from peers to: {}", segmentName, indexDir);
      return indexDir;
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_FROM_PEERS_FAILURES, 1);
      throw e;
    } finally {
      FileUtils.deleteQuietly(tempRootDir);
    }
  }

  private File untarSegment(String segmentName, File segmentTarFile, File tempRootDir)
      throws IOException {
    File untarDir = new File(tempRootDir, segmentName);
    _logger.info("Untarring segment: {} from: {} to: {}", segmentName, segmentTarFile, untarDir);
    try {
      // If an exception is thrown when untarring, it means the tar file is broken or not found after the retry. Thus,
      // there's no need to retry again.
      File untarredSegmentDir = TarGzCompressionUtils.untar(segmentTarFile, untarDir).get(0);
      _logger.info("Untarred segment: {} into: {}", segmentName, untarredSegmentDir);
      return untarredSegmentDir;
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.UNTAR_FAILURES, 1);
      throw e;
    }
  }

  private File moveSegment(String segmentName, File untarredSegmentDir)
      throws IOException {
    File indexDir = getSegmentDataDir(segmentName);
    try {
      FileUtils.deleteDirectory(indexDir);
      FileUtils.moveDirectory(untarredSegmentDir, indexDir);
      return indexDir;
    } catch (Exception e) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.SEGMENT_DIR_MOVEMENT_FAILURES, 1);
      throw e;
    }
  }

  @VisibleForTesting
  File untarAndMoveSegment(String segmentName, File segmentTarFile, File tempRootDir)
      throws IOException {
    return moveSegment(segmentName, untarSegment(segmentName, segmentTarFile, tempRootDir));
  }

  @VisibleForTesting
  File getSegmentDataDir(String segmentName) {
    return new File(_indexDir, segmentName);
  }

  @Override
  public File getSegmentDataDir(String segmentName, @Nullable String segmentTier, TableConfig tableConfig) {
    if (segmentTier == null) {
      return getSegmentDataDir(segmentName);
    }
    String tierDataDir =
        TierConfigUtils.getDataDirForTier(tableConfig, segmentTier, _instanceDataManagerConfig.getTierConfigs());
    if (StringUtils.isEmpty(tierDataDir)) {
      return getSegmentDataDir(segmentName);
    }
    File tierTableDataDir = new File(tierDataDir, _tableNameWithType);
    return new File(tierTableDataDir, segmentName);
  }

  @Nullable
  private String getSegmentCurrentTier(String segmentName) {
    SegmentDataManager segment = _segmentDataManagerMap.get(segmentName);
    if (segment != null && segment.getSegment() instanceof ImmutableSegment) {
      return ((ImmutableSegment) segment.getSegment()).getTier();
    }
    return null;
  }

  @VisibleForTesting
  protected File getTmpSegmentDataDir(String segmentName)
      throws IOException {
    File tmpDir = new File(_resourceTmpDir, segmentName);
    if (tmpDir.exists()) {
      FileUtils.deleteQuietly(tmpDir);
    }
    FileUtils.forceMkdir(tmpDir);
    return tmpDir;
  }

  /**
   * Create a backup directory to handle failure of segment reloading.
   * First rename index directory to segment backup directory so that original segment have all file
   * descriptors point to the segment backup directory to ensure original segment serves queries properly.
   * The original index directory is restored lazily, as depending on the conditions,
   * it may be restored from the backup directory or segment downloaded from deep store.
   */
  private void createBackup(File indexDir) {
    if (!indexDir.exists()) {
      return;
    }
    File parentDir = indexDir.getParentFile();
    File segmentBackupDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    // Rename index directory to segment backup directory (atomic).
    Preconditions.checkState(indexDir.renameTo(segmentBackupDir),
        "Failed to rename index directory: %s to segment backup directory: %s", indexDir, segmentBackupDir);
  }

  /**
   * Remove the backup directory to mark the completion of segment reloading.
   * First rename then delete is as renaming is an atomic operation, but deleting is not.
   * When we rename the segment backup directory to segment temporary directory, we know the reload
   * already succeeded, so that we can safely delete the segment temporary directory.
   */
  private void removeBackup(File indexDir)
      throws IOException {
    File parentDir = indexDir.getParentFile();
    File segmentBackupDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_BACKUP_DIR_SUFFIX);
    if (!segmentBackupDir.exists()) {
      return;
    }
    // Rename segment backup directory to segment temporary directory (atomic).
    File segmentTempDir = new File(parentDir, indexDir.getName() + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);
    Preconditions.checkState(segmentBackupDir.renameTo(segmentTempDir),
        "Failed to rename segment backup directory: %s to segment temporary directory: %s", segmentBackupDir,
        segmentTempDir);
    FileUtils.deleteDirectory(segmentTempDir);
  }

  @Override
  public boolean tryLoadExistingSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    String segmentName = zkMetadata.getSegmentName();
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot load existing segment: %s of table: %s", segmentName,
        _tableNameWithType);

    // Try to recover the segment from potential segment reloading failure.
    String segmentTier = zkMetadata.getTier();
    File indexDir = getSegmentDataDir(segmentName, segmentTier, indexLoadingConfig.getTableConfig());
    recoverReloadFailureQuietly(_tableNameWithType, segmentName, indexDir);

    // Creates the SegmentDirectory object to access the segment metadata.
    // The metadata is null if the segment doesn't exist yet.
    SegmentDirectory segmentDirectory =
        tryInitSegmentDirectory(segmentName, String.valueOf(zkMetadata.getCrc()), indexLoadingConfig);
    SegmentMetadataImpl segmentMetadata = (segmentDirectory == null) ? null : segmentDirectory.getSegmentMetadata();

    // If the segment doesn't exist on server or its CRC has changed, then we
    // need to fall back to download the segment from deep store to load it.
    if (segmentMetadata == null || !hasSameCRC(zkMetadata, segmentMetadata)) {
      if (segmentMetadata == null) {
        _logger.info("Segment: {} does not exist", segmentName);
      } else if (!hasSameCRC(zkMetadata, segmentMetadata)) {
        _logger.info("Segment: {} has CRC changed from: {} to: {}", segmentName, segmentMetadata.getCrc(),
            zkMetadata.getCrc());
      }
      closeSegmentDirectoryQuietly(segmentDirectory);
      return false;
    }

    try {
      // If the segment is still kept by the server, then we can
      // either load it directly if it's still consistent with latest table config and schema;
      // or reprocess it to reflect latest table config and schema before loading.
      Schema schema = indexLoadingConfig.getSchema();
      if (!ImmutableSegmentLoader.needPreprocess(segmentDirectory, indexLoadingConfig, schema)) {
        _logger.info("Segment: {} is consistent with latest table config and schema", segmentName);
      } else {
        _logger.info("Segment: {} needs reprocess to reflect latest table config and schema", segmentName);
        segmentDirectory.copyTo(indexDir);
        // Close the stale SegmentDirectory object and recreate it with reprocessed segment.
        closeSegmentDirectoryQuietly(segmentDirectory);
        ImmutableSegmentLoader.preprocess(indexDir, indexLoadingConfig, schema);
        segmentDirectory = initSegmentDirectory(segmentName, String.valueOf(zkMetadata.getCrc()), indexLoadingConfig);
      }
      ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDirectory, indexLoadingConfig, schema);
      addSegment(segment);
      _logger.info("Loaded existing segment: {} with CRC: {} on tier: {}", segmentName, zkMetadata.getCrc(),
          TierConfigUtils.normalizeTierName(segmentTier));
      return true;
    } catch (Exception e) {
      _logger.error("Failed to load existing segment: {} with CRC: {} on tier: {}", segmentName, zkMetadata.getCrc(),
          TierConfigUtils.normalizeTierName(segmentTier), e);
      closeSegmentDirectoryQuietly(segmentDirectory);
      return false;
    }
  }

  @Nullable
  private SegmentDirectory tryInitSegmentDirectory(String segmentName, String segmentCrc,
      IndexLoadingConfig indexLoadingConfig) {
    try {
      return initSegmentDirectory(segmentName, segmentCrc, indexLoadingConfig);
    } catch (Exception e) {
      _logger.warn("Failed to initialize SegmentDirectory for segment: {} with error: {}", segmentName, e.getMessage());
      return null;
    }
  }

  private SegmentDirectory initSegmentDirectory(String segmentName, String segmentCrc,
      IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    SegmentDirectoryLoaderContext loaderContext =
        new SegmentDirectoryLoaderContext.Builder().setTableConfig(indexLoadingConfig.getTableConfig())
            .setSchema(indexLoadingConfig.getSchema()).setInstanceId(indexLoadingConfig.getInstanceId())
            .setTableDataDir(indexLoadingConfig.getTableDataDir()).setSegmentName(segmentName).setSegmentCrc(segmentCrc)
            .setSegmentTier(indexLoadingConfig.getSegmentTier())
            .setInstanceTierConfigs(indexLoadingConfig.getInstanceTierConfigs())
            .setSegmentDirectoryConfigs(indexLoadingConfig.getSegmentDirectoryConfigs()).build();
    SegmentDirectoryLoader segmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    File indexDir =
        getSegmentDataDir(segmentName, indexLoadingConfig.getSegmentTier(), indexLoadingConfig.getTableConfig());
    return segmentDirectoryLoader.load(indexDir.toURI(), loaderContext);
  }

  private static boolean hasSameCRC(SegmentZKMetadata zkMetadata, SegmentMetadata localMetadata) {
    return zkMetadata.getCrc() == Long.parseLong(localMetadata.getCrc());
  }

  private static void recoverReloadFailureQuietly(String tableNameWithType, String segmentName, File indexDir) {
    try {
      LoaderUtils.reloadFailureRecovery(indexDir);
    } catch (Exception e) {
      LOGGER.warn("Failed to recover segment: {} of table: {} due to error: {}", segmentName, tableNameWithType,
          e.getMessage());
    }
  }

  private static void closeSegmentDirectoryQuietly(SegmentDirectory segmentDirectory) {
    if (segmentDirectory != null) {
      try {
        segmentDirectory.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close SegmentDirectory due to error: {}", e.getMessage());
      }
    }
  }
}
