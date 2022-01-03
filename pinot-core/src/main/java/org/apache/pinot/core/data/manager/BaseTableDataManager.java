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
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManagerConfig;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.Pair;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseTableDataManager implements TableDataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTableDataManager.class);

  protected final ConcurrentHashMap<String, SegmentDataManager> _segmentDataManagerMap = new ConcurrentHashMap<>();

  protected TableDataManagerConfig _tableDataManagerConfig;
  protected String _instanceId;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected ServerMetrics _serverMetrics;
  protected String _tableNameWithType;
  protected String _tableDataDir;
  protected File _indexDir;
  protected File _resourceTmpDir;
  protected Logger _logger;
  protected HelixManager _helixManager;
  protected String _authToken;

  // Fixed size LRU cache with TableName - SegmentName pair as key, and segment related
  // errors as the value.
  protected LoadingCache<Pair<String, String>, SegmentErrorInfo> _errorCache;

  @Override
  public void init(TableDataManagerConfig tableDataManagerConfig, String instanceId,
      ZkHelixPropertyStore<ZNRecord> propertyStore, ServerMetrics serverMetrics, HelixManager helixManager,
      @Nullable LoadingCache<Pair<String, String>, SegmentErrorInfo> errorCache) {
    LOGGER.info("Initializing table data manager for table: {}", tableDataManagerConfig.getTableName());

    _tableDataManagerConfig = tableDataManagerConfig;
    _instanceId = instanceId;
    _propertyStore = propertyStore;
    _serverMetrics = serverMetrics;
    _helixManager = helixManager;
    _authToken = tableDataManagerConfig.getAuthToken();

    _tableNameWithType = tableDataManagerConfig.getTableName();
    _tableDataDir = tableDataManagerConfig.getDataDir();
    _indexDir = new File(_tableDataDir);
    if (!_indexDir.exists()) {
      Preconditions.checkState(_indexDir.mkdirs());
    }
    _resourceTmpDir = new File(_indexDir, "tmp");
    // This is meant to cleanup temp resources from TableDataManager. But other code using this same
    // directory will have those deleted as well.
    FileUtils.deleteQuietly(_resourceTmpDir);
    if (!_resourceTmpDir.exists()) {
      Preconditions.checkState(_resourceTmpDir.mkdirs());
    }
    _errorCache = errorCache;
    _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + getClass().getSimpleName());

    doInit();

    _logger.info("Initialized table data manager for table: {} with data directory: {}", _tableNameWithType,
        _tableDataDir);
  }

  protected abstract void doInit();

  @Override
  public void start() {
    _logger.info("Starting table data manager for table: {}", _tableNameWithType);
    doStart();
    _logger.info("Started table data manager for table: {}", _tableNameWithType);
  }

  protected abstract void doStart();

  @Override
  public void shutDown() {
    _logger.info("Shutting down table data manager for table: {}", _tableNameWithType);
    doShutdown();
    _logger.info("Shut down table data manager for table: {}", _tableNameWithType);
  }

  protected abstract void doShutdown();

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
    _logger.info("Adding immutable segment: {} to table: {}", segmentName, _tableNameWithType);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);

    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.put(segmentName, newSegmentManager);
    if (oldSegmentManager == null) {
      _logger.info("Added new immutable segment: {} to table: {}", segmentName, _tableNameWithType);
    } else {
      _logger.info("Replaced immutable segment: {} of table: {}", segmentName, _tableNameWithType);
      releaseSegment(oldSegmentManager);
    }
  }

  @Override
  public void addSegment(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSegment(String segmentName, TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when we get a helix transition to go to offline or dropped state.
   * We need to remove it safely, keeping in mind that there may be queries that are
   * using the segment,
   * @param segmentName name of the segment to remove.
   */
  @Override
  public void removeSegment(String segmentName) {
    _logger.info("Removing segment: {} from table: {}", segmentName, _tableNameWithType);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.remove(segmentName);
    if (segmentDataManager != null) {
      releaseSegment(segmentDataManager);
      _logger.info("Removed segment: {} from table: {}", segmentName, _tableNameWithType);
    } else {
      _logger.info("Failed to find segment: {} in table: {}", segmentName, _tableNameWithType);
    }
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
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (String segmentName : segmentNames) {
      SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
      if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
      } else {
        missingSegments.add(segmentName);
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
    _logger.info("Closing segment: {} of table: {}", segmentName, _tableNameWithType);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, -1L);
    _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_SEGMENT_COUNT, 1L);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        -segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs());
    segmentDataManager.destroy();
    _logger.info("Closed segment: {} of table: {}", segmentName, _tableNameWithType);
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
  public void addSegmentError(String segmentName, SegmentErrorInfo segmentErrorInfo) {
    _errorCache.put(new Pair<>(_tableNameWithType, segmentName), segmentErrorInfo);
  }

  @Override
  public Map<String, SegmentErrorInfo> getSegmentErrors() {
    if (_errorCache == null) {
      return Collections.emptyMap();
    } else {
      // Filter out entries that match the table name.
      return _errorCache.asMap().entrySet().stream().filter(map -> map.getKey().getFirst().equals(_tableNameWithType))
          .collect(Collectors.toMap(map -> map.getKey().getSecond(), Map.Entry::getValue));
    }
  }

  @Override
  public void reloadSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata zkMetadata,
      SegmentMetadata segmentMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    File indexDir = getSegmentDataDir(segmentName);
    // Create backup dir to make segment reloading atomic for local tier backend.
    LoaderUtils.createBackup(indexDir);
    try {
      boolean shouldDownloadRawSegment = forceDownload || !hasSameCRC(zkMetadata, segmentMetadata);
      if (shouldDownloadRawSegment && allowDownloadRawSegment(segmentName, zkMetadata)) {
        downloadRawSegmentAndProcess(segmentName, indexLoadingConfig, zkMetadata, schema);
      } else {
        downloadTierSegmentAndProcess(segmentName, indexLoadingConfig, schema);
      }
      // Remove backup dir to mark the completion of reloading for local tier backend.
      LoaderUtils.removeBackup(indexDir);
    } catch (Exception reloadFailureException) {
      try {
        // Recover the segment dir from backup dir for local tier backend.
        LoaderUtils.reloadFailureRecovery(indexDir);
      } catch (Exception recoveryFailureException) {
        LOGGER.error("Failed to recover after reload failure", recoveryFailureException);
        reloadFailureException.addSuppressed(recoveryFailureException);
      }
      throw reloadFailureException;
    }
  }

  @Override
  public void addOrReplaceSegment(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata, @Nullable SegmentMetadata segmentMetadata)
      throws Exception {
    // Non-null segment metadata means the segment has already been loaded.
    if (segmentMetadata != null) {
      if (hasSameCRC(zkMetadata, segmentMetadata)) {
        // Simply returns if the CRC hasn't changed. The table config may have changed
        // since segment is loaded, but that is handled by reloadSegment() method.
        LOGGER.info("Segment: {} of table: {} has crc: {} same as before, already loaded, do nothing", segmentName,
            _tableNameWithType, segmentMetadata.getCrc());
      } else {
        // Download the raw segment, reprocess and load it if the CRC has changed.
        LOGGER.info("Segment: {} of table: {} already loaded but its crc: {} differs from new crc: {}", segmentName,
            _tableNameWithType, segmentMetadata.getCrc(), zkMetadata.getCrc());
        downloadRawSegmentAndProcess(segmentName, indexLoadingConfig, zkMetadata,
            ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType));
      }
      return;
    }

    // For local tier backend, try to recover the segment from potential
    // reload failure. Continue upon any failure.
    File indexDir = getSegmentDataDir(segmentName);
    recoverReloadFailureQuietly(_tableNameWithType, segmentName, indexDir);

    // Creates the SegmentDirectory object to access the segment metadata that
    // may be from local tier backend or remote tier backend.
    SegmentDirectory segmentDirectory = null;
    try {
      SegmentDirectoryLoaderContext loaderContext =
          new SegmentDirectoryLoaderContext(indexLoadingConfig.getTableConfig(), indexLoadingConfig.getInstanceId(),
              segmentName, indexLoadingConfig.getSegmentDirectoryConfigs());
      SegmentDirectoryLoader segmentDirectoryLoader =
          SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
      segmentDirectory = segmentDirectoryLoader.load(indexDir.toURI(), loaderContext);
    } catch (Exception e) {
      LOGGER.warn("Failed to create SegmentDirectory for segment: {} of table: {} due to error: {}", segmentName,
          _tableNameWithType, e.getMessage());
    }

    // Download the raw segment, reprocess and load it if it is never loaded or its CRC has changed.
    if (segmentDirectory == null) {
      LOGGER.info("Segment: {} of table: {} does not exist", segmentName, _tableNameWithType);
      downloadRawSegmentAndProcess(segmentName, indexLoadingConfig, zkMetadata,
          ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType));
      return;
    }
    if (!hasSameCRC(zkMetadata, segmentDirectory.getSegmentMetadata())) {
      LOGGER.info("Segment: {} of table: {} has crc: {} different from new crc: {}", segmentName,
          _tableNameWithType, segmentDirectory.getSegmentMetadata().getCrc(), zkMetadata.getCrc());
      closeSegmentDirectoryQuietly(segmentDirectory);
      downloadRawSegmentAndProcess(segmentName, indexLoadingConfig, zkMetadata,
          ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType));
      return;
    }

    try {
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      if (!ImmutableSegmentLoader.needPreprocess(segmentDirectory, indexLoadingConfig, schema)) {
        // The loaded segment is still consistent with current table config or schema.
        LOGGER.info("Segment: {} of table: {} is consistent with table config and schema", segmentName,
            _tableNameWithType);
        addSegment(ImmutableSegmentLoader.load(segmentDirectory, indexLoadingConfig, schema));
        return;
      }
      // If any discrepancy is found, get the segment from tier backend, reprocess and load it.
      LOGGER.info("Segment: {} of table: {} needs reprocess to reflect latest table config and schema", segmentName,
          _tableNameWithType);
      // Close the stale SegmentDirectory object before loading the newly processed segment.
      closeSegmentDirectoryQuietly(segmentDirectory);
      downloadTierSegmentAndProcess(segmentName, indexLoadingConfig, schema);
    } catch (Exception e) {
      closeSegmentDirectoryQuietly(segmentDirectory);
      LOGGER.error("Failed to reprocess and load segment: {} of table: {}", segmentName, _tableNameWithType, e);
      downloadRawSegmentAndProcess(segmentName, indexLoadingConfig, zkMetadata,
          ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType));
    }
  }

  /**
   * Get the segment from the configured tier backend, reprocess it with the latest table
   * config and schema and then load it. Please note that the segment is from tier backend,
   * not deep store, for incremental processing.
   */
  private void downloadTierSegmentAndProcess(String segmentName, IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema)
      throws Exception {
    SegmentDirectoryLoaderContext loaderContext =
        new SegmentDirectoryLoaderContext(indexLoadingConfig.getTableConfig(), indexLoadingConfig.getInstanceId(),
            segmentName, indexLoadingConfig.getSegmentDirectoryConfigs());
    SegmentDirectoryLoader segmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    File indexDir = getSegmentDataDir(segmentName);
    segmentDirectoryLoader.download(indexDir, loaderContext);
    processAndLoadSegment(segmentDirectoryLoader, loaderContext, segmentName, indexDir, indexLoadingConfig, schema);
    LOGGER.info("Reprocessed tier segment: {} of table: {} and loaded it", segmentName, _tableNameWithType);
  }

  /**
   * This method downloads the raw segment from the deep store and process it, mainly
   * for cases where segment CRC has changed or the existing segment fails to load.
   */
  private void downloadRawSegmentAndProcess(String segmentName, IndexLoadingConfig indexLoadingConfig,
      SegmentZKMetadata zkMetadata, @Nullable Schema schema)
      throws Exception {
    Preconditions.checkState(allowDownloadRawSegment(segmentName, zkMetadata),
        "Segment: %s of table: %s does not allow download raw segment", segmentName, _tableNameWithType);

    SegmentDirectoryLoaderContext loaderContext =
        new SegmentDirectoryLoaderContext(indexLoadingConfig.getTableConfig(), indexLoadingConfig.getInstanceId(),
            segmentName, indexLoadingConfig.getSegmentDirectoryConfigs());
    SegmentDirectoryLoader segmentDirectoryLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    File indexDir = downloadRawSegment(segmentName, zkMetadata);
    processAndLoadSegment(segmentDirectoryLoader, loaderContext, segmentName, indexDir, indexLoadingConfig, schema);
    LOGGER.info("Downloaded raw segment: {} of table: {} with crc: {} and loaded it", segmentName,
        _tableNameWithType, zkMetadata.getCrc());
  }

  private void processAndLoadSegment(SegmentDirectoryLoader segmentDirectoryLoader,
      SegmentDirectoryLoaderContext loaderContext, String segmentName, File indexDir,
      IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema)
      throws Exception {
    // Preprocess the segment locally with current table config and schema.
    ImmutableSegmentLoader.preprocess(indexDir, indexLoadingConfig, schema);

    SegmentDirectory segmentDirectory = null;
    try {
      // Upload the processed segment to server tier backend, which can be local or remote.
      segmentDirectoryLoader.upload(indexDir, loaderContext);
      // Create the SegmentDirectory object with the newly processed segment from tier backend.
      segmentDirectory = segmentDirectoryLoader.load(indexDir.toURI(), loaderContext);
      addSegment(ImmutableSegmentLoader.load(segmentDirectory, indexLoadingConfig, schema));
    } catch (Exception e) {
      closeSegmentDirectoryQuietly(segmentDirectory);
      LOGGER.error("Failed to load newly processed segment: {} of table: {} due to error: {}", segmentName,
          _tableNameWithType, e.getMessage());
      throw e;
    }
  }

  protected boolean allowDownloadRawSegment(String segmentName, SegmentZKMetadata zkMetadata) {
    return true;
  }

  protected File downloadRawSegment(String segmentName, SegmentZKMetadata zkMetadata)
      throws Exception {
    // TODO: may support download from peer servers for RealTime table.
    return downloadSegmentFromDeepStore(segmentName, zkMetadata);
  }

  private File downloadSegmentFromDeepStore(String segmentName, SegmentZKMetadata zkMetadata)
      throws Exception {
    File tempRootDir = getTmpSegmentDataDir("tmp-" + segmentName + "-" + UUID.randomUUID());
    FileUtils.forceMkdir(tempRootDir);
    try {
      File tarFile = downloadAndDecrypt(segmentName, zkMetadata, tempRootDir);
      return untarAndMoveSegment(segmentName, tarFile, tempRootDir);
    } finally {
      FileUtils.deleteQuietly(tempRootDir);
    }
  }

  @VisibleForTesting
  File downloadAndDecrypt(String segmentName, SegmentZKMetadata zkMetadata, File tempRootDir)
      throws Exception {
    File tarFile = new File(tempRootDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    String uri = zkMetadata.getDownloadUrl();
    try {
      SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(uri, tarFile, zkMetadata.getCrypterName());
      LOGGER.info("Downloaded tarred segment: {} for table: {} from: {} to: {}, file length: {}", segmentName,
          _tableNameWithType, uri, tarFile, tarFile.length());
      return tarFile;
    } catch (AttemptsExceededException e) {
      LOGGER.error("Attempts exceeded when downloading segment: {} for table: {} from: {} to: {}", segmentName,
          _tableNameWithType, uri, tarFile);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.SEGMENT_DOWNLOAD_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  File untarAndMoveSegment(String segmentName, File tarFile, File tempRootDir)
      throws IOException {
    File untarDir = new File(tempRootDir, segmentName);
    try {
      // If an exception is thrown when untarring, it means the tar file is broken
      // or not found after the retry. Thus, there's no need to retry again.
      File untaredSegDir = TarGzCompressionUtils.untar(tarFile, untarDir).get(0);
      LOGGER.info("Uncompressed tar file: {} into target dir: {}", tarFile, untarDir);
      // Replace the existing index directory.
      File indexDir = getSegmentDataDir(segmentName);
      FileUtils.deleteDirectory(indexDir);
      FileUtils.moveDirectory(untaredSegDir, indexDir);
      LOGGER.info("Successfully downloaded segment: {} of table: {} to index dir: {}", segmentName, _tableNameWithType,
          indexDir);
      return indexDir;
    } catch (Exception e) {
      LOGGER.error("Failed to untar segment: {} of table: {} from: {} to: {}", segmentName, _tableNameWithType, tarFile,
          untarDir);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.UNTAR_FAILURES, 1L);
      throw e;
    }
  }

  @VisibleForTesting
  File getSegmentDataDir(String segmentName) {
    return new File(_indexDir, segmentName);
  }

  @VisibleForTesting
  protected File getTmpSegmentDataDir(String segmentName) {
    return new File(_resourceTmpDir, segmentName);
  }

  private static boolean hasSameCRC(SegmentZKMetadata zkMetadata, SegmentMetadata localMetadata) {
    return zkMetadata.getCrc() == Long.parseLong(localMetadata.getCrc());
  }

  private static void recoverReloadFailureQuietly(String tableNameWithType, String segmentName, File indexDir) {
    try {
      LoaderUtils.reloadFailureRecovery(indexDir);
    } catch (Exception e) {
      LOGGER.warn("Failed to recover segment: {} of table: {}", segmentName, tableNameWithType, e);
    }
  }

  private static void closeSegmentDirectoryQuietly(SegmentDirectory segmentDirectory) {
    if (segmentDirectory != null) {
      try {
        segmentDirectory.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }
}
