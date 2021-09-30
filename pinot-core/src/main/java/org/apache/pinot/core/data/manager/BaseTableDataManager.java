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
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
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
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
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
  public List<SegmentDataManager> acquireSegments(List<String> segmentNames) {
    List<SegmentDataManager> segmentDataManagers = new ArrayList<>();
    for (String segmentName : segmentNames) {
      SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
      if (segmentDataManager != null && segmentDataManager.increaseReferenceCount()) {
        segmentDataManagers.add(segmentDataManager);
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
      SegmentMetadata localMetadata, @Nullable Schema schema, boolean forceDownload)
      throws Exception {
    File indexDir = localMetadata.getIndexDir();
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

      // Download from remote or copy from local backup directory into index directory,
      // and then continue to load the segment from index directory.
      boolean shouldDownload = forceDownload || !hasSameCRC(zkMetadata, localMetadata);
      if (shouldDownload && allowDownload(segmentName, zkMetadata)) {
        if (forceDownload) {
          LOGGER.info("Segment: {} of table: {} is forced to download", segmentName, _tableNameWithType);
        } else {
          LOGGER.info("Download segment:{} of table: {} as local crc: {} mismatches remote crc: {}", segmentName,
              _tableNameWithType, localMetadata.getCrc(), zkMetadata.getCrc());
        }
        indexDir = downloadSegment(segmentName, zkMetadata);
      } else {
        LOGGER.info("Reload the local copy of segment: {} of table: {}", segmentName, _tableNameWithType);
        FileUtils.copyDirectory(segmentBackupDir, indexDir);
      }

      // Load from index directory and replace the old segment in memory.
      addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, schema));

      // Rename segment backup directory to segment temporary directory (atomic)
      // The reason to first rename then delete is that, renaming is an atomic operation, but deleting is not. When we
      // rename the segment backup directory to segment temporary directory, we know the reload already succeeded, so
      // that we can safely delete the segment temporary directory
      File segmentTempDir = new File(parentFile, indexDir.getName() + CommonConstants.Segment.SEGMENT_TEMP_DIR_SUFFIX);
      Preconditions.checkState(segmentBackupDir.renameTo(segmentTempDir),
          "Failed to rename segment backup directory: %s to segment temporary directory: %s", segmentBackupDir,
          segmentTempDir);
      FileUtils.deleteDirectory(segmentTempDir);
    } catch (Exception reloadFailureException) {
      try {
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
      SegmentZKMetadata zkMetadata, @Nullable SegmentMetadata localMetadata)
      throws Exception {
    if (!isNewSegment(zkMetadata, localMetadata)) {
      LOGGER.info("Segment: {} of table: {} has crc: {} same as before, already loaded, do nothing", segmentName,
          _tableNameWithType, localMetadata.getCrc());
      return;
    }

    // Try to recover if no local metadata is provided.
    if (localMetadata == null) {
      LOGGER.info("Segment: {} of table: {} is not loaded, checking disk", segmentName, _tableNameWithType);
      localMetadata = recoverSegmentQuietly(segmentName);
      if (!isNewSegment(zkMetadata, localMetadata)) {
        LOGGER.info("Segment: {} of table {} has crc: {} same as before, loading", segmentName, _tableNameWithType,
            localMetadata.getCrc());
        if (loadSegmentQuietly(segmentName, indexLoadingConfig)) {
          return;
        }
        // Set local metadata to null to indicate that the local segment fails to load,
        // although it exists and has same crc with the remote one.
        localMetadata = null;
      }
    }

    Preconditions.checkState(allowDownload(segmentName, zkMetadata), "Segment: %s of table: %s does not allow download",
        segmentName, _tableNameWithType);

    // Download segment and replace the local one, either due to failure to recover local segment,
    // or the segment data is updated and has new CRC now.
    if (localMetadata == null) {
      LOGGER.info("Download segment: {} of table: {} as no good one exists locally", segmentName, _tableNameWithType);
    } else {
      LOGGER.info("Download segment: {} of table: {} as local crc: {} mismatches remote crc: {}.", segmentName,
          _tableNameWithType, localMetadata.getCrc(), zkMetadata.getCrc());
    }
    File indexDir = downloadSegment(segmentName, zkMetadata);
    addSegment(indexDir, indexLoadingConfig);
    LOGGER.info("Downloaded and loaded segment: {} of table: {} with crc: {}", segmentName, _tableNameWithType,
        zkMetadata.getCrc());
  }

  protected boolean allowDownload(String segmentName, SegmentZKMetadata zkMetadata) {
    return true;
  }

  protected File downloadSegment(String segmentName, SegmentZKMetadata zkMetadata)
      throws Exception {
    // TODO: may support download from peer servers for RealTime table.
    return downloadSegmentFromDeepStore(segmentName, zkMetadata);
  }

  /**
   * Server restart during segment reload might leave segment directory in inconsistent state, like the index
   * directory might not exist but segment backup directory existed. This method tries to recover from reload
   * failure before checking the existence of the index directory and loading segment metadata from it.
   */
  private SegmentMetadata recoverSegmentQuietly(String segmentName) {
    File indexDir = getSegmentDataDir(segmentName);
    try {
      LoaderUtils.reloadFailureRecovery(indexDir);
      if (!indexDir.exists()) {
        LOGGER.info("Segment: {} of table: {} is not found on disk", segmentName, _tableNameWithType);
        return null;
      }
      SegmentMetadataImpl localMetadata = new SegmentMetadataImpl(indexDir);
      LOGGER.info("Segment: {} of table: {} with crc: {} from disk is ready for loading", segmentName,
          _tableNameWithType, localMetadata.getCrc());
      return localMetadata;
    } catch (Exception e) {
      LOGGER.error("Failed to recover segment: {} of table: {} from disk", segmentName, _tableNameWithType, e);
      FileUtils.deleteQuietly(indexDir);
      return null;
    }
  }

  private boolean loadSegmentQuietly(String segmentName, IndexLoadingConfig indexLoadingConfig) {
    File indexDir = getSegmentDataDir(segmentName);
    try {
      addSegment(indexDir, indexLoadingConfig);
      LOGGER.info("Loaded segment: {} of table: {} from disk", segmentName, _tableNameWithType);
      return true;
    } catch (Exception e) {
      FileUtils.deleteQuietly(indexDir);
      LOGGER.error("Failed to load segment: {} of table: {} from disk", segmentName, _tableNameWithType, e);
      return false;
    }
  }

  private File downloadSegmentFromDeepStore(String segmentName, SegmentZKMetadata zkMetadata)
      throws Exception {
    File tempRootDir = getSegmentDataDir("tmp-" + segmentName + "-" + UUID.randomUUID());
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
  static boolean isNewSegment(SegmentZKMetadata zkMetadata, @Nullable SegmentMetadata localMetadata) {
    return localMetadata == null || !hasSameCRC(zkMetadata, localMetadata);
  }

  private static boolean hasSameCRC(SegmentZKMetadata zkMetadata, SegmentMetadata localMetadata) {
    return zkMetadata.getCrc() == Long.parseLong(localMetadata.getCrc());
  }
}
