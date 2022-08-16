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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.tablestate.TableStateUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;

import static org.apache.pinot.spi.utils.CommonConstants.Segment.METADATA_URI_FOR_PEER_DOWNLOAD;


@ThreadSafe
public class RealtimeTableDataManager extends BaseTableDataManager {
  private final ExecutorService _segmentAsyncExecutorService =
      Executors.newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));
  private SegmentBuildTimeLeaseExtender _leaseExtender;
  private RealtimeSegmentStatsHistory _statsHistory;
  private final Semaphore _segmentBuildSemaphore;
  // Maintains a map of partitionGroup
  // Ids to semaphores.
  // The semaphore ensures that exactly one PartitionConsumer instance consumes from any stream partition.
  // In some streams, it's possible that having multiple consumers (with the same consumer name on the same host)
  // consuming from the same stream partition can lead to bugs.
  // The semaphores will stay in the hash map even if the consuming partitions move to a different host.
  // We expect that there will be a small number of semaphores, but that may be ok.
  private final Map<Integer, Semaphore> _partitionGroupIdToSemaphoreMap = new ConcurrentHashMap<>();

  // The old name of the stats file used to be stats.ser which we changed when we moved all packages
  // from com.linkedin to org.apache because of not being able to deserialize the old files using the newer classes
  private static final String STATS_FILE_NAME = "segment-stats.ser";
  private static final String CONSUMERS_DIR = "consumers";

  // Topics tend to have similar cardinality for values across partitions consumed during the same time.
  // Multiple partitions of a topic are likely to be consumed in each server, and these will tend to
  // transition from CONSUMING to ONLINE at roughly the same time. So, if we include statistics from
  // all partitions in the RealtimeSegmentStatsHistory we will over-weigh the most recent partition,
  // and cause spikes in memory allocation for dictionary. It is best to consider one partition as a sample
  // amongst the partitions that complete at the same time.
  //
  // It is not predictable which partitions, or how many partitions get allocated to a single server,
  // but it is likely that a partition takes more than half an hour to consume a full segment. So we set
  // the minimum interval between updates to RealtimeSegmentStatsHistory as 30 minutes. This way it is
  // likely that we get fresh data each time instead of multiple copies of roughly same data.
  private static final int MIN_INTERVAL_BETWEEN_STATS_UPDATES_MINUTES = 30;

  private final AtomicBoolean _allSegmentsLoaded = new AtomicBoolean();

  private TableDedupMetadataManager _tableDedupMetadataManager;
  private TableUpsertMetadataManager _tableUpsertMetadataManager;

  public RealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
    _segmentBuildSemaphore = segmentBuildSemaphore;
  }

  @Override
  protected void doInit() {
    _leaseExtender = SegmentBuildTimeLeaseExtender.getOrCreate(_instanceId, _serverMetrics, _tableNameWithType);

    File statsFile = new File(_tableDataDir, STATS_FILE_NAME);
    try {
      _statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);
    } catch (IOException | ClassNotFoundException e) {
      _logger.error("Error reading history object for table {} from {}", _tableNameWithType,
          statsFile.getAbsolutePath(), e);
      File savedFile = new File(_tableDataDir, STATS_FILE_NAME + "." + UUID.randomUUID());
      try {
        FileUtils.moveFile(statsFile, savedFile);
      } catch (IOException e1) {
        _logger.error("Could not move {} to {}", statsFile.getAbsolutePath(), savedFile.getAbsolutePath(), e1);
        throw new RuntimeException(e);
      }
      _logger.warn("Saved unreadable {} into {}. Creating a fresh instance", statsFile.getAbsolutePath(),
          savedFile.getAbsolutePath());
      try {
        _statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);
      } catch (Exception e2) {
        Utils.rethrowException(e2);
      }
    }
    _statsHistory.setMinIntervalBetweenUpdatesMillis(
        TimeUnit.MILLISECONDS.convert(MIN_INTERVAL_BETWEEN_STATS_UPDATES_MINUTES, TimeUnit.MINUTES));

    String consumerDirPath = getConsumerDir();
    File consumerDir = new File(consumerDirPath);
    if (consumerDir.exists()) {
      File[] segmentFiles = consumerDir.listFiles((dir, name) -> !name.equals(STATS_FILE_NAME));
      Preconditions.checkState(segmentFiles != null, "Failed to list segment files from consumer dir: {} for table: {}",
          consumerDirPath, _tableNameWithType);
      for (File file : segmentFiles) {
        if (file.delete()) {
          _logger.info("Deleted old file {}", file.getAbsolutePath());
        } else {
          _logger.error("Cannot delete file {}", file.getAbsolutePath());
        }
      }
    }

    // Set up dedup/upsert metadata manager
    // NOTE: Dedup/upsert has to be set up when starting the server. Changing the table config without restarting the
    //       server won't enable/disable them on the fly.
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, _tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", _tableNameWithType);

    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    boolean dedupEnabled = dedupConfig != null && dedupConfig.isDedupEnabled();
    if (dedupEnabled) {
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _tableNameWithType);

      List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
      Preconditions.checkState(!CollectionUtils.isEmpty(primaryKeyColumns),
          "Primary key columns must be configured for dedup");
      _tableDedupMetadataManager = new TableDedupMetadataManager(_tableNameWithType, primaryKeyColumns, _serverMetrics,
          dedupConfig.getHashFunction());
    }

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    if (upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE) {
      Preconditions.checkState(!dedupEnabled, "Dedup and upsert cannot be both enabled for table: %s",
          _tableUpsertMetadataManager);
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _tableNameWithType);
      _tableUpsertMetadataManager = TableUpsertMetadataManagerFactory.create(tableConfig, schema, this, _serverMetrics);
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    _segmentAsyncExecutorService.shutdown();
    if (_tableUpsertMetadataManager != null) {
      try {
        _tableUpsertMetadataManager.close();
      } catch (IOException e) {
        _logger.warn("Cannot close upsert metadata manager properly for table: {}", _tableNameWithType, e);
      }
    }
    for (SegmentDataManager segmentDataManager : _segmentDataManagerMap.values()) {
      segmentDataManager.destroy();
    }
    if (_leaseExtender != null) {
      _leaseExtender.shutDown();
    }
  }

  public RealtimeSegmentStatsHistory getStatsHistory() {
    return _statsHistory;
  }

  public Semaphore getSegmentBuildSemaphore() {
    return _segmentBuildSemaphore;
  }

  public String getConsumerDir() {
    String consumerDirPath = _tableDataManagerConfig.getConsumerDir();
    File consumerDir;
    // If a consumer directory has been configured, use it to create a per-table path under the consumer dir.
    // Otherwise, create a sub-dir under the table-specific data director and use it for consumer mmaps
    if (consumerDirPath != null) {
      consumerDir = new File(consumerDirPath, _tableNameWithType);
    } else {
      consumerDirPath = _tableDataDir + File.separator + CONSUMERS_DIR;
      consumerDir = new File(consumerDirPath);
    }

    if (!consumerDir.exists()) {
      if (!consumerDir.mkdirs()) {
        _logger.error("Failed to create consumer directory {}", consumerDir.getAbsolutePath());
      }
    }

    return consumerDir.getAbsolutePath();
  }

  public boolean isDedupEnabled() {
    return _tableDedupMetadataManager != null;
  }

  public boolean isUpsertEnabled() {
    return _tableUpsertMetadataManager != null;
  }

  public boolean isPartialUpsertEnabled() {
    return _tableUpsertMetadataManager != null
        && _tableUpsertMetadataManager.getUpsertMode() == UpsertConfig.Mode.PARTIAL;
  }

  /*
   * This call comes in one of two ways:
   * For HL Segments:
   * - We are being directed by helix to own up all the segments that we committed and are still in retention. In
   * this case we treat it exactly like how OfflineTableDataManager would -- wrap it into an
   * OfflineSegmentDataManager, and put it in the map.
   * - We are being asked to own up a new realtime segment. In this case, we wrap the segment with a
   * RealTimeSegmentDataManager (that kicks off consumption). When the segment is committed we get notified via the
   * notifySegmentCommitted call, at which time we replace the segment with the OfflineSegmentDataManager
   *
   * For LL Segments:
   * - We are being asked to start consuming from a partition.
   * - We did not know about the segment and are being asked to download and own the segment (re-balancing, or
   *   replacing a realtime server with a fresh one, maybe). We need to look at segment metadata and decide whether
   *   to start consuming or download the segment.
   */
  @Override
  public void addSegment(String segmentName, TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager != null) {
      _logger.warn("Skipping adding existing segment: {} for table: {} with data manager class: {}", segmentName,
          _tableNameWithType, segmentDataManager.getClass().getSimpleName());
      return;
    }

    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, _tableNameWithType, segmentName);
    Preconditions.checkNotNull(segmentZKMetadata);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkNotNull(schema);

    File segmentDir = new File(_indexDir, segmentName);
    // Restart during segment reload might leave segment in inconsistent state (index directory might not exist but
    // segment backup directory existed), need to first try to recover from reload failure before checking the existence
    // of the index directory and loading segment from it
    LoaderUtils.reloadFailureRecovery(segmentDir);

    boolean isHLCSegment = SegmentName.isHighLevelConsumerSegmentName(segmentName);
    if (segmentZKMetadata.getStatus().isCompleted()) {
      if (segmentDir.exists()) {
        // Local segment exists, try to load it
        try {
          addSegment(ImmutableSegmentLoader.load(segmentDir, indexLoadingConfig, schema));
          return;
        } catch (Exception e) {
          if (!isHLCSegment) {
            // For LLC and uploaded segments, delete the local copy and download a new copy
            _logger.error("Caught exception while loading segment: {}, downloading a new copy", segmentName, e);
            FileUtils.deleteQuietly(segmentDir);
          } else {
            // For HLC segments, throw out the exception because there is no way to recover (controller does not have a
            // copy of the segment)
            throw new RuntimeException("Failed to load local HLC segment: " + segmentName, e);
          }
        }
      } else {
        if (isHLCSegment) {
          throw new RuntimeException("Failed to find local copy for committed HLC segment: " + segmentName);
        }
      }
      // Local segment doesn't exist or cannot load, download a new copy
      downloadAndReplaceSegment(segmentName, segmentZKMetadata, indexLoadingConfig, tableConfig);
      return;
    } else {
      // Metadata has not been committed, delete the local segment if exists
      FileUtils.deleteQuietly(segmentDir);
    }

    // Start a new consuming segment
    if (!isValid(schema, tableConfig.getIndexingConfig())) {
      _logger.error("Not adding segment {}", segmentName);
      throw new RuntimeException("Mismatching schema/table config for " + _tableNameWithType);
    }
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);

    if (!isHLCSegment) {
      // Generates only one semaphore for every partitionGroupId
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionGroupId = llcSegmentName.getPartitionGroupId();
      Semaphore semaphore = _partitionGroupIdToSemaphoreMap.computeIfAbsent(partitionGroupId, k -> new Semaphore(1));
      PartitionUpsertMetadataManager partitionUpsertMetadataManager =
          _tableUpsertMetadataManager != null ? _tableUpsertMetadataManager.getOrCreatePartitionManager(
              partitionGroupId) : null;
      PartitionDedupMetadataManager partitionDedupMetadataManager =
          _tableDedupMetadataManager != null ? _tableDedupMetadataManager.getOrCreatePartitionManager(partitionGroupId)
              : null;
      // For dedup and partial-upsert, wait for all segments loaded before creating the consuming segment
      if (isDedupEnabled() || isPartialUpsertEnabled()) {
        if (!_allSegmentsLoaded.get()) {
          synchronized (_allSegmentsLoaded) {
            if (!_allSegmentsLoaded.get()) {
              TableStateUtils.waitForAllSegmentsLoaded(_helixManager, _tableNameWithType);
              _allSegmentsLoaded.set(true);
            }
          }
        }
      }

      schema = SegmentGeneratorConfig.updateSchemaWithTimestampIndexes(schema,
          SegmentGeneratorConfig.extractTimestampIndexConfigsFromTableConfig(tableConfig));

      segmentDataManager =
          new LLRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, this, _indexDir.getAbsolutePath(),
              indexLoadingConfig, schema, llcSegmentName, semaphore, _serverMetrics, partitionUpsertMetadataManager,
              partitionDedupMetadataManager);
    } else {
      InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(_propertyStore, _instanceId);
      segmentDataManager = new HLRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, instanceZKMetadata, this,
          _indexDir.getAbsolutePath(), indexLoadingConfig, schema, _serverMetrics);
    }

    _logger.info("Initialized RealtimeSegmentDataManager - " + segmentName);
    _segmentDataManagerMap.put(segmentName, segmentDataManager);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    if (isUpsertEnabled()) {
      handleUpsert(immutableSegment);
      return;
    }

    // TODO: Change dedup handling to handle segment replacement
    if (isDedupEnabled()) {
      buildDedupMeta((ImmutableSegmentImpl) immutableSegment);
    }
    super.addSegment(immutableSegment);
  }

  private void buildDedupMeta(ImmutableSegmentImpl immutableSegment) {
    // TODO(saurabh) refactor commons code with handleUpsert
    String segmentName = immutableSegment.getSegmentName();
    Integer partitionGroupId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, null);
    Preconditions.checkNotNull(partitionGroupId,
        String.format("PartitionGroupId is not available for segment: '%s' (dedup-enabled table: %s)", segmentName,
            _tableNameWithType));
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        _tableDedupMetadataManager.getOrCreatePartitionManager(partitionGroupId);
    immutableSegment.enableDedup(partitionDedupMetadataManager);
    partitionDedupMetadataManager.addSegment(immutableSegment);
  }

  private void handleUpsert(ImmutableSegment immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} to upsert-enabled table: {}", segmentName, _tableNameWithType);

    Integer partitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, null);
    Preconditions.checkNotNull(partitionId,
        String.format("Failed to get partition id for segment: %s (upsert-enabled table: %s)", segmentName,
            _tableNameWithType));
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionId);

    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.put(segmentName, newSegmentManager);
    if (oldSegmentManager == null) {
      partitionUpsertMetadataManager.addSegment(immutableSegment);
      _logger.info("Added new immutable segment: {} to upsert-enabled table: {}", segmentName, _tableNameWithType);
    } else {
      IndexSegment oldSegment = oldSegmentManager.getSegment();
      partitionUpsertMetadataManager.replaceSegment(immutableSegment, oldSegment);
      _logger.info("Replaced {} segment: {} of upsert-enabled table: {}",
          oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, _tableNameWithType);
      releaseSegment(oldSegmentManager);
    }
  }

  @Override
  protected boolean allowDownload(String segmentName, SegmentZKMetadata zkMetadata) {
    // Cannot download HLC segment or consuming segment
    if (SegmentName.isHighLevelConsumerSegmentName(segmentName) || zkMetadata.getStatus() == Status.IN_PROGRESS) {
      return false;
    }
    // TODO: may support download from peer servers as well.
    return !METADATA_URI_FOR_PEER_DOWNLOAD.equals(zkMetadata.getDownloadUrl());
  }

  void downloadAndReplaceSegment(String segmentName, SegmentZKMetadata segmentZKMetadata,
      IndexLoadingConfig indexLoadingConfig, TableConfig tableConfig) {
    String uri = segmentZKMetadata.getDownloadUrl();
    if (!METADATA_URI_FOR_PEER_DOWNLOAD.equals(uri)) {
      try {
        downloadSegmentFromDeepStore(segmentName, indexLoadingConfig, uri);
      } catch (Exception e) {
        _logger.warn("Download segment {} from deepstore uri {} failed.", segmentName, uri, e);
        // Download from deep store failed; try to download from peer if peer download is setup for the table.
        if (isPeerSegmentDownloadEnabled(tableConfig)) {
          downloadSegmentFromPeer(segmentName, tableConfig.getValidationConfig().getPeerSegmentDownloadScheme(),
              indexLoadingConfig);
        } else {
          throw e;
        }
      }
    } else {
      if (isPeerSegmentDownloadEnabled(tableConfig)) {
        downloadSegmentFromPeer(segmentName, tableConfig.getValidationConfig().getPeerSegmentDownloadScheme(),
            indexLoadingConfig);
      } else {
        throw new RuntimeException("Peer segment download not enabled for segment " + segmentName);
      }
    }
  }

  private void downloadSegmentFromDeepStore(String segmentName, IndexLoadingConfig indexLoadingConfig, String uri) {
    File segmentTarFile = new File(_indexDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    try {
      SegmentFetcherFactory.fetchSegmentToLocal(uri, segmentTarFile);
      _logger.info("Downloaded file from {} to {}; Length of downloaded file: {}", uri, segmentTarFile,
          segmentTarFile.length());
      untarAndMoveSegment(segmentName, indexLoadingConfig, segmentTarFile);
    } catch (Exception e) {
      _logger.warn("Failed to download segment {} from deep store: ", segmentName, e);
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(segmentTarFile);
    }
  }

  /**
   * Untars the new segment and replaces the existing segment.
   */
  private void untarAndMoveSegment(String segmentName, IndexLoadingConfig indexLoadingConfig, File segmentTarFile)
      throws IOException {
    // This could leave temporary directories in _indexDir if JVM shuts down before the temp directory is deleted.
    // This is fine since the temporary directories are deleted when the table data manager calls init.
    File tempSegmentDir = getTmpSegmentDataDir("tmp-" + segmentName + "." + System.currentTimeMillis());
    try {
      File tempIndexDir = TarGzCompressionUtils.untar(segmentTarFile, tempSegmentDir).get(0);
      _logger.info("Uncompressed file {} into tmp dir {}", segmentTarFile, tempSegmentDir);
      File indexDir = new File(_indexDir, segmentName);
      FileUtils.deleteQuietly(indexDir);
      FileUtils.moveDirectory(tempIndexDir, indexDir);
      _logger.info("Replacing LLC Segment {}", segmentName);
      replaceLLSegment(segmentName, indexLoadingConfig);
    } finally {
      FileUtils.deleteQuietly(tempSegmentDir);
    }
  }

  private boolean isPeerSegmentDownloadEnabled(TableConfig tableConfig) {
    return
        CommonConstants.HTTP_PROTOCOL.equalsIgnoreCase(tableConfig.getValidationConfig().getPeerSegmentDownloadScheme())
            || CommonConstants.HTTPS_PROTOCOL.equalsIgnoreCase(
            tableConfig.getValidationConfig().getPeerSegmentDownloadScheme());
  }

  private void downloadSegmentFromPeer(String segmentName, String downloadScheme,
      IndexLoadingConfig indexLoadingConfig) {
    File segmentTarFile = new File(_indexDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    try {
      // First find servers hosting the segment in a ONLINE state.
      List<URI> peerSegmentURIs = PeerServerSegmentFinder.getPeerServerURIs(segmentName, downloadScheme, _helixManager);
      // Next download the segment from a randomly chosen server using configured scheme.
      SegmentFetcherFactory.getSegmentFetcher(downloadScheme).fetchSegmentToLocal(peerSegmentURIs, segmentTarFile);
      _logger.info("Fetched segment {} from: {} to: {} of size: {}", segmentName, peerSegmentURIs, segmentTarFile,
          segmentTarFile.length());
      untarAndMoveSegment(segmentName, indexLoadingConfig, segmentTarFile);
    } catch (Exception e) {
      _logger.warn("Download and move segment {} from peer with scheme {} failed.", segmentName, downloadScheme, e);
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(segmentTarFile);
    }
  }

  /**
   * Replaces a committed HLC REALTIME segment.
   */
  public void replaceHLSegment(SegmentZKMetadata segmentZKMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, _tableNameWithType, segmentZKMetadata);
    File indexDir = new File(_indexDir, segmentZKMetadata.getSegmentName());
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, schema));
  }

  /**
   * Replaces a committed LLC REALTIME segment.
   */
  public void replaceLLSegment(String segmentName, IndexLoadingConfig indexLoadingConfig) {
    try {
      File indexDir = new File(_indexDir, segmentName);
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, schema));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getServerInstance() {
    return _instanceId;
  }

  /**
   * Validate a schema against the table config for real-time record consumption.
   * Ideally, we should validate these things when schema is added or table is created, but either of these
   * may be changed while the table is already provisioned. For the change to take effect, we need to restart the
   * servers, so  validation at this place is fine.
   *
   * As of now, the following validations are done:
   * 1. Make sure that the sorted column, if specified, is not multi-valued.
   * 2. Validate the schema itself
   *
   * We allow the user to specify multiple sorted columns, but only consider the first one for now.
   * (secondary sort is not yet implemented).
   *
   * If we add more validations, it may make sense to split this method into multiple validation methods.
   * But then, we are trying to figure out all the invalid cases before we return from this method...
   *
   * @return true if schema is valid.
   */
  private boolean isValid(Schema schema, IndexingConfig indexingConfig) {
    // 1. Make sure that the sorted column is not a multi-value field.
    List<String> sortedColumns = indexingConfig.getSortedColumn();
    boolean isValid = true;
    if (CollectionUtils.isNotEmpty(sortedColumns)) {
      final String sortedColumn = sortedColumns.get(0);
      if (sortedColumns.size() > 1) {
        _logger.warn("More than one sorted column configured. Using {}", sortedColumn);
      }
      FieldSpec fieldSpec = schema.getFieldSpecFor(sortedColumn);
      if (!fieldSpec.isSingleValueField()) {
        _logger.error("Cannot configure multi-valued column {} as sorted column", sortedColumn);
        isValid = false;
      }
    }
    // 2. We want to get the schema errors, if any, even if isValid is false;
    try {
      SchemaUtils.validate(schema);
    } catch (Exception e) {
      _logger.error("Caught exception while validating schema: {}", schema.getSchemaName(), e);
      isValid = false;
    }
    return isValid;
  }
}
