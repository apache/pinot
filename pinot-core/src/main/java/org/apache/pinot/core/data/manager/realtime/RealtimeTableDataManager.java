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
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.loader.V3RemoveIndexException;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.PartialUpsertHandler;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
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
  // In some streams, it's possible that having multiple consumers (with the same consumer name on the same host) consuming from the same stream partition can lead to bugs.
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

  private UpsertConfig.Mode _upsertMode;
  private TableUpsertMetadataManager _tableUpsertMetadataManager;
  private List<String> _primaryKeyColumns;
  private String _upsertComparisonColumn;

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
      _logger
          .error("Error reading history object for table {} from {}", _tableNameWithType, statsFile.getAbsolutePath(),
              e);
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

    // NOTE: Upsert has to be set up when starting the server. Changing the table config without restarting the server
    //       won't enable/disable the upsert on the fly.
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, _tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", _tableNameWithType);
    _upsertMode = tableConfig.getUpsertMode();
    if (isUpsertEnabled()) {
      UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
      assert upsertConfig != null;
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _tableNameWithType);

      PartialUpsertHandler partialUpsertHandler = null;
      if (isPartialUpsertEnabled()) {
        partialUpsertHandler =
            new PartialUpsertHandler(_helixManager, _tableNameWithType, upsertConfig.getPartialUpsertStrategies());
      }
      _tableUpsertMetadataManager =
          new TableUpsertMetadataManager(_tableNameWithType, _serverMetrics, partialUpsertHandler);
      _primaryKeyColumns = schema.getPrimaryKeyColumns();
      Preconditions.checkState(!CollectionUtils.isEmpty(_primaryKeyColumns),
          "Primary key columns must be configured for upsert");
      String comparisonColumn = upsertConfig.getComparisonColumn();
      _upsertComparisonColumn =
          comparisonColumn != null ? comparisonColumn : tableConfig.getValidationConfig().getTimeColumnName();
    }

    if (consumerDir.exists()) {
      File[] segmentFiles = consumerDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return !name.equals(STATS_FILE_NAME);
        }
      });
      for (File file : segmentFiles) {
        if (file.delete()) {
          _logger.info("Deleted old file {}", file.getAbsolutePath());
        } else {
          _logger.error("Cannot delete file {}", file.getAbsolutePath());
        }
      }
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    _segmentAsyncExecutorService.shutdown();
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

  public boolean isUpsertEnabled() {
    return _upsertMode != UpsertConfig.Mode.NONE;
  }

  public boolean isPartialUpsertEnabled() {
    return _upsertMode == UpsertConfig.Mode.PARTIAL;
  }

  /*
   * This call comes in one of two ways:
   * For HL Segments:
   * - We are being directed by helix to own up all the segments that we committed and are still in retention. In this case
   *   we treat it exactly like how OfflineTableDataManager would -- wrap it into an OfflineSegmentDataManager, and put it
   *   in the map.
   * - We are being asked to own up a new realtime segment. In this case, we wrap the segment with a RealTimeSegmentDataManager
   *   (that kicks off consumption). When the segment is committed we get notified via the notifySegmentCommitted call, at
   *   which time we replace the segment with the OfflineSegmentDataManager
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

    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
        ZKMetadataProvider.getRealtimeSegmentZKMetadata(_propertyStore, _tableNameWithType, segmentName);
    Preconditions.checkNotNull(realtimeSegmentZKMetadata);
    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
    Preconditions.checkNotNull(schema);

    File segmentDir = new File(_indexDir, segmentName);
    // Restart during segment reload might leave segment in inconsistent state (index directory might not exist but
    // segment backup directory existed), need to first try to recover from reload failure before checking the existence
    // of the index directory and loading segment from it
    LoaderUtils.reloadFailureRecovery(segmentDir);

    boolean isLLCSegment = SegmentName.isLowLevelConsumerSegmentName(segmentName);
    if (segmentDir.exists()) {
      // Segment already exists on disk
      if (realtimeSegmentZKMetadata.getStatus() == Status.DONE) {
        // Metadata has been committed, load the local segment
        try {
          addSegment(ImmutableSegmentLoader.load(segmentDir, indexLoadingConfig, schema));
          return;
        } catch (Exception e) {
          if (isLLCSegment) {
            // For LLC and segments, delete the local copy and download a new copy from the controller
            FileUtils.deleteQuietly(segmentDir);
            if (e instanceof V3RemoveIndexException) {
              _logger.info("Unable to remove index from V3 format segment: {}, downloading a new copy", segmentName, e);
            } else {
              _logger.error("Caught exception while loading segment: {}, downloading a new copy", segmentName, e);
            }
          } else {
            // For HLC segments, throw out the exception because there is no way to recover (controller does not have a
            // copy of the segment)
            throw e;
          }
        }
      } else {
        // Metadata has not been committed, delete the local segment
        FileUtils.deleteQuietly(segmentDir);
      }
    } else if (realtimeSegmentZKMetadata.getStatus() == Status.UPLOADED) {
      // The segment is uploaded to an upsert enabled realtime table. Download the segment and load.
      Preconditions.checkArgument(realtimeSegmentZKMetadata instanceof LLCRealtimeSegmentZKMetadata,
          "Upload segment is not LLC segment");
      String downloadUrl = ((LLCRealtimeSegmentZKMetadata) realtimeSegmentZKMetadata).getDownloadUrl();
      Preconditions.checkNotNull(downloadUrl, "Upload segment metadata has no download url");
      downloadSegmentFromDeepStore(segmentName, indexLoadingConfig, downloadUrl);
      _logger
          .info("Downloaded, untarred and add segment {} of table {} from {}", segmentName, tableConfig.getTableName(),
              downloadUrl);
      return;
    }

    // Start a new consuming segment or download the segment from the controller

    if (!isValid(schema, tableConfig.getIndexingConfig())) {
      _logger.error("Not adding segment {}", segmentName);
      throw new RuntimeException("Mismatching schema/table config for " + _tableNameWithType);
    }
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);

    if (isLLCSegment) {
      if (realtimeSegmentZKMetadata.getStatus() == Status.DONE) {
        downloadAndReplaceSegment(segmentName, (LLCRealtimeSegmentZKMetadata) realtimeSegmentZKMetadata,
            indexLoadingConfig, tableConfig);
        return;
      }

      // Generates only one semaphore for every partitionGroupId
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionGroupId = llcSegmentName.getPartitionGroupId();
      Semaphore semaphore = _partitionGroupIdToSemaphoreMap.computeIfAbsent(partitionGroupId, k -> new Semaphore(1));
      PartitionUpsertMetadataManager partitionUpsertMetadataManager =
          _tableUpsertMetadataManager != null ? _tableUpsertMetadataManager
              .getOrCreatePartitionManager(partitionGroupId) : null;
      segmentDataManager =
          new LLRealtimeSegmentDataManager(realtimeSegmentZKMetadata, tableConfig, this, _indexDir.getAbsolutePath(),
              indexLoadingConfig, schema, llcSegmentName, semaphore, _serverMetrics, partitionUpsertMetadataManager);
    } else {
      InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(_propertyStore, _instanceId);
      segmentDataManager =
          new HLRealtimeSegmentDataManager(realtimeSegmentZKMetadata, tableConfig, instanceZKMetadata, this,
              _indexDir.getAbsolutePath(), indexLoadingConfig, schema, _serverMetrics);
    }

    _logger.info("Initialized RealtimeSegmentDataManager - " + segmentName);
    _segmentDataManagerMap.put(segmentName, segmentDataManager);
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    if (isUpsertEnabled()) {
      handleUpsert((ImmutableSegmentImpl) immutableSegment);
    }
    super.addSegment(immutableSegment);
  }

  private void handleUpsert(ImmutableSegmentImpl immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    int partitionGroupId = SegmentUtils
        .getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, _primaryKeyColumns.get(0));
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionGroupId);
    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    immutableSegment.enableUpsert(partitionUpsertMetadataManager, validDocIds);

    Map<String, PinotSegmentColumnReader> columnToReaderMap = new HashMap<>();
    for (String primaryKeyColumn : _primaryKeyColumns) {
      columnToReaderMap.put(primaryKeyColumn, new PinotSegmentColumnReader(immutableSegment, primaryKeyColumn));
    }
    columnToReaderMap
        .put(_upsertComparisonColumn, new PinotSegmentColumnReader(immutableSegment, _upsertComparisonColumn));
    int numTotalDocs = immutableSegment.getSegmentMetadata().getTotalDocs();
    int numPrimaryKeyColumns = _primaryKeyColumns.size();
    Iterator<PartitionUpsertMetadataManager.RecordInfo> recordInfoIterator =
        new Iterator<PartitionUpsertMetadataManager.RecordInfo>() {
          private int _docId = 0;

          @Override
          public boolean hasNext() {
            return _docId < numTotalDocs;
          }

          @Override
          public PartitionUpsertMetadataManager.RecordInfo next() {
            Object[] values = new Object[numPrimaryKeyColumns];
            for (int i = 0; i < numPrimaryKeyColumns; i++) {
              Object value = columnToReaderMap.get(_primaryKeyColumns.get(i)).getValue(_docId);
              if (value instanceof byte[]) {
                value = new ByteArray((byte[]) value);
              }
              values[i] = value;
            }
            PrimaryKey primaryKey = new PrimaryKey(values);
            Object timeValue = columnToReaderMap.get(_upsertComparisonColumn).getValue(_docId);
            Preconditions.checkArgument(timeValue instanceof Comparable, "time column shall be comparable");
            long timestamp = IngestionUtils.extractTimeValue((Comparable) timeValue);
            return new PartitionUpsertMetadataManager.RecordInfo(primaryKey, _docId++, timestamp);
          }
        };
    partitionUpsertMetadataManager.addSegment(immutableSegment, recordInfoIterator);
  }

  public void downloadAndReplaceSegment(String segmentName, LLCRealtimeSegmentZKMetadata llcSegmentMetadata,
      IndexLoadingConfig indexLoadingConfig, TableConfig tableConfig) {
    final String uri = llcSegmentMetadata.getDownloadUrl();
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
    // TODO: This could leave temporary directories in _indexDir if JVM shuts down before the temporary directory is
    //       deleted. Consider cleaning up all temporary directories when starting the server.
    File tempSegmentDir = new File(_indexDir, "tmp-" + segmentName + "." + System.currentTimeMillis());
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
            || CommonConstants.HTTPS_PROTOCOL
            .equalsIgnoreCase(tableConfig.getValidationConfig().getPeerSegmentDownloadScheme());
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
  public void replaceHLSegment(RealtimeSegmentZKMetadata segmentZKMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    ZKMetadataProvider.setRealtimeSegmentZKMetadata(_propertyStore, _tableNameWithType, segmentZKMetadata);
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
