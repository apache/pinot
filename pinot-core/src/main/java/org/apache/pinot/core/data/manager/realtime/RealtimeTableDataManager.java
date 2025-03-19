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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.core.data.manager.DuoSegmentDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManager;
import org.apache.pinot.segment.local.dedup.TableDedupMetadataManagerFactory;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.tablestate.TableStateUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;


@ThreadSafe
public class RealtimeTableDataManager extends BaseTableDataManager {
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
  private final Map<Integer, SemaphoreAccessCoordinator> _partitionGroupIdToSemaphoreCoordinatorMap =
      new ConcurrentHashMap<>();
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

  public static final long READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);

  public static final long DEFAULT_SEGMENT_DOWNLOAD_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10); // 10 minutes
  public static final long SLEEP_INTERVAL_MS = 30000; // 30 seconds sleep interval
  private static final String SEGMENT_DOWNLOAD_TIMEOUT_MINUTES = "segmentDownloadTimeoutMinutes";

  // TODO: Change it to BooleanSupplier
  private final Supplier<Boolean> _isServerReadyToServeQueries;

  // Object to track ingestion delay for all partitions
  private IngestionDelayTracker _ingestionDelayTracker;

  private TableDedupMetadataManager _tableDedupMetadataManager;
  private TableUpsertMetadataManager _tableUpsertMetadataManager;
  private BooleanSupplier _isTableReadyToConsumeData;
  private boolean _enforceConsumptionInOrder = false;

  public RealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
    this(segmentBuildSemaphore, () -> true);
  }

  public RealtimeTableDataManager(Semaphore segmentBuildSemaphore, Supplier<Boolean> isServerReadyToServeQueries) {
    _segmentBuildSemaphore = segmentBuildSemaphore;
    _isServerReadyToServeQueries = isServerReadyToServeQueries;
  }

  @Override
  protected void doInit() {
    _leaseExtender = SegmentBuildTimeLeaseExtender.getOrCreate(_instanceId, _serverMetrics, _tableNameWithType);
    // Tracks ingestion delay of all partitions being served for this table
    _ingestionDelayTracker =
        new IngestionDelayTracker(_serverMetrics, _tableNameWithType, this, _isServerReadyToServeQueries);
    File statsFile = new File(_tableDataDir, STATS_FILE_NAME);
    try {
      _statsHistory = RealtimeSegmentStatsHistory.deserialzeFrom(statsFile);
    } catch (IOException | ClassNotFoundException e) {
      _logger.error("Caught exception while reading stats history from: {}", statsFile.getAbsolutePath(), e);
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
      Preconditions.checkState(segmentFiles != null, "Failed to list segment files from consumer dir: %s for table: %s",
          consumerDirPath, _tableNameWithType);
      for (File file : segmentFiles) {
        if (FileUtils.deleteQuietly(file)) {
          _logger.info("Deleted old file {}", file.getAbsolutePath());
        } else {
          _logger.error("Cannot delete file {}", file.getAbsolutePath());
        }
      }
    }

    // Set up dedup/upsert metadata manager
    // NOTE: Dedup/upsert has to be set up when starting the server. Changing the table config without restarting the
    //       server won't enable/disable them on the fly.
    DedupConfig dedupConfig = _tableConfig.getDedupConfig();
    boolean dedupEnabled = dedupConfig != null && dedupConfig.isDedupEnabled();
    if (dedupEnabled) {
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _tableNameWithType);

      List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
      Preconditions.checkState(!CollectionUtils.isEmpty(primaryKeyColumns),
          "Primary key columns must be configured for dedup");
      _tableDedupMetadataManager = TableDedupMetadataManagerFactory.create(_tableConfig, schema, this, _serverMetrics,
          _instanceDataManagerConfig.getDedupConfig());
    }

    UpsertConfig upsertConfig = _tableConfig.getUpsertConfig();
    if (upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE) {
      Preconditions.checkState(!dedupEnabled, "Dedup and upsert cannot be both enabled for table: %s",
          _tableUpsertMetadataManager);
      Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _tableNameWithType);
      Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _tableNameWithType);
      _tableUpsertMetadataManager =
          TableUpsertMetadataManagerFactory.create(_tableConfig, _instanceDataManagerConfig.getUpsertConfig());
      _tableUpsertMetadataManager.init(_tableConfig, schema, this);
    }

    _enforceConsumptionInOrder = enforceConsumptionInOrder();

    // For dedup and partial-upsert, need to wait for all segments loaded before starting consuming data
    if (isDedupEnabled() || isPartialUpsertEnabled()) {
      _isTableReadyToConsumeData = new BooleanSupplier() {
        volatile boolean _allSegmentsLoaded;
        long _lastCheckTimeMs;

        @Override
        public boolean getAsBoolean() {
          if (_allSegmentsLoaded) {
            return true;
          } else {
            synchronized (this) {
              if (_allSegmentsLoaded) {
                return true;
              }
              long currentTimeMs = System.currentTimeMillis();
              if (currentTimeMs - _lastCheckTimeMs <= READY_TO_CONSUME_DATA_CHECK_INTERVAL_MS) {
                return false;
              }
              _lastCheckTimeMs = currentTimeMs;
              _allSegmentsLoaded = TableStateUtils.isAllSegmentsLoaded(_helixManager, _tableNameWithType);
              return _allSegmentsLoaded;
            }
          }
        }
      };
    } else {
      _isTableReadyToConsumeData = () -> true;
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    // Make sure we do metric cleanup when we shut down the table.
    // Do this first, so we do not show ingestion lag during shutdown.
    _ingestionDelayTracker.shutdown();
    if (_tableUpsertMetadataManager != null || _tableDedupMetadataManager != null) {
      // Stop the upsert metadata manager first to prevent removing metadata when destroying segments
      if (_tableUpsertMetadataManager != null) {
        _tableUpsertMetadataManager.stop();
      }
      if (_tableDedupMetadataManager != null) {
        _tableDedupMetadataManager.stop();
      }
      releaseAndRemoveAllSegments();
      try {
        if (_tableUpsertMetadataManager != null) {
          _tableUpsertMetadataManager.close();
        }
        if (_tableDedupMetadataManager != null) {
          _tableDedupMetadataManager.close();
        }
      } catch (IOException e) {
        _logger.warn("Caught exception while closing upsert metadata manager", e);
      }
    } else {
      releaseAndRemoveAllSegments();
    }
    if (_leaseExtender != null) {
      _leaseExtender.shutDown();
    }
  }

  /**
   * Updates the ingestion metrics for the given partition.
   *
   * @param segmentName name of the consuming segment
   * @param partitionId partition id of the consuming segment (directly passed in to avoid parsing the segment name)
   * @param ingestionTimeMs ingestion time of the last consumed message (from {@link RowMetadata})
   * @param firstStreamIngestionTimeMs ingestion time of the last consumed message in the first stream (from
   *                                   {@link RowMetadata})
   * @param currentOffset offset of the last consumed message (from {@link RowMetadata})
   * @param latestOffset offset of the latest message in the partition (from {@link StreamMetadataProvider})
   */
  public void updateIngestionMetrics(String segmentName, int partitionId, long ingestionTimeMs,
      long firstStreamIngestionTimeMs, @Nullable StreamPartitionMsgOffset currentOffset,
      @Nullable StreamPartitionMsgOffset latestOffset) {
    _ingestionDelayTracker.updateIngestionMetrics(segmentName, partitionId, ingestionTimeMs, firstStreamIngestionTimeMs,
        currentOffset, latestOffset);
  }

  /**
   * Returns the ingestion time of the last consumed message for the partition of the given segment. Returns
   * {@code Long.MIN_VALUE} when it is not available.
   */
  public long getPartitionIngestionTimeMs(String segmentName) {
    return _ingestionDelayTracker.getPartitionIngestionTimeMs(new LLCSegmentName(segmentName).getPartitionGroupId());
  }

  /**
   * Removes the ingestion metrics for the partition of the given segment, and also ignores the updates from the given
   * segment. This is useful when we want to stop tracking the ingestion delay for a partition when the segment might
   * still be consuming, e.g. when the new consuming segment is created on a different server.
   */
  public void removeIngestionMetrics(String segmentName) {
    _ingestionDelayTracker.stopTrackingPartitionIngestionDelay(segmentName);
  }

  /**
   * Method to handle CONSUMING -> DROPPED segment state transitions:
   * We stop tracking partitions whose segments are dropped.
   *
   * @param segmentName name of segment which is transitioning state.
   */
  @Override
  public void onConsumingToDropped(String segmentName) {
    // NOTE: No need to mark segment ignored here because it should have already been dropped.
    _ingestionDelayTracker.stopTrackingPartitionIngestionDelay(new LLCSegmentName(segmentName).getPartitionGroupId());
  }

  /**
   * Method to handle CONSUMING -> ONLINE segment state transitions:
   * We mark partitions for verification against ideal state when we do not see a consuming segment for some time
   * for that partition. The idea is to remove the related metrics when the partition moves from the current server.
   *
   * @param segmentName name of segment which is transitioning state.
   */
  @Override
  public void onConsumingToOnline(String segmentName) {
    _ingestionDelayTracker.markPartitionForVerification(segmentName);
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    List<SegmentContext> segmentContexts = new ArrayList<>(selectedSegments.size());
    selectedSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    if (isUpsertEnabled() && !QueryOptionsUtils.isSkipUpsert(queryOptions)) {
      _tableUpsertMetadataManager.setSegmentContexts(segmentContexts, queryOptions);
    }
    return segmentContexts;
  }

  /**
   * Returns all partitionGroupIds for the partitions hosted by this server for current table.
   * @apiNote this involves Zookeeper read and should not be used frequently due to efficiency concerns.
   */
  public Set<Integer> getHostedPartitionsGroupIds() {
    Set<Integer> partitionsHostedByThisServer = new HashSet<>();
    List<String> segments = TableStateUtils.getSegmentsInGivenStateForThisInstance(_helixManager, _tableNameWithType,
        CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING);
    for (String segmentNameStr : segments) {
      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
      partitionsHostedByThisServer.add(segmentName.getPartitionGroupId());
    }
    return partitionsHostedByThisServer;
  }

  public RealtimeSegmentStatsHistory getStatsHistory() {
    return _statsHistory;
  }

  public Semaphore getSegmentBuildSemaphore() {
    return _segmentBuildSemaphore;
  }

  public String getConsumerDir() {
    File consumerDir = getConsumerDirPath();
    if (!consumerDir.exists()) {
      if (!consumerDir.mkdirs()) {
        _logger.error("Failed to create consumer directory {}", consumerDir.getAbsolutePath());
      }
    }

    return consumerDir.getAbsolutePath();
  }

  public File getConsumerDirPath() {
    String consumerDirPath = _instanceDataManagerConfig.getConsumerDir();
    File consumerDir;
    // If a consumer directory has been configured, use it to create a per-table path under the consumer dir.
    // Otherwise, create a sub-dir under the table-specific data director and use it for consumer mmaps
    if (consumerDirPath != null) {
      consumerDir = new File(consumerDirPath, _tableNameWithType);
    } else {
      consumerDirPath = _tableDataDir + File.separator + CONSUMERS_DIR;
      consumerDir = new File(consumerDirPath);
    }
    return consumerDir;
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

  private void handleSegmentPreload(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    // Today a table can use either upsert or dedup but not both at the same time, so preloading is done by either the
    // upsert manager or the dedup manager.
    // TODO: if a table can enable both dedup and upsert in the future, we need to revisit the preloading logic here,
    //       as we can only preload segments once but have to restore metadata for both dedup and upsert managers.
    handleUpsertPreload(zkMetadata, indexLoadingConfig);
    handleDedupPreload(zkMetadata, indexLoadingConfig);
  }

  /**
   * Handles upsert preload if the upsert preload is enabled.
   */
  private void handleUpsertPreload(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    if (_tableUpsertMetadataManager == null || !_tableUpsertMetadataManager.isEnablePreload()) {
      return;
    }
    String segmentName = zkMetadata.getSegmentName();
    Integer partitionId = SegmentUtils.getRealtimeSegmentPartitionId(segmentName, zkMetadata, null);
    Preconditions.checkState(partitionId != null, "Failed to get partition id for segment: " + segmentName
        + " in upsert-enabled table: " + _tableNameWithType);
    _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionId).preloadSegments(indexLoadingConfig);
  }

  /**
   * Handles dedup preload if the dedup preload is enabled.
   */
  private void handleDedupPreload(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig) {
    if (_tableDedupMetadataManager == null || !_tableDedupMetadataManager.isEnablePreload()) {
      return;
    }
    String segmentName = zkMetadata.getSegmentName();
    Integer partitionId = SegmentUtils.getRealtimeSegmentPartitionId(segmentName, zkMetadata, null);
    Preconditions.checkState(partitionId != null, "Failed to get partition id for segment: " + segmentName
        + " in dedup-enabled table: " + _tableNameWithType);
    _tableDedupMetadataManager.getOrCreatePartitionManager(partitionId).preloadSegments(indexLoadingConfig);
  }

  protected void doAddOnlineSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    Preconditions.checkState(zkMetadata.getStatus() != Status.IN_PROGRESS,
        "Segment: %s of table: %s is not committed, cannot make it ONLINE", segmentName, _tableNameWithType);
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
    handleSegmentPreload(zkMetadata, indexLoadingConfig);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager == null) {
      addNewOnlineSegment(zkMetadata, indexLoadingConfig);
      return;
    }
    if (segmentDataManager instanceof RealtimeSegmentDataManager) {
      _logger.info("Changing segment: {} from CONSUMING to ONLINE", segmentName);
      ((RealtimeSegmentDataManager) segmentDataManager).goOnlineFromConsuming(zkMetadata);
      onConsumingToOnline(segmentName);
      return;
    }
    // For pauseless ingestion, the segment is marked ONLINE before it's built and before the COMMIT_END_METADATA
    // call completes.
    // The server should replace the segment only after the CRC is set by COMMIT_END_METADATA and the segment is
    // marked DONE.
    // This ensures the segment's download URL is available before discarding the locally built copy, preventing
    // data loss if COMMIT_END_METADATA fails.
    if (zkMetadata.getStatus() == Status.DONE) {
      replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
    }
  }

  @Override
  public void addConsumingSegment(String segmentName)
      throws AttemptsExceededException, RetriableOperationException {
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot add CONSUMING segment: %s to table: %s", segmentName,
        _tableNameWithType);
    _logger.info("Adding CONSUMING segment: {}", segmentName);
    Lock segmentLock = getSegmentLock(segmentName);
    segmentLock.lock();
    try {
      doAddConsumingSegment(segmentName);
    } catch (Exception e) {
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while adding CONSUMING segment", e));
      throw e;
    } finally {
      segmentLock.unlock();
    }
  }

  private void doAddConsumingSegment(String segmentName)
      throws AttemptsExceededException, RetriableOperationException {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    if ((zkMetadata.getStatus() != Status.IN_PROGRESS) && (!_enforceConsumptionInOrder)) {
      // NOTE: We do not throw exception here because the segment might have just been committed before the state
      //       transition is processed. We can skip adding this segment, and the segment will enter CONSUMING state in
      //       Helix, then we can rely on the following CONSUMING -> ONLINE state transition to add it.
      _logger.warn("Segment: {} is already committed, skipping adding it as CONSUMING segment", segmentName);
      return;
    }
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    handleSegmentPreload(zkMetadata, indexLoadingConfig);
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager != null) {
      _logger.warn("Segment: {} ({}) already exists, skipping adding it as CONSUMING segment", segmentName,
          segmentDataManager instanceof RealtimeSegmentDataManager ? "CONSUMING" : "COMPLETED");
      return;
    }

    _logger.info("Adding new CONSUMING segment: {}", segmentName);

    // Delete the segment directory if it already exists
    File segmentDir = new File(_indexDir, segmentName);
    FileUtils.deleteQuietly(segmentDir);

    TableConfig tableConfig = indexLoadingConfig.getTableConfig();
    Schema schema = indexLoadingConfig.getSchema();
    assert tableConfig != null && schema != null;
    validate(tableConfig, schema);
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);
    setDefaultTimeValueIfInvalid(tableConfig, schema, zkMetadata);

    // Generates only one semaphore for every partition
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    int partitionGroupId = llcSegmentName.getPartitionGroupId();
    SemaphoreAccessCoordinator semaphoreCoordinator = getSemaphoreAccessCoordinator(partitionGroupId);

    // Create the segment data manager and register it
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        _tableUpsertMetadataManager != null ? _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionGroupId)
            : null;
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        _tableDedupMetadataManager != null ? _tableDedupMetadataManager.getOrCreatePartitionManager(partitionGroupId)
            : null;
    RealtimeSegmentDataManager realtimeSegmentDataManager =
        createRealtimeSegmentDataManager(zkMetadata, tableConfig, indexLoadingConfig, schema, llcSegmentName,
            semaphoreCoordinator, partitionUpsertMetadataManager, partitionDedupMetadataManager,
            _isTableReadyToConsumeData);
    registerSegment(segmentName, realtimeSegmentDataManager, partitionUpsertMetadataManager);
    if (partitionUpsertMetadataManager != null) {
      partitionUpsertMetadataManager.trackNewlyAddedSegment(segmentName);
    }
    realtimeSegmentDataManager.startConsumption();
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1);

    _logger.info("Added new CONSUMING segment: {}", segmentName);
  }

  @Override
  public File downloadSegment(SegmentZKMetadata zkMetadata)
      throws Exception {
    Preconditions.checkState(zkMetadata.getStatus() != Status.IN_PROGRESS,
        "Segment: %s is still IN_PROGRESS and cannot be downloaded", zkMetadata.getSegmentName());

    // Case: The commit protocol has completed, and the segment is ready to be downloaded either
    // from deep storage or from a peer (if peer-to-peer download is enabled).
    if (zkMetadata.getStatus() == Status.DONE) {
      return super.downloadSegment(zkMetadata);
    }

    // The segment status is COMMITTING, indicating that the segment commit process is incomplete.
    // Attempting a waited download within the configured time limit.
    long downloadTimeoutMilliseconds =
        getDownloadTimeOutMilliseconds(ZKMetadataProvider.getTableConfig(_propertyStore, _tableNameWithType));
    final long startTime = System.currentTimeMillis();
    List<URI> onlineServerURIs;
    while (System.currentTimeMillis() - startTime < downloadTimeoutMilliseconds) {
      // ZK Metadata may change during segment download process; fetch it on every retry.
      zkMetadata = fetchZKMetadata(zkMetadata.getSegmentName());

      if (zkMetadata.getDownloadUrl() != null) {
        // The downloadSegment() will throw an exception in case there are some genuine issues.
        // We don't want to retry in those scenarios and will throw an exception
        return downloadSegmentFromDeepStore(zkMetadata);
      }

      if (_peerDownloadScheme != null) {
        _logger.info("Peer download is enabled for the segment: {}", zkMetadata.getSegmentName());
        try {
          onlineServerURIs = new ArrayList<>();
          PeerServerSegmentFinder.getOnlineServersFromExternalView(_helixManager.getClusterManagmentTool(),
              _helixManager.getClusterName(), _tableNameWithType, zkMetadata.getSegmentName(), _peerDownloadScheme,
              onlineServerURIs);
          if (!onlineServerURIs.isEmpty()) {
            return downloadSegmentFromPeers(zkMetadata);
          }
        } catch (Exception e) {
          _logger.warn("Could not download segment: {} from peer", zkMetadata.getSegmentName(), e);
        }
      }

      long timeElapsed = System.currentTimeMillis() - startTime;
      long timeRemaining = downloadTimeoutMilliseconds - timeElapsed;

      if (timeRemaining <= 0) {
        break;
      }

      _logger.info("Sleeping for 30 seconds as the segment url is missing. Time remaining: {} minutes",
          Math.round(timeRemaining / 60000.0));

      // Sleep for the shorter of our normal interval or remaining time
      Thread.sleep(Math.min(SLEEP_INTERVAL_MS, timeRemaining));
    }

    // If we exit the loop without returning, throw an exception
    throw new TimeoutException(
        "Failed to download segment after " + TimeUnit.MILLISECONDS.toMinutes(downloadTimeoutMilliseconds)
            + " minutes of retrying. Segment: " + zkMetadata.getSegmentName());
  }

  private long getDownloadTimeOutMilliseconds(@Nullable TableConfig tableConfig) {
    return Optional.ofNullable(tableConfig).map(TableConfig::getIngestionConfig)
        .map(IngestionConfig::getStreamIngestionConfig).map(StreamIngestionConfig::getStreamConfigMaps)
        .filter(maps -> !maps.isEmpty()).map(maps -> maps.get(0)).map(map -> map.get(SEGMENT_DOWNLOAD_TIMEOUT_MINUTES))
        .map(timeoutStr -> {
          try {
            return TimeUnit.MINUTES.toMillis(Long.parseLong(timeoutStr));
          } catch (NumberFormatException e) {
            return DEFAULT_SEGMENT_DOWNLOAD_TIMEOUT_MS;
          }
        }).orElse(DEFAULT_SEGMENT_DOWNLOAD_TIMEOUT_MS);
  }

  @VisibleForTesting
  protected RealtimeSegmentDataManager createRealtimeSegmentDataManager(SegmentZKMetadata zkMetadata,
      TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig, Schema schema, LLCSegmentName llcSegmentName,
      SemaphoreAccessCoordinator semaphoreCoordinator, PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      PartitionDedupMetadataManager partitionDedupMetadataManager, BooleanSupplier isTableReadyToConsumeData)
      throws AttemptsExceededException, RetriableOperationException {
    return new RealtimeSegmentDataManager(zkMetadata, tableConfig, this, _indexDir.getAbsolutePath(),
        indexLoadingConfig, schema, llcSegmentName, semaphoreCoordinator, _serverMetrics,
        partitionUpsertMetadataManager, partitionDedupMetadataManager, isTableReadyToConsumeData);
  }

  /**
   * Sets the default time value in the schema as the segment creation time if it is invalid. Time column is used to
   * manage the segments, so its values have to be within the valid range.
   */
  @VisibleForTesting
  static void setDefaultTimeValueIfInvalid(TableConfig tableConfig, Schema schema, SegmentZKMetadata zkMetadata) {
    String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    if (StringUtils.isEmpty(timeColumnName)) {
      return;
    }
    DateTimeFieldSpec timeColumnSpec = schema.getSpecForTimeColumn(timeColumnName);
    Preconditions.checkState(timeColumnSpec != null, "Failed to find time field: %s from schema: %s", timeColumnName,
        schema.getSchemaName());
    String defaultTimeString = timeColumnSpec.getDefaultNullValueString();
    DateTimeFormatSpec dateTimeFormatSpec = timeColumnSpec.getFormatSpec();
    try {
      long defaultTimeMs = dateTimeFormatSpec.fromFormatToMillis(defaultTimeString);
      if (TimeUtils.timeValueInValidRange(defaultTimeMs)) {
        return;
      }
    } catch (Exception e) {
      // Ignore
    }
    String creationTimeString = dateTimeFormatSpec.fromMillisToFormat(zkMetadata.getCreationTime());
    Object creationTime = timeColumnSpec.getDataType().convert(creationTimeString);
    timeColumnSpec.setDefaultNullValue(creationTime);
    LOGGER.info(
        "Default time: {} does not comply with format: {}, using creation time: {} as the default time for table: {}",
        defaultTimeString, timeColumnSpec.getFormat(), creationTime, tableConfig.getTableName());
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    Preconditions.checkState(!_shutDown, "Table data manager is already shut down, cannot add segment: %s to table: %s",
        segmentName, _tableNameWithType);
    if (isUpsertEnabled()) {
      handleUpsert(immutableSegment);
      return;
    }

    if (isDedupEnabled() && immutableSegment instanceof ImmutableSegmentImpl) {
      handleDedup((ImmutableSegmentImpl) immutableSegment);
    }
    super.addSegment(immutableSegment);
  }

  private void handleDedup(ImmutableSegmentImpl immutableSegment) {
    // TODO(saurabh) refactor commons code with handleUpsert
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} with dedup enabled", segmentName);
    Integer partitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, null);
    Preconditions.checkNotNull(partitionId, "PartitionId is not available for segment: '" + segmentName
        + "' (dedup-enabled table: " + _tableNameWithType + ")");
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        _tableDedupMetadataManager.getOrCreatePartitionManager(partitionId);
    immutableSegment.enableDedup(partitionDedupMetadataManager);
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.get(segmentName);
    if (partitionDedupMetadataManager.isPreloading()) {
      partitionDedupMetadataManager.preloadSegment(immutableSegment);
      LOGGER.info("Preloaded immutable segment: {} with dedup enabled", segmentName);
      return;
    }
    if (oldSegmentManager == null) {
      partitionDedupMetadataManager.addSegment(immutableSegment);
      LOGGER.info("Added new immutable segment: {} with dedup enabled", segmentName);
    } else {
      IndexSegment oldSegment = oldSegmentManager.getSegment();
      partitionDedupMetadataManager.replaceSegment(oldSegment, immutableSegment);
      LOGGER.info("Replaced {} segment: {} with dedup enabled",
          oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName);
    }
  }

  private void handleUpsert(ImmutableSegment immutableSegment) {
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} with upsert enabled", segmentName);

    Integer partitionId =
        SegmentUtils.getRealtimeSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, null);
    Preconditions.checkNotNull(partitionId, "Failed to get partition id for segment: " + segmentName
        + " (upsert-enabled table: " + _tableNameWithType + ")");
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionId);

    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    if (partitionUpsertMetadataManager.isPreloading()) {
      // Register segment after it is preloaded and has initialized its validDocIds. The order of preloading and
      // registering segment doesn't matter much as preloading happens before table partition is ready for queries.
      partitionUpsertMetadataManager.preloadSegment(immutableSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
      _logger.info("Preloaded immutable segment: {} with upsert enabled", segmentName);
      return;
    }
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.get(segmentName);
    if (oldSegmentManager == null) {
      // When adding a new segment, we should register it 'before' it is fully initialized by
      // partitionUpsertMetadataManager. Because when processing docs in the new segment, the docs in the other
      // segments may be invalidated, making the queries see less valid docs than expected. We should let query
      // access the new segment asap even though its validDocId bitmap is still being filled by
      // partitionUpsertMetadataManager.
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
      partitionUpsertMetadataManager.trackNewlyAddedSegment(segmentName);
      partitionUpsertMetadataManager.addSegment(immutableSegment);
      _logger.info("Added new immutable segment: {} with upsert enabled", segmentName);
    } else {
      replaceUpsertSegment(segmentName, oldSegmentManager, newSegmentManager, partitionUpsertMetadataManager);
    }
  }

  private void replaceUpsertSegment(String segmentName, SegmentDataManager oldSegmentManager,
      ImmutableSegmentDataManager newSegmentManager, PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    // When replacing a segment, we should register the new segment 'after' it is fully initialized by
    // partitionUpsertMetadataManager to fill up its validDocId bitmap. Otherwise, the queries will lose the access
    // to the valid docs in the old segment immediately, but the validDocId bitmap of the new segment is still
    // being filled by partitionUpsertMetadataManager, making the queries see less valid docs than expected.
    IndexSegment oldSegment = oldSegmentManager.getSegment();
    ImmutableSegment immutableSegment = newSegmentManager.getSegment();
    UpsertConfig.ConsistencyMode consistencyMode = _tableUpsertMetadataManager.getUpsertConsistencyMode();
    if (consistencyMode == UpsertConfig.ConsistencyMode.NONE) {
      partitionUpsertMetadataManager.replaceSegment(immutableSegment, oldSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
    } else {
      // By default, when replacing a segment, the old segment is kept intact and visible to query until the new
      // segment is registered as in the if-branch above. But the newly ingested records will invalidate valid
      // docs in the new segment as the upsert metadata gets updated during replacement, so the query will miss the
      // new updates in the new segment, until it's registered after the replacement is done.
      // For consistent data view, we make both old and new segment visible to the query and update both in place
      // when segment replacement and new data ingestion are happening in parallel.
      SegmentDataManager duoSegmentDataManager = new DuoSegmentDataManager(newSegmentManager, oldSegmentManager);
      registerSegment(segmentName, duoSegmentDataManager, partitionUpsertMetadataManager);
      partitionUpsertMetadataManager.replaceSegment(immutableSegment, oldSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
    }
    _logger.info("Replaced {} segment: {} with upsert enabled and consistency mode: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, consistencyMode);
    oldSegmentManager.offload();
    releaseSegment(oldSegmentManager);
  }

  private void registerSegment(String segmentName, SegmentDataManager segmentDataManager,
      @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    if (partitionUpsertMetadataManager != null) {
      // Register segment to the upsert metadata manager before registering it to table manager, so that the upsert
      // metadata manger can update the upsert view before the segment becomes visible to queries.
      partitionUpsertMetadataManager.trackSegmentForUpsertView(segmentDataManager.getSegment());
    }
    registerSegment(segmentName, segmentDataManager);
  }

  @Override
  protected SegmentDataManager registerSegment(String segmentName, SegmentDataManager segmentDataManager) {
    if (_enforceConsumptionInOrder) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      Preconditions.checkNotNull(llcSegmentName);
      SemaphoreAccessCoordinator semaphoreAccessCoordinator =
          getSemaphoreAccessCoordinator(llcSegmentName.getPartitionGroupId());
      semaphoreAccessCoordinator.trackSegment(llcSegmentName);
    }
    return super.registerSegment(segmentName, segmentDataManager);
  }

  /**
   * Replaces the CONSUMING segment with a downloaded committed one.
   */
  public void downloadAndReplaceConsumingSegment(SegmentZKMetadata zkMetadata)
      throws Exception {
    String segmentName = zkMetadata.getSegmentName();
    _logger.info("Downloading and replacing CONSUMING segment: {} with committed one", segmentName);
    File indexDir = downloadSegment(zkMetadata);
    // Get a new index loading config with latest table config and schema to load the segment
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
    addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, _segmentOperationsThrottler));
    _logger.info("Downloaded and replaced CONSUMING segment: {}", segmentName);
  }

  /**
   * Replaces the CONSUMING segment with the one sealed locally.
   */
  public void replaceConsumingSegment(String segmentName)
      throws Exception {
    _logger.info("Replacing CONSUMING segment: {} with the one sealed locally", segmentName);
    File indexDir = new File(_indexDir, segmentName);
    // Get a new index loading config with latest table config and schema to load the segment
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, _segmentOperationsThrottler));
    _logger.info("Replaced CONSUMING segment: {}", segmentName);
  }

  public String getServerInstance() {
    return _instanceId;
  }

  @VisibleForTesting
  public TableUpsertMetadataManager getTableUpsertMetadataManager() {
    return _tableUpsertMetadataManager;
  }

  /**
   * Retrieves a mapping of partition id to the primary key count for the partition.
   *
   * @return A {@code Map} where keys are partition id and values are count of primary keys for that specific partition.
   */
  public Map<Integer, Long> getUpsertPartitionToPrimaryKeyCount() {
    if (isUpsertEnabled()) {
      return _tableUpsertMetadataManager.getPartitionToPrimaryKeyCount();
    }
    return Collections.emptyMap();
  }

  public ConcurrentHashMap.KeySetView<String, SegmentDataManager> getSegments() {
    return _segmentDataManagerMap.keySet();
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
   */
  private void validate(TableConfig tableConfig, Schema schema) {
    // 1. Make sure that the sorted column is not a multi-value field.
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (CollectionUtils.isNotEmpty(sortedColumns)) {
      String sortedColumn = sortedColumns.get(0);
      if (sortedColumns.size() > 1) {
        _logger.warn("More than one sorted column configured. Using {}", sortedColumn);
      }
      FieldSpec fieldSpec = schema.getFieldSpecFor(sortedColumn);
      Preconditions.checkArgument(fieldSpec != null, "Failed to find sorted column: %s in schema for table: %s",
          sortedColumn, _tableNameWithType);
      Preconditions.checkArgument(fieldSpec.isSingleValueField(),
          "Cannot configure multi-valued column %s as sorted column for table: %s", sortedColumn, _tableNameWithType);
    }
    // 2. Validate the schema itself
    SchemaUtils.validate(schema);
  }

  private SemaphoreAccessCoordinator getSemaphoreAccessCoordinator(int partitionGroupId) {
    return _partitionGroupIdToSemaphoreCoordinatorMap.computeIfAbsent(partitionGroupId,
        k -> new SemaphoreAccessCoordinator(new Semaphore(1), _enforceConsumptionInOrder, this));
  }

  public BooleanSupplier getIsTableReadyToConsumeData() {
    return _isTableReadyToConsumeData;
  }

  private boolean enforceConsumptionInOrder() {
    if (_tableConfig.getIngestionConfig() == null
        || _tableConfig.getIngestionConfig().getStreamIngestionConfig() == null) {
      return (isDedupEnabled() || isPartialUpsertEnabled());
    }
    return _tableConfig.getIngestionConfig().getStreamIngestionConfig().isEnforceConsumptionInOrder() && (
        isDedupEnabled() || isPartialUpsertEnabled());
  }
}
