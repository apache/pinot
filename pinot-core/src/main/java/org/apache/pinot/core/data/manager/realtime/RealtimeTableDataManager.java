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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.segment.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.loader.LoaderUtils;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;


@ThreadSafe
public class RealtimeTableDataManager extends BaseTableDataManager {
  private final ExecutorService _segmentAsyncExecutorService =
      Executors.newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));
  private SegmentBuildTimeLeaseExtender _leaseExtender;
  private RealtimeSegmentStatsHistory _statsHistory;
  private final Semaphore _segmentBuildSemaphore;

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

  public RealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
    _segmentBuildSemaphore = segmentBuildSemaphore;
  }

  @Override
  protected void doInit() {
    _leaseExtender = SegmentBuildTimeLeaseExtender.create(_instanceId, _serverMetrics, _tableNameWithType);

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

    File indexDir = new File(_indexDir, segmentName);
    // Restart during segment reload might leave segment in inconsistent state (index directory might not exist but
    // segment backup directory existed), need to first try to recover from reload failure before checking the existence
    // of the index directory and loading segment from it
    LoaderUtils.reloadFailureRecovery(indexDir);

    if (indexDir.exists() && (realtimeSegmentZKMetadata.getStatus() == Status.DONE)) {
      // Segment already exists on disk, and metadata has been committed. Treat it like an offline segment

      addSegment(ImmutableSegmentLoader.load(indexDir, indexLoadingConfig, schema));
    } else {
      // Either we don't have the segment on disk or we have not committed in ZK. We should be starting the consumer
      // for realtime segment here. If we wrote it on disk but could not get to commit to zk yet, we should replace the
      // on-disk segment next time

      if (!isValid(schema, tableConfig.getIndexingConfig())) {
        _logger.error("Not adding segment {}", segmentName);
        throw new RuntimeException("Mismatching schema/table config for " + _tableNameWithType);
      }
      VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSchema(schema);

      InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(_propertyStore, _instanceId);

      SegmentDataManager manager;
      if (SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
        manager = new HLRealtimeSegmentDataManager(realtimeSegmentZKMetadata, tableConfig, instanceZKMetadata, this,
            _indexDir.getAbsolutePath(), indexLoadingConfig, schema, _serverMetrics);
      } else {
        LLCRealtimeSegmentZKMetadata llcSegmentMetadata = (LLCRealtimeSegmentZKMetadata) realtimeSegmentZKMetadata;
        if (realtimeSegmentZKMetadata.getStatus().equals(Status.DONE)) {
          // TODO Remove code duplication here and in LLRealtimeSegmentDataManager
          downloadAndReplaceSegment(segmentName, llcSegmentMetadata, indexLoadingConfig);
          return;
        }
        manager = new LLRealtimeSegmentDataManager(realtimeSegmentZKMetadata, tableConfig, instanceZKMetadata, this,
            _indexDir.getAbsolutePath(), indexLoadingConfig, schema, _serverMetrics);
      }
      _logger.info("Initialize RealtimeSegmentDataManager - " + segmentName);
      _segmentDataManagerMap.put(segmentName, manager);
    }
  }

  public void downloadAndReplaceSegment(String segmentName, LLCRealtimeSegmentZKMetadata llcSegmentMetadata,
      IndexLoadingConfig indexLoadingConfig) {
    final String uri = llcSegmentMetadata.getDownloadUrl();
    File tempSegmentFolder = new File(_indexDir, "tmp-" + segmentName + "." + System.currentTimeMillis());
    File tempFile = new File(_indexDir, segmentName + ".tar.gz");
    final File segmentFolder = new File(_indexDir, segmentName);
    FileUtils.deleteQuietly(segmentFolder);
    try {
      SegmentFetcherFactory.getInstance().getSegmentFetcherBasedOnURI(uri).fetchSegmentToLocal(uri, tempFile);
      _logger.info("Downloaded file from {} to {}; Length of downloaded file: {}", uri, tempFile, tempFile.length());
      TarGzCompressionUtils.unTar(tempFile, tempSegmentFolder);
      _logger.info("Uncompressed file {} into tmp dir {}", tempFile, tempSegmentFolder);
      FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], segmentFolder);
      _logger.info("Replacing LLC Segment {}", segmentName);
      replaceLLSegment(segmentName, indexLoadingConfig);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(tempFile);
      FileUtils.deleteQuietly(tempSegmentFolder);
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
   * @param schema
   * @param indexingConfig
   * @return true if schema is valid.
   */
  private boolean isValid(Schema schema, IndexingConfig indexingConfig) {
    // 1. Make sure that the sorted column is not a multi-value field.
    List<String> sortedColumns = indexingConfig.getSortedColumn();
    boolean isValid = true;
    if (!sortedColumns.isEmpty()) {
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
    if (!schema.validate(_logger)) {
      isValid = false;
    }

    return isValid;
  }
}
