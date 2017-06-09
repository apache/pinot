/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.manager.realtime;

import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.manager.offline.AbstractTableDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaConsumerManager;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.loader.LoaderUtils;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.LoggerFactory;

// TODO Use the refcnt object inside SegmentDataManager
public class RealtimeTableDataManager extends AbstractTableDataManager {

  private final ExecutorService _segmentAsyncExecutorService = Executors
      .newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  private SegmentBuildTimeLeaseExtender _leaseExtender;

  public RealtimeTableDataManager() {
    super();
  }

  @Override
  protected void doShutdown() {
    _segmentAsyncExecutorService.shutdown();
    for (SegmentDataManager segmentDataManager :_segmentsMap.values() ) {
      segmentDataManager.destroy();
    }
    KafkaConsumerManager.closeAllConsumers();
    if (_leaseExtender != null) {
      _leaseExtender.shutDown();
    }
  }

  protected void doInit() {
    _leaseExtender = SegmentBuildTimeLeaseExtender.create(getServerInstance());
    LOGGER = LoggerFactory.getLogger(_tableName + "-RealtimeTableDataManager");
  }

  public void notifySegmentCommitted(RealtimeSegmentZKMetadata metadata, IndexSegment segment) {
    ZKMetadataProvider.setRealtimeSegmentZKMetadata(_helixPropertyStore, metadata);
    markSegmentAsLoaded(metadata.getSegmentName());
    addSegment(segment);
  }

  /*
   * This call comes in one of two ways:
   * For HL Segments:
   * - We are being directed by helix to own up all the segments that we committed and are still in retention. In this case
   *   we treat it exactly like how OfflineTableDataManager would -- wrap it into an OfflineSegmentDataManager, and put it
   *   in the map.
   * - We are being asked to own up a new realtime segment. In this case, we wrap the segment with a RealTimeSegmentDataManager
   *   (that kicks off Kafka consumption). When the segment is committed we get notified via the notifySegmentCommitted call, at
   *   which time we replace the segment with the OfflineSegmentDataManager
   * For LL Segments:
   * - We are being asked to start consuming from a kafka partition.
   * - We did not know about the segment and are being asked to download and own the segment (re-balancing, or
   *   replacing a realtime server with a fresh one, maybe). We need to look at segment metadata and decide whether
   *   to start consuming or download the segment.
   */
  @Override
  public void addSegment(@Nonnull ZkHelixPropertyStore<ZNRecord> propertyStore, @Nonnull TableConfig tableConfig,
      @Nullable InstanceZKMetadata instanceZKMetadata, @Nonnull SegmentZKMetadata segmentZKMetadata,
      @Nonnull IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    // TODO FIXME
    // Hack. We get the _helixPropertyStore here and save it, knowing that we will get this addSegment call
    // before the notifyCommitted call (that uses _helixPropertyStore)
    _helixPropertyStore = propertyStore;

    String segmentName = segmentZKMetadata.getSegmentName();
    String tableName = segmentZKMetadata.getTableName();
    if (!(segmentZKMetadata instanceof RealtimeSegmentZKMetadata)) {
      throw new IllegalArgumentException(
          "Trying to add a segment into REALTIME table with non-REALTIME segment ZK metadata");
    }
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = (RealtimeSegmentZKMetadata) segmentZKMetadata;
    LOGGER.info("Attempting to add realtime segment {} for table {}", segmentName, tableName);

    File indexDir = new File(_indexDir, segmentName);
    // Restart during segment reload might leave segment in inconsistent state (index directory might not exist but
    // segment backup directory existed), need to first try to recover from reload failure before checking the existence
    // of the index directory and loading segment from it
    LoaderUtils.reloadFailureRecovery(indexDir);
    if (indexDir.exists() && (realtimeSegmentZKMetadata.getStatus() == Status.DONE)) {
      // segment already exists on file, and we have committed the realtime segment in ZK. Treat it like an offline segment
      if (_segmentsMap.containsKey(segmentName)) {
        LOGGER.warn("Got reload for segment already on disk {} table {}, have {}", segmentName, tableName,
            _segmentsMap.get(segmentName).getClass().getSimpleName());
        return;
      }

      IndexSegment segment = ColumnarSegmentLoader.load(indexDir, indexLoadingConfig);
      addSegment(segment);
      markSegmentAsLoaded(segmentName);
    } else {
      // Either we don't have the segment on disk or we have not committed in ZK. We should be starting the consumer
      // for realtime segment here. If we wrote it on disk but could not get to commit to zk yet, we should replace the
      // on-disk segment next time
      if (_segmentsMap.containsKey(segmentName)) {
        LOGGER.warn("Got reload for segment not on disk {} table {}, have {}", segmentName, tableName,
            _segmentsMap.get(segmentName).getClass().getSimpleName());
        return;
      }
      Schema schema =
          ZKMetadataProvider.getTableSchema(propertyStore, TableNameBuilder.REALTIME.tableNameWithType(tableName));
      if (!isValid(schema, tableConfig.getIndexingConfig())) {
        LOGGER.error("Not adding segment {}", segmentName);
        throw new RuntimeException("Mismatching schema/table config for " + _tableName);
      }
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
      LOGGER.info("Initialize RealtimeSegmentDataManager - " + segmentName);
      try {
        _rwLock.writeLock().lock();
        _segmentsMap.put(segmentName, manager);
      } finally {
        _rwLock.writeLock().unlock();
      }
      _loadingSegments.add(segmentName);
    }
  }

  public void downloadAndReplaceSegment(@Nonnull String segmentName,
      @Nonnull LLCRealtimeSegmentZKMetadata llcSegmentMetadata, @Nonnull IndexLoadingConfig indexLoadingConfig) {
    final String uri = llcSegmentMetadata.getDownloadUrl();
    File tempSegmentFolder =
        new File(_indexDir, "tmp-" + segmentName + "." + String.valueOf(System.currentTimeMillis()));
    File tempFile = new File(_indexDir, segmentName + ".tar.gz");
    try {
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI(uri).fetchSegmentToLocal(uri, tempFile);
      LOGGER.info("Downloaded file from {} to {}; Length of downloaded file: {}", uri, tempFile, tempFile.length());
      TarGzCompressionUtils.unTar(tempFile, tempSegmentFolder);
      LOGGER.info("Uncompressed file {} into tmp dir {}", tempFile, tempSegmentFolder);
      FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], new File(_indexDir, segmentName));
      LOGGER.info("Replacing LLC Segment {}", segmentName);
      replaceLLSegment(segmentName, indexLoadingConfig);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(tempFile);
      FileUtils.deleteQuietly(tempSegmentFolder);
    }
  }

  // Replace a committed segment.
  public void replaceLLSegment(@Nonnull String segmentName, @Nonnull IndexLoadingConfig indexLoadingConfig) {
    try {
      IndexSegment indexSegment = ColumnarSegmentLoader.load(new File(_indexDir, segmentName), indexLoadingConfig);
      addSegment(indexSegment);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getServerInstance() {
    return _serverInstance;
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
        LOGGER.warn("More than one sorted column configured. Using {}", sortedColumn);
      }
      FieldSpec fieldSpec = schema.getFieldSpecFor(sortedColumn);
      if (!fieldSpec.isSingleValueField()) {
        LOGGER.error("Cannot configure multi-valued column {} as sorted column", sortedColumn);
        isValid = false;
      }
    }
    // 2. We want to get the schema errors, if any, even if isValid is false;
    if (!schema.validate(LOGGER)) {
      isValid = false;
    }

    return isValid;
  }

  @Override
  public void addSegment(@Nonnull SegmentMetadata segmentMetadata, @Nonnull IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema)
      throws Exception {
    throw new UnsupportedOperationException(
        "Unsupported adding segment: " + segmentMetadata.getName() + " to OFFLINE table: "
            + segmentMetadata.getTableName() + " using RealtimeTableDataManager");
  }

  private void markSegmentAsLoaded(String segmentId) {
    _loadingSegments.remove(segmentId);
    if (!_activeSegments.contains(segmentId)) {
      _activeSegments.add(segmentId);
    }
  }
}
