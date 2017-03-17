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

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.IndexingConfig;
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
import com.linkedin.pinot.common.utils.SchemaUtils;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.helix.PinotHelixPropertyStoreZnRecordProvider;
import com.linkedin.pinot.core.data.manager.offline.AbstractTableDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaConsumerManager;

// TODO Use the refcnt object inside SegmentDataManager
public class RealtimeTableDataManager extends AbstractTableDataManager {

  private final ExecutorService _segmentAsyncExecutorService = Executors
      .newSingleThreadExecutor(new NamedThreadFactory("SegmentAsyncExecutorService"));
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;

  public RealtimeTableDataManager() {
    super();
  }

  public int getAvgMultiValueCount() {
    return _tableDataManagerConfig.getRealtimeAvgMvCount();
  }

  @Override
  protected void doShutdown() {
    _segmentAsyncExecutorService.shutdown();
    for (SegmentDataManager segmentDataManager :_segmentsMap.values() ) {
      segmentDataManager.destroy();
    }
    KafkaConsumerManager.closeAllConsumers();
  }

  protected void doInit() {
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
  public void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata inputSegmentZKMetadata) throws Exception {
    // TODO FIXME
    // Hack. We get the _helixPropertyStore here and save it, knowing that we will get this addSegment call
    // before the notifyCommitted call (that uses _helixPropertyStore)
    this._helixPropertyStore = propertyStore;

    final String segmentId = inputSegmentZKMetadata.getSegmentName();
    final String tableName = inputSegmentZKMetadata.getTableName();
    if (!(inputSegmentZKMetadata instanceof  RealtimeSegmentZKMetadata)) {
      LOGGER.warn("Got called with an unexpected instance object:{},table {}, segment {}",
          inputSegmentZKMetadata.getClass().getSimpleName(), tableName, segmentId);
      return;
    }
    RealtimeSegmentZKMetadata segmentZKMetadata = (RealtimeSegmentZKMetadata) inputSegmentZKMetadata;
    LOGGER.info("Attempting to add realtime segment {} for table {}", segmentId, tableName);

    if (new File(_indexDir, segmentId).exists() && (segmentZKMetadata).getStatus() == Status.DONE) {
      // segment already exists on file, and we have committed the realtime segment in ZK. Treat it like an offline segment
      if (_segmentsMap.containsKey(segmentId)) {
        LOGGER.warn("Got reload for segment already on disk {} table {}, have {}", segmentId, tableName,
            _segmentsMap.get(segmentId).getClass().getSimpleName());
        return;
      }

      IndexSegment segment = ColumnarSegmentLoader.load(new File(_indexDir, segmentId), _readMode, _indexLoadingConfigMetadata);
      addSegment(segment);
      markSegmentAsLoaded(segmentId);
    } else {
      // Either we don't have the segment on disk or we have not committed in ZK. We should be starting the consumer
      // for realtime segment here. If we wrote it on disk but could not get to commit to zk yet, we should replace the
      // on-disk segment next time
      if (_segmentsMap.containsKey(segmentId)) {
        LOGGER.warn("Got reload for segment not on disk {} table {}, have {}", segmentId, tableName,
            _segmentsMap.get(segmentId).getClass().getSimpleName());
        return;
      }
      PinotHelixPropertyStoreZnRecordProvider propertyStoreHelper = PinotHelixPropertyStoreZnRecordProvider.forSchema(propertyStore);
      ZNRecord record = propertyStoreHelper.get(tableConfig.getValidationConfig().getSchemaName());
      LOGGER.info("Found schema {} ", tableConfig.getValidationConfig().getSchemaName());
      Schema schema = SchemaUtils.fromZNRecord(record);
      if (!isValid(schema, tableConfig.getIndexingConfig())) {
        LOGGER.error("Not adding segment {}", segmentId);
        throw new RuntimeException("Mismatching schema/table config for " + _tableName);
      }
      SegmentDataManager manager;
      if (SegmentName.isHighLevelConsumerSegmentName(segmentId)) {
        manager = new HLRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, instanceZKMetadata, this,
            _indexDir.getAbsolutePath(), _readMode, SchemaUtils.fromZNRecord(record), _serverMetrics);
      } else {
        LLCRealtimeSegmentZKMetadata llcSegmentMetadata = (LLCRealtimeSegmentZKMetadata) segmentZKMetadata;
        if (segmentZKMetadata.getStatus().equals(Status.DONE)) {
          // TODO Remove code duplication here and in LLRealtimeSegmentDataManager
          downloadAndReplaceSegment(segmentId, llcSegmentMetadata);
          return;
        }
        manager = new LLRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, instanceZKMetadata, this,
            _indexDir.getAbsolutePath(), SchemaUtils.fromZNRecord(record), _serverMetrics);
      }
      LOGGER.info("Initialize RealtimeSegmentDataManager - " + segmentId);
      try {
        _rwLock.writeLock().lock();
        _segmentsMap.put(segmentId, manager);
      } finally {
        _rwLock.writeLock().unlock();
      }
      _loadingSegments.add(segmentId);
    }
  }

  public void downloadAndReplaceSegment(final String segmentNameStr, LLCRealtimeSegmentZKMetadata llcSegmentMetadata) {
    final String uri = llcSegmentMetadata.getDownloadUrl();
    File tempSegmentFolder = new File(_indexDir, "tmp-" + segmentNameStr + "." + String.valueOf(System.currentTimeMillis()));
    File tempFile = new File(_indexDir, segmentNameStr + ".tar.gz");
    try {
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI(uri).fetchSegmentToLocal(uri, tempFile);
      LOGGER.info("Downloaded file from {} to {}; Length of downloaded file: {}", uri, tempFile, tempFile.length());
      TarGzCompressionUtils.unTar(tempFile, tempSegmentFolder);
      LOGGER.info("Uncompressed file {} into tmp dir {}", tempFile, tempSegmentFolder);
      FileUtils.moveDirectory(tempSegmentFolder.listFiles()[0], new File(_indexDir, segmentNameStr));
      LOGGER.info("Replacing LLC Segment {}", segmentNameStr);
      replaceLLSegment(segmentNameStr);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      FileUtils.deleteQuietly(tempFile);
      FileUtils.deleteQuietly(tempSegmentFolder);
    }
    return;
  }

  // Replace a committed segment.
  public void replaceLLSegment(String segmentId) {
    try {
      IndexSegment segment = ColumnarSegmentLoader.load(new File(_indexDir, segmentId), _readMode, _indexLoadingConfigMetadata);
      addSegment(segment);
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
  public void addSegment(SegmentMetadata segmentMetaToAdd, Schema schema)
      throws Exception {
    throw new UnsupportedOperationException(
        "Unsupported addSegment(SegmentMetadata, Schema) in RealtimeTableDataManager for table: "
            + segmentMetaToAdd.getTableName() + " segment: " + segmentMetaToAdd.getName());
  }

  private void markSegmentAsLoaded(String segmentId) {
    _loadingSegments.remove(segmentId);
    if (!_activeSegments.contains(segmentId)) {
      _activeSegments.add(segmentId);
    }
  }
}
