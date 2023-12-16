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
package org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.http.client.utils.URIBuilder;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.RealtimeToOfflineSegmentsTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.framework.MergeType;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.plugin.minion.tasks.BaseMultipleSegmentsConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MergeTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A task to convert segments from a REALTIME table to segments for its corresponding OFFLINE table.
 * The realtime segments could span across multiple time windows.
 * This task extracts data and creates segments for a configured time window.
 * The {@link SegmentProcessorFramework} is used for the segment conversion, which does the following steps
 * 1. Filter records based on the time window
 * 2. Round the time value in the records (optional)
 * 3. Partition the records if partitioning is enabled in the table config
 * 4. Merge records based on the merge type
 * 5. Sort records if sorting is enabled in the table config
 *
 * Before beginning the task, the <code>watermarkMs</code> is checked in the minion task metadata ZNode,
 * located at MINION_TASK_METADATA/${tableNameWithType}/RealtimeToOfflineSegmentsTask
 * It should match the <code>windowStartMs</code>.
 * The version of the znode is cached.
 *
 * After the segments are uploaded, this task updates the <code>watermarkMs</code> in the minion task metadata ZNode.
 * The znode version is checked during update,
 * and update only succeeds if version matches with the previously cached version
 */
public class RealtimeToOfflineSegmentsTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeToOfflineSegmentsTaskExecutor.class);

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;
  private int _expectedVersion = Integer.MIN_VALUE;

  private static HelixManager _helixManager = MINION_CONTEXT.getHelixManager();
  private static HelixAdmin _clusterManagementTool = _helixManager.getClusterManagmentTool();
  private static String _clusterName = _helixManager.getClusterName();

  
  public RealtimeToOfflineSegmentsTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager,
      MinionConf minionConf) {
    super(minionConf);
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
  }

  /**
   * Fetches the RealtimeToOfflineSegmentsTask metadata ZNode for the realtime table.
   * Checks that the <code>watermarkMs</code> from the ZNode matches the windowStartMs in the task configs.
   * If yes, caches the ZNode version to check during update.
   */
  @Override
  public void preProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);

    ZNRecord realtimeToOfflineSegmentsTaskZNRecord =
        _minionTaskZkMetadataManager.getTaskMetadataZNRecord(realtimeTableName,
            RealtimeToOfflineSegmentsTask.TASK_TYPE);
    Preconditions.checkState(realtimeToOfflineSegmentsTaskZNRecord != null,
        "RealtimeToOfflineSegmentsTaskMetadata ZNRecord for table: %s should not be null. Exiting task.",
        realtimeTableName);

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(realtimeToOfflineSegmentsTaskZNRecord);
    long windowStartMs = Long.parseLong(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_START_MS_KEY));
    Preconditions.checkState(realtimeToOfflineSegmentsTaskMetadata.getWatermarkMs() <= windowStartMs,
        "watermarkMs in RealtimeToOfflineSegmentsTask metadata: %s shouldn't be larger than windowStartMs: %d in task"
            + " configs for table: %s. ZNode may have been modified by another task",
        realtimeToOfflineSegmentsTaskMetadata.getWatermarkMs(), windowStartMs, realtimeTableName);

    _expectedVersion = realtimeToOfflineSegmentsTaskZNRecord.getVersion();
  }

  @Override
  protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> segmentDirs,
      File workingDir)
      throws Exception {
    int numInputSegments = segmentDirs.size();
    _eventObserver.notifyProgress(pinotTaskConfig, "Converting segments: " + numInputSegments);
    String taskType = pinotTaskConfig.getTaskType();
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String rawTableName = TableNameBuilder.extractRawTableName(realtimeTableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    TableConfig tableConfig = getTableConfig(offlineTableName);
    Schema schema = getSchema(offlineTableName);

    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema);

    // Time handler config
    segmentProcessorConfigBuilder
        .setTimeHandlerConfig(MergeTaskUtils.getTimeHandlerConfig(tableConfig, schema, configs));

    // Partitioner config
    segmentProcessorConfigBuilder
        .setPartitionerConfigs(MergeTaskUtils.getPartitionerConfigs(tableConfig, schema, configs));

    // Merge type
    MergeType mergeType = MergeTaskUtils.getMergeType(configs);
    // Handle legacy key
    if (mergeType == null) {
      String legacyMergeTypeStr = configs.get(RealtimeToOfflineSegmentsTask.COLLECTOR_TYPE_KEY);
      if (legacyMergeTypeStr != null) {
        mergeType = MergeType.valueOf(legacyMergeTypeStr.toUpperCase());
      }
    }
    segmentProcessorConfigBuilder.setMergeType(mergeType);

    // Aggregation types
    segmentProcessorConfigBuilder.setAggregationTypes(MergeTaskUtils.getAggregationTypes(configs));

    // Segment config
    segmentProcessorConfigBuilder.setSegmentConfig(MergeTaskUtils.getSegmentConfig(configs));

    // Progress observer
    segmentProcessorConfigBuilder.setProgressObserver(p -> _eventObserver.notifyProgress(_pinotTaskConfig, p));

    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    List<RecordReader> recordReaders = new ArrayList<>(numInputSegments);
    int count = 1;
  
    ImmutableRoaringBitmap validDocIds = null;
    if (configs.get("shouldUseCompactReader").equals("true")) {
      validDocIds = getValidDocIds(realtimeTableName, configs);
    }

    for (File segmentDir : segmentDirs) {
      _eventObserver.notifyProgress(_pinotTaskConfig,
          String.format("Creating RecordReader for: %s (%d out of %d)", segmentDir, count++, numInputSegments));   
      if (validDocIds != null) {
        CompactedRecordReader recordReader = new CompactedRecordReader(segmentDir, validDocIds);
        recordReaders.add(recordReader);
      } else {
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      // NOTE: Do not fill null field with default value to be consistent with other record readers
      recordReader.init(segmentDir, null, null, true);
      recordReaders.add(recordReader);
      }
    }
    List<File> outputSegmentDirs;
    try {
      _eventObserver.notifyProgress(_pinotTaskConfig, "Generating segments");
      outputSegmentDirs = new SegmentProcessorFramework(recordReaders, segmentProcessorConfig, workingDir).process();
    } finally {
      for (RecordReader recordReader : recordReaders) {
        recordReader.close();
      }
    }

    long endMillis = System.currentTimeMillis();
    LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType, configs, (endMillis - startMillis));
    List<SegmentConversionResult> results = new ArrayList<>();
    for (File outputSegmentDir : outputSegmentDirs) {
      String outputSegmentName = outputSegmentDir.getName();
      results.add(new SegmentConversionResult.Builder().setFile(outputSegmentDir).setSegmentName(outputSegmentName)
          .setTableNameWithType(offlineTableName).build());
    }
    return results;
  }

  /**
   * Fetches the RealtimeToOfflineSegmentsTask metadata ZNode for the realtime table.
   * Checks that the version of the ZNode matches with the version cached earlier. If yes, proceeds to update
   * watermark in the ZNode
   * TODO: Making the minion task update the ZK metadata is an anti-pattern, however cannot see another way to do it
   */
  @Override
  public void postProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String realtimeTableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    long waterMarkMs = Long.parseLong(configs.get(RealtimeToOfflineSegmentsTask.WINDOW_END_MS_KEY));
    RealtimeToOfflineSegmentsTaskMetadata newMinionMetadata =
        new RealtimeToOfflineSegmentsTaskMetadata(realtimeTableName, waterMarkMs);
    _minionTaskZkMetadataManager.setTaskMetadataZNRecord(newMinionMetadata, RealtimeToOfflineSegmentsTask.TASK_TYPE,
        _expectedVersion);
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.emptyMap());
  }

   // TODO: Consider moving this method to a more appropriate class (eg ServerSegmentMetadataReader)
  private static ImmutableRoaringBitmap getValidDocIds(String tableNameWithType, Map<String, String> configs)
      throws URISyntaxException {
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String server = getServer(segmentName, tableNameWithType);

    // get the url for the validDocIds for the server
    InstanceConfig instanceConfig = _clusterManagementTool.getInstanceConfig(_clusterName, server);
    String endpoint = InstanceUtils.getServerAdminEndpoint(instanceConfig);
    String url =
        new URIBuilder(endpoint).setPath(String.format("/segments/%s/%s/validDocIds", tableNameWithType, segmentName))
            .toString();

    // get the validDocIds from that server
    Response response = ClientBuilder.newClient().target(url).request().get(Response.class);
    Preconditions.checkState(response.getStatus() == Response.Status.OK.getStatusCode(),
        "Unable to retrieve validDocIds from %s", url);
    byte[] snapshot = response.readEntity(byte[].class);
    ImmutableRoaringBitmap validDocIds = new ImmutableRoaringBitmap(ByteBuffer.wrap(snapshot));
    return validDocIds;
  }

  @VisibleForTesting
  public static String getServer(String segmentName, String tableNameWithType) {
    ExternalView externalView = _clusterManagementTool.getResourceExternalView(_clusterName, tableNameWithType);
    if (externalView == null) {
      throw new IllegalStateException("External view does not exist for table: " + tableNameWithType);
    }
    Map<String, String> instanceStateMap = externalView.getStateMap(segmentName);
    if (instanceStateMap == null) {
      throw new IllegalStateException("Failed to find segment: " + segmentName);
    }
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      if (entry.getValue().equals(SegmentStateModel.ONLINE)) {
        return entry.getKey();
      }
    }
    throw new IllegalStateException("Failed to find ONLINE server for segment: " + segmentName);
  }

  private class CompactedRecordReader implements RecordReader {
    private final PinotSegmentRecordReader _pinotSegmentRecordReader;
    private final PeekableIntIterator _validDocIdsIterator;
    // Reusable generic row to store the next row to return
    GenericRow _nextRow = new GenericRow();
    // Flag to mark whether we need to fetch another row
    boolean _nextRowReturned = true;

    CompactedRecordReader(File indexDir, ImmutableRoaringBitmap validDocIds) {
      _pinotSegmentRecordReader = new PinotSegmentRecordReader();
      _pinotSegmentRecordReader.init(indexDir, null, null);
      _validDocIdsIterator = validDocIds.getIntIterator();
    }

    @Override
    public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
    }

    @Override
    public boolean hasNext() {
      if (!_validDocIdsIterator.hasNext() && _nextRowReturned) {
        return false;
      }

      // If next row has not been returned, return true
      if (!_nextRowReturned) {
        return true;
      }

      // Try to get the next row to return
      if (_validDocIdsIterator.hasNext()) {
        int docId = _validDocIdsIterator.next();
        _nextRow.clear();
        _pinotSegmentRecordReader.getRecord(docId, _nextRow);
        _nextRowReturned = false;
        return true;
      }

      // Cannot find next row to return, return false
      return false;
    }

    @Override
    public GenericRow next() {
      return next(new GenericRow());
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      Preconditions.checkState(!_nextRowReturned);
      reuse.init(_nextRow);
      _nextRowReturned = true;
      return reuse;
    }

    @Override
    public void rewind() {
      _pinotSegmentRecordReader.rewind();
      _nextRowReturned = true;
    }

    @Override
    public void close()
        throws IOException {
      _pinotSegmentRecordReader.close();
    }
  }
}
