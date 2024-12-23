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

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.minion.ExpectedSubtaskResult;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
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
import org.apache.pinot.segment.spi.creator.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
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
 *
 * Before the segments are uploaded, this task updates the <code>ExpectedRealtimeToOfflineTaskResultList</code>
 * in the minion task metadata ZNode.
 * The znode version is checked during update, retrying until max attempts and version of znode is equal to expected.
 * Reason for above is that, since multiple subtasks run in parallel, there can be race condition
 * with updating the znode.
 */
public class RealtimeToOfflineSegmentsTaskExecutor extends BaseMultipleSegmentsConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeToOfflineSegmentsTaskExecutor.class);
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;

  public RealtimeToOfflineSegmentsTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager,
      MinionConf minionConf) {
    super(minionConf);
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
  }

  /**
   * Fetches the RealtimeToOfflineSegmentsTask metadata ZNode for the realtime table.
   * Checks that the <code>watermarkMs</code> from the ZNode matches the windowStartMs in the task configs.
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
    Preconditions.checkState(realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs() == windowStartMs,
        "watermarkMs in RealtimeToOfflineSegmentsTask metadata: %s shouldn't be larger than windowStartMs: %d in task"
            + " configs for table: %s. ZNode may have been modified by another task",
        realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs(), windowStartMs, realtimeTableName);
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

    // Aggregation function parameters
    segmentProcessorConfigBuilder.setAggregationFunctionParameters(
        MergeTaskUtils.getAggregationFunctionParameters(configs));

    // Segment config
    segmentProcessorConfigBuilder.setSegmentConfig(MergeTaskUtils.getSegmentConfig(configs));

    // Since multiple subtasks run in parallel, there shouldn't be a name conflict.
    // Append uuid
    segmentProcessorConfigBuilder.setSegmentNameGenerator(
        new SimpleSegmentNameGenerator(rawTableName, null, true, false));

    // Progress observer
    segmentProcessorConfigBuilder.setProgressObserver(p -> _eventObserver.notifyProgress(_pinotTaskConfig, p));

    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    List<RecordReader> recordReaders = new ArrayList<>(numInputSegments);
    int count = 1;
    for (File segmentDir : segmentDirs) {
      _eventObserver.notifyProgress(_pinotTaskConfig,
          String.format("Creating RecordReader for: %s (%d out of %d)", segmentDir, count++, numInputSegments));
      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      // NOTE: Do not fill null field with default value to be consistent with other record readers
      recordReader.init(segmentDir, null, null, true);
      recordReaders.add(recordReader);
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
   * Before uploading the segments, updates the metadata with the expected results
   * of the successful execution of current subtask.
   * The expected result updated in metadata is read by the next iteration of Task Generator
   * to ensure data correctness.
   */
  @Override
  protected void preUploadSegments(SegmentUploadContext context)
      throws Exception {
    super.preUploadSegments(context);
    String realtimeTableName = context.getTableNameWithType();
    int attemptCount;
    try {
      attemptCount = DEFAULT_RETRY_POLICY.attempt(() -> {
        try {
          ZNRecord realtimeToOfflineSegmentsTaskZNRecord =
              _minionTaskZkMetadataManager.getTaskMetadataZNRecord(realtimeTableName,
                  RealtimeToOfflineSegmentsTask.TASK_TYPE);
          int expectedVersion = realtimeToOfflineSegmentsTaskZNRecord.getVersion();

          // Adding ExpectedRealtimeToOfflineSegmentsTaskResult might fail.
          // In-case of failure there will be runtime exception thrown
          RealtimeToOfflineSegmentsTaskMetadata updatedRealtimeToOfflineSegmentsTaskMetadata =
              getUpdatedTaskMetadata(context, realtimeToOfflineSegmentsTaskZNRecord);

          // Setting to zookeeper might fail due to version mismatch, but in this case
          // the exception is caught and retried.
          _minionTaskZkMetadataManager.setTaskMetadataZNRecord(updatedRealtimeToOfflineSegmentsTaskMetadata,
              RealtimeToOfflineSegmentsTask.TASK_TYPE,
              expectedVersion);
          return true;
        } catch (ZkBadVersionException e) {
          LOGGER.info(
              "Version changed while updating num of subtasks left in RTO task metadata for table: {}, Retrying.",
              realtimeTableName);
          return false;
        }
      });
    } catch (Exception e) {
      String errorMsg =
          String.format("Failed to update the RealtimeToOfflineSegmentsTaskMetadata during preUploadSegments. "
              + "(tableName = %s)", realtimeTableName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
    LOGGER.info(
        "Successfully updated the RealtimeToOfflineSegmentsTaskMetadata during preUploadSegments for table: {}, "
            + "attemptCount: {}",
        realtimeTableName, attemptCount);
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.emptyMap());
  }

  private RealtimeToOfflineSegmentsTaskMetadata getUpdatedTaskMetadata(SegmentUploadContext context,
      ZNRecord realtimeToOfflineSegmentsTaskZNRecord) {

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(realtimeToOfflineSegmentsTaskZNRecord);

    ExpectedSubtaskResult expectedSubtaskResult =
        getExpectedRealtimeToOfflineTaskResult(context);

    realtimeToOfflineSegmentsTaskMetadata.addExpectedSubTaskResult(
        expectedSubtaskResult);
    return realtimeToOfflineSegmentsTaskMetadata;
  }

  private ExpectedSubtaskResult getExpectedRealtimeToOfflineTaskResult(
      SegmentUploadContext context) {

    PinotTaskConfig pinotTaskConfig = context.getPinotTaskConfig();
    String taskId = pinotTaskConfig.getTaskId();

    List<String> segmentsFrom =
        Arrays.stream(StringUtils.split(context.getInputSegmentNames(), MinionConstants.SEGMENT_NAME_SEPARATOR))
            .map(String::trim).collect(Collectors.toList());

    List<String> segmentsTo =
        context.getSegmentConversionResults().stream().map(SegmentConversionResult::getSegmentName)
            .collect(Collectors.toList());

    return new ExpectedSubtaskResult(segmentsFrom, segmentsTo, taskId);
  }
}
