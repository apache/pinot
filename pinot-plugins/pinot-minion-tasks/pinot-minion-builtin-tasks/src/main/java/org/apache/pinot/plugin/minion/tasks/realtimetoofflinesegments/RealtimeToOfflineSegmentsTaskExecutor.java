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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
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
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.plugin.minion.tasks.mergerollup.MergeRollupTaskUtils;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;
import org.apache.pinot.spi.utils.Obfuscator;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A task to convert segments from a REALTIME table to segments for its corresponding OFFLINE table.
 * The realtime segments could span across multiple time windows.
 * This task extracts data and creates segments for a configured time window.
 *
 * Supports both regular tables and upsert tables. For upsert tables, automatically filters out
 * invalid records using validDocIds from servers.
 *
 * The {@link SegmentProcessorFramework} is used for the segment conversion, which does the following steps
 * 1. Filter records based on the time window (and valid doc IDs for upsert tables)
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
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Starting task: {} with configs: {}", taskType, Obfuscator.DEFAULT.toJsonString(configs));
    }
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
        MergeRollupTaskUtils.getAggregationFunctionParameters(configs));

    // Segment config
    segmentProcessorConfigBuilder.setSegmentConfig(MergeTaskUtils.getSegmentConfig(configs));

    // Progress observer
    segmentProcessorConfigBuilder.setProgressObserver(p -> _eventObserver.notifyProgress(_pinotTaskConfig, p));

    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    // Check if this is an upsert table from the pre-computed config value
    boolean isUpsertTable =
        Boolean.parseBoolean(configs.getOrDefault(RealtimeToOfflineSegmentsTask.IS_UPSERT_TABLE, "false"));

    // Create record readers - use CompactedPinotSegmentRecordReader for upsert tables
    List<RecordReaderFileConfig> recordReaderFileConfigs = new ArrayList<>(numInputSegments);
    int count = 1;

    if (isUpsertTable) {
      LOGGER.info("Processing upsert table: {}, using CompactedPinotSegmentRecordReader for {} segments",
          realtimeTableName, numInputSegments);
      _eventObserver.notifyProgress(pinotTaskConfig, "Creating upsert-aware record readers");

      // Get segment metadata for CRC validation
      List<SegmentMetadataImpl> segmentMetadataList = segmentDirs.stream().map(x -> {
        try {
          return new SegmentMetadataImpl(x);
        } catch (Exception e) {
          throw new RuntimeException(String.format("Error fetching segment-metadata for segmentDir: %s", x), e);
        }
      }).collect(Collectors.toList());

      // Validate if CRC of deepstore copies is same as that in ZK of segments
      List<String> originalSegmentCrcFromTaskGenerator =
          List.of(configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY).split(","));
      validateCRCForInputSegments(segmentMetadataList, originalSegmentCrcFromTaskGenerator);

      for (int i = 0; i < segmentDirs.size(); i++) {
        File segmentDir = segmentDirs.get(i);
        SegmentMetadataImpl segmentMetadata = segmentMetadataList.get(i);
        _eventObserver.notifyProgress(_pinotTaskConfig,
            String.format("Creating CompactedRecordReader for: %s (%d out of %d)", segmentDir, count++,
                numInputSegments));

        // Fetch validDocID snapshot from server and get record-reader for compacted reader (hardcode SNAPSHOT like
        // UpsertCompactMergeTask)
        RoaringBitmap validDocIds = MinionTaskUtils.getValidDocIdFromServerMatchingCrc(
            realtimeTableName, segmentMetadata.getName(), ValidDocIdsType.SNAPSHOT.name(), MINION_CONTEXT,
            segmentMetadata.getCrc());

        if (validDocIds == null) {
          // No valid crc match found or no validDocIds obtained from all servers
          // Error out the task instead of silently failing so that we can track it via task-error metrics
          String message = String.format("No validDocIds found from all servers. They either failed to download "
                  + "or did not match crc from segment copy obtained from deepstore / servers. " + "Expected crc: %s",
              segmentMetadata.getCrc());
          LOGGER.error(message);
          throw new IllegalStateException(message);
        }

        LOGGER.info("Fetched {} valid doc IDs for segment: {}", validDocIds.getCardinality(),
            segmentMetadata.getName());

        // Create CompactedPinotSegmentRecordReader
        CompactedPinotSegmentRecordReader recordReader = new CompactedPinotSegmentRecordReader(validDocIds);
        RecordReaderFileConfig recordReaderFileConfig =
            new RecordReaderFileConfig(null, segmentDir, null, null, recordReader);
        recordReaderFileConfigs.add(recordReaderFileConfig);
      }
    } else {
      // Standard record readers for non-upsert tables
      LOGGER.info("Processing non-upsert table: {}, using standard PinotSegmentRecordReader", realtimeTableName);
      for (File segmentDir : segmentDirs) {
        _eventObserver.notifyProgress(_pinotTaskConfig,
            String.format("Creating RecordReader for: %s (%d out of %d)", segmentDir, count++, numInputSegments));

        PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
        RecordReaderFileConfig recordReaderFileConfig =
            new RecordReaderFileConfig(null, segmentDir, null, null, recordReader);
        recordReaderFileConfigs.add(recordReaderFileConfig);
      }
    }

    // Use SegmentProcessorFramework with RecordReaderFileConfig
    List<File> outputSegmentDirs;
    _eventObserver.notifyProgress(_pinotTaskConfig, "Generating segments");
    SegmentProcessorFramework framework = new SegmentProcessorFramework(
        segmentProcessorConfig, workingDir, recordReaderFileConfigs, Collections.emptyList(), null);
    outputSegmentDirs = framework.process();
    _eventObserver.notifyProgress(pinotTaskConfig,
        "Segment processing stats - incomplete rows:" + framework.getIncompleteRowsFound() + ", dropped rows:"
            + framework.getSkippedRowsFound() + ", sanitized rows:" + framework.getSanitizedRowsFound());

    long endMillis = System.currentTimeMillis();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType,
          Obfuscator.DEFAULT.toJsonString(configs), (endMillis - startMillis));
    }
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

  /**
   * Validates that the CRC values from segment metadata match the expected CRC values.
   * Copied from UpsertCompactMergeTaskExecutor for consistency.
   */
  void validateCRCForInputSegments(List<SegmentMetadataImpl> segmentMetadataList,
      List<String> expectedCRCList) {
    for (int i = 0; i < segmentMetadataList.size(); i++) {
      SegmentMetadataImpl segmentMetadata = segmentMetadataList.get(i);
      if (!Objects.equals(segmentMetadata.getCrc(), expectedCRCList.get(i))) {
        String message = String.format("Crc mismatched between ZK and deepstore copy of segment: %s. Expected crc "
                + "from ZK: %s, crc from deepstore: %s", segmentMetadata.getName(), expectedCRCList.get(i),
            segmentMetadata.getCrc());
        LOGGER.error(message);
        throw new IllegalStateException(message);
      }
    }
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.emptyMap());
  }
}
