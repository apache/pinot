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
package org.apache.pinot.plugin.minion.tasks.upsertcompactmerge;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.plugin.minion.tasks.BaseMultipleSegmentsConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.name.UploadedRealtimeSegmentNameGenerator;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Minion task that compacts and merges multiple segments of an upsert table and uploads it back as one single
 * segment. This helps in keeping the segment count in check and also prevents a lot of small segments created over
 * time.
 */
public class UpsertCompactMergeTaskExecutor extends BaseMultipleSegmentsConversionExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactMergeTaskExecutor.class);

  public UpsertCompactMergeTaskExecutor(MinionConf minionConf) {
    super(minionConf);
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

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    TableConfig tableConfig = getTableConfig(tableNameWithType);
    Schema schema = getSchema(tableNameWithType);

    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema);

    // Progress observer
    segmentProcessorConfigBuilder.setProgressObserver(p -> _eventObserver.notifyProgress(_pinotTaskConfig, p));

    List<RecordReader> recordReaders = new ArrayList<>(numInputSegments);
    int partitionId = -1;
    long maxCreationTimeOfMergingSegments = 0;
    List<String> originalSegmentCrcFromTaskGenerator =
        List.of(configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY).split(","));
    for (int i = 0; i < numInputSegments; i++) {
      File segmentDir = segmentDirs.get(i);
      _eventObserver.notifyProgress(_pinotTaskConfig,
          String.format("Creating RecordReader for: %s (%d out of %d)", segmentDir, (i + 1), numInputSegments));

      SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(segmentDir);
      String segmentName = segmentMetadata.getName();
      Integer segmentPartitionId = SegmentUtils.getPartitionIdFromRealtimeSegmentName(segmentName);
      if (segmentPartitionId == null) {
        throw new IllegalStateException(String.format("Partition id not found for %s", segmentName));
      }
      if (partitionId != -1 && partitionId != segmentPartitionId) {
        throw new IllegalStateException(String.format("Partition id mismatched for %s, expected partition id: %d",
            segmentName, partitionId));
      }
      partitionId = segmentPartitionId;
      maxCreationTimeOfMergingSegments = Math.max(maxCreationTimeOfMergingSegments,
          segmentMetadata.getIndexCreationTime());

      String crcFromDeepStorageSegment = segmentMetadata.getCrc();
      if (!originalSegmentCrcFromTaskGenerator.get(i).equals(crcFromDeepStorageSegment)) {
        String message = String.format("Crc mismatched between ZK and deepstore copy of segment: %s. Expected crc "
                + "from ZK: %s, crc from deepstore: %s", segmentName, originalSegmentCrcFromTaskGenerator.get(i),
            crcFromDeepStorageSegment);
        LOGGER.error(message);
        throw new IllegalStateException(message);
      }
      RoaringBitmap validDocIds = MinionTaskUtils.getValidDocIdFromServerMatchingCrc(tableNameWithType, segmentName,
              ValidDocIdsType.SNAPSHOT.name(), MINION_CONTEXT, crcFromDeepStorageSegment);
      if (validDocIds == null) {
        // no valid crc match found or no validDocIds obtained from all servers
        // error out the task instead of silently failing so that we can track it via task-error metrics
        String message = String.format("No validDocIds found from all servers. They either failed to download "
                + "or did not match crc from segment copy obtained from deepstore / servers. " + "Expected crc: %s",
            "");
        LOGGER.error(message);
        throw new IllegalStateException(message);
      }

      recordReaders.add(new CompactedPinotSegmentRecordReader(segmentDir, validDocIds));
    }

    segmentProcessorConfigBuilder.setSegmentNameGenerator(
        new UploadedRealtimeSegmentNameGenerator(TableNameBuilder.extractRawTableName(tableNameWithType), partitionId,
            System.currentTimeMillis(), MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENT_NAME_PREFIX, null));
    if (maxCreationTimeOfMergingSegments != 0) {
      segmentProcessorConfigBuilder.setCustomCreationTime(maxCreationTimeOfMergingSegments);
    } else {
      String message = "No valid creation time found for the new merged segment. This might be due to "
          + "missing creation time for merging segments";
      LOGGER.error(message);
      throw new IllegalStateException(message);
    }
    SegmentProcessorConfig segmentProcessorConfig = segmentProcessorConfigBuilder.build();
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
          .setTableNameWithType(tableNameWithType).build());
    }
    return results;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    Map<String, String> updateMap = new TreeMap<>();
    updateMap.put(MinionConstants.UpsertCompactMergeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
        String.valueOf(System.currentTimeMillis()));
    updateMap.put(MinionConstants.UpsertCompactMergeTask.TASK_TYPE
        + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX,
        pinotTaskConfig.getConfigs().get(MinionConstants.SEGMENT_NAME_KEY));
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, updateMap);
  }
}
