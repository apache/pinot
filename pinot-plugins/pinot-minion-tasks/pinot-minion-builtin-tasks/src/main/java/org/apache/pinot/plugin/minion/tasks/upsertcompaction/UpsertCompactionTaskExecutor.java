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
package org.apache.pinot.plugin.minion.tasks.upsertcompaction;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpsertCompactionTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskExecutor.class);

  @Override
  protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
      throws Exception {
    _eventObserver.notifyProgress(pinotTaskConfig, "Compacting segment: " + indexDir);
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String segmentName = configs.get(MinionConstants.SEGMENT_NAME_KEY);
    String taskType = pinotTaskConfig.getTaskType();
    LOGGER.info("Starting task: {} with configs: {}", taskType, configs);
    long startMillis = System.currentTimeMillis();

    String tableNameWithType = configs.get(MinionConstants.TABLE_NAME_KEY);
    TableConfig tableConfig = getTableConfig(tableNameWithType);

    String validDocIdsTypeStr =
        configs.getOrDefault(UpsertCompactionTask.VALID_DOC_IDS_TYPE, ValidDocIdsType.SNAPSHOT.name());
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    String originalSegmentCrcFromTaskGenerator = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);
    String crcFromDeepStorageSegment = segmentMetadata.getCrc();
    boolean ignoreCrcMismatch = Boolean.parseBoolean(configs.getOrDefault(UpsertCompactionTask.IGNORE_CRC_MISMATCH_KEY,
        String.valueOf(UpsertCompactionTask.DEFAULT_IGNORE_CRC_MISMATCH)));
    if (!ignoreCrcMismatch && !originalSegmentCrcFromTaskGenerator.equals(crcFromDeepStorageSegment)) {
      String message = String.format("Crc mismatched between ZK and deepstore copy of segment: %s. Expected crc "
              + "from ZK: %s, crc from deepstore: %s", segmentName, originalSegmentCrcFromTaskGenerator,
          crcFromDeepStorageSegment);
      LOGGER.error(message);
      throw new IllegalStateException(message);
    }
    RoaringBitmap validDocIds =
        MinionTaskUtils.getValidDocIdFromServerMatchingCrc(tableNameWithType, segmentName, validDocIdsTypeStr,
            MINION_CONTEXT, originalSegmentCrcFromTaskGenerator);
    if (validDocIds == null) {
      // no valid crc match found or no validDocIds obtained from all servers
      // error out the task instead of silently failing so that we can track it via task-error metrics
      String message = String.format("No validDocIds found from all servers. They either failed to download "
              + "or did not match crc from segment copy obtained from deepstore / servers. " + "Expected crc: %s",
          originalSegmentCrcFromTaskGenerator);
      LOGGER.error(message);
      throw new IllegalStateException(message);
    }
    if (validDocIds.isEmpty()) {
      // prevents empty segment generation
      LOGGER.info("validDocIds is empty, skip the task. Table: {}, segment: {}", tableNameWithType, segmentName);
      if (indexDir.exists() && !FileUtils.deleteQuietly(indexDir)) {
        LOGGER.warn("Failed to delete input segment: {}", indexDir.getAbsolutePath());
      }
      if (!FileUtils.deleteQuietly(workingDir)) {
        LOGGER.warn("Failed to delete working directory: {}", workingDir.getAbsolutePath());
      }
      return new SegmentConversionResult.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
          .build();
    }

    int totalDocsAfterCompaction;
    try (CompactedPinotSegmentRecordReader compactedRecordReader = new CompactedPinotSegmentRecordReader(indexDir,
        validDocIds)) {
      SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata, segmentName,
          getSchema(tableNameWithType));
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, compactedRecordReader);
      driver.build();
      totalDocsAfterCompaction = driver.getSegmentStats().getTotalDocCount();
    }

    File compactedSegmentFile = new File(workingDir, segmentName);
    SegmentConversionResult result =
        new SegmentConversionResult.Builder().setFile(compactedSegmentFile).setTableNameWithType(tableNameWithType)
            .setSegmentName(segmentName).build();
    _minionMetrics.addMeteredTableValue(tableNameWithType, MinionMeter.COMPACTED_RECORDS_COUNT,
            segmentMetadata.getTotalDocs() - totalDocsAfterCompaction);

    long endMillis = System.currentTimeMillis();
    LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms. Total docs before compaction: {}. "
            + "Total docs after compaction: {}.", taskType, configs, (endMillis - startMillis),
            segmentMetadata.getTotalDocs(), totalDocsAfterCompaction);

    return result;
  }

  private static SegmentGeneratorConfig getSegmentGeneratorConfig(File workingDir, TableConfig tableConfig,
      SegmentMetadataImpl segmentMetadata, String segmentName, Schema schema) {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(workingDir.getPath());
    config.setSegmentName(segmentName);
    // Keep index creation time the same as original segment because both segments use the same raw data.
    // This way, for REFRESH case, when new segment gets pushed to controller, we can use index creation time to
    // identify if the new pushed segment has newer data than the existing one.
    config.setCreationTime(String.valueOf(segmentMetadata.getIndexCreationTime()));

    // The time column type info is not stored in the segment metadata.
    // Keep segment start/end time to properly handle time column type other than EPOCH (e.g.SIMPLE_FORMAT).
    if (segmentMetadata.getTimeInterval() != null) {
      config.setTimeColumnName(tableConfig.getValidationConfig().getTimeColumnName());
      config.setStartTime(Long.toString(segmentMetadata.getStartTime()));
      config.setEndTime(Long.toString(segmentMetadata.getEndTime()));
      config.setSegmentTimeUnit(segmentMetadata.getTimeUnit());
    }
    return config;
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
      SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
        Collections.singletonMap(UpsertCompactionTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
