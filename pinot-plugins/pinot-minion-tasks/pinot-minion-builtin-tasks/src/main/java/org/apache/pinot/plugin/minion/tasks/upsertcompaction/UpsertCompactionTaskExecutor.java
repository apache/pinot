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
import org.apache.pinot.common.restlet.resources.ValidDocIdsBitmapResponse;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.BaseSingleSegmentConversionExecutor;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpsertCompactionTaskExecutor extends BaseSingleSegmentConversionExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskExecutor.class);
  private static final String DEFAULT_VALID_DOC_ID_TYPE = "validDocIdsSnapshot";

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

    String validDocIdType =
        configs.getOrDefault(MinionConstants.UpsertCompactionTask.VALID_DOC_ID_TYPE, DEFAULT_VALID_DOC_ID_TYPE);
    ValidDocIdsBitmapResponse validDocIdsBitmapResponse =
        MinionTaskUtils.getValidDocIdsBitmap(tableNameWithType, segmentName, validDocIdType, MINION_CONTEXT);

    // Check crc from the downloaded segment against the crc returned from the server along with the valid doc id
    // bitmap. If this doesn't match, this means that we are hitting the race condition where the segment has been
    // uploaded successfully while the server is still reloading the segment. Reloading can take a while when the
    // offheap upsert is used because we will need to delete & add all primary keys.
    // `BaseSingleSegmentConversionExecutor.executeTask()` already checks for the crc from the task generator
    // against the crc from the current segment zk metadata, so we don't need to check that here.
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    String originalSegmentCrcFromTaskGenerator = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);
    String crcFromDeepStorageSegment = segmentMetadata.getCrc();
    String crcFromValidDocIdsBitmap = validDocIdsBitmapResponse.getSegmentCrc();
    if (!originalSegmentCrcFromTaskGenerator.equals(crcFromDeepStorageSegment)
        || !originalSegmentCrcFromTaskGenerator.equals(crcFromValidDocIdsBitmap)) {
      LOGGER.warn("CRC mismatch for segment: {}, expected: {}, actual crc from server: {}", segmentName,
          crcFromDeepStorageSegment, validDocIdsBitmapResponse.getSegmentCrc());
      return new SegmentConversionResult.Builder().setTableNameWithType(tableNameWithType).setSegmentName(segmentName)
          .build();
    }

    RoaringBitmap validDocIds = RoaringBitmapUtils.deserialize(validDocIdsBitmapResponse.getBitmap());

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

    try (CompactedPinotSegmentRecordReader compactedRecordReader = new CompactedPinotSegmentRecordReader(indexDir,
        validDocIds)) {
      SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata, segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, compactedRecordReader);
      driver.build();
    }

    File compactedSegmentFile = new File(workingDir, segmentName);
    SegmentConversionResult result =
        new SegmentConversionResult.Builder().setFile(compactedSegmentFile).setTableNameWithType(tableNameWithType)
            .setSegmentName(segmentName).build();

    long endMillis = System.currentTimeMillis();
    LOGGER.info("Finished task: {} with configs: {}. Total time: {}ms", taskType, configs, (endMillis - startMillis));

    return result;
  }

  private static SegmentGeneratorConfig getSegmentGeneratorConfig(File workingDir, TableConfig tableConfig,
      SegmentMetadataImpl segmentMetadata, String segmentName) {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, segmentMetadata.getSchema());
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
        Collections.singletonMap(MinionConstants.UpsertCompactionTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
