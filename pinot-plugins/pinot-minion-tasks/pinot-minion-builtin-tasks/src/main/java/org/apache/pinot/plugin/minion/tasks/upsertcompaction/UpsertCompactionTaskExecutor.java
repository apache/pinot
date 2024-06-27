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
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.restlet.resources.ValidDocIdsBitmapResponse;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
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
        configs.getOrDefault(MinionConstants.UpsertCompactionTask.VALID_DOC_IDS_TYPE, ValidDocIdsType.SNAPSHOT.name());
    ValidDocIdsType validDocIdsType = ValidDocIdsType.valueOf(validDocIdsTypeStr.toUpperCase());
    String clusterName = MINION_CONTEXT.getHelixManager().getClusterName();
    HelixAdmin helixAdmin = MINION_CONTEXT.getHelixManager().getClusterManagmentTool();
    List<String> servers = MinionTaskUtils.getServers(segmentName, tableNameWithType, helixAdmin, clusterName);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    String originalSegmentCrcFromTaskGenerator = configs.get(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY);
    String crcFromDeepStorageSegment = segmentMetadata.getCrc();
    RoaringBitmap validDocIds = null;
    for (String server : servers) {
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, server);
      String endpoint = InstanceUtils.getServerAdminEndpoint(instanceConfig);

      // We only need aggregated table size and the total number of docs/rows. Skipping column related stats, by
      // passing an empty list.
      ServerSegmentMetadataReader serverSegmentMetadataReader = new ServerSegmentMetadataReader();
      ValidDocIdsBitmapResponse validDocIdsBitmapResponse;
      try {
        validDocIdsBitmapResponse = serverSegmentMetadataReader.getValidDocIdsBitmapFromServer(tableNameWithType,
            segmentName, endpoint, validDocIdsType.toString(), 60_000);
      } catch (Exception e) {
        LOGGER.warn(
            String.format("Unable to retrieve validDocIds bitmap for segment: %s from endpoint: %s", segmentName,
                endpoint), e);
        continue;
      }

      // Check crc from the downloaded segment against the crc returned from the server along with the valid doc id
      // bitmap. If this doesn't match, this means that we are hitting the race condition where the segment has been
      // uploaded successfully while the server is still reloading the segment. Reloading can take a while when the
      // offheap upsert is used because we will need to delete & add all primary keys.
      // `BaseSingleSegmentConversionExecutor.executeTask()` already checks for the crc from the task generator
      // against the crc from the current segment zk metadata, so we don't need to check that here.
      String crcFromValidDocIdsBitmap = validDocIdsBitmapResponse.getSegmentCrc();
      if (!originalSegmentCrcFromTaskGenerator.equals(crcFromValidDocIdsBitmap)) {
        // In this scenario, we are hitting the other replica of the segment which did not commit to ZK or deepstore.
        // We will skip processing this bitmap to query other server to confirm if there is a valid matching CRC.
        String message = String.format("CRC mismatch for segment: %s, expected value based on task generator: %s, "
                + "actual crc from validDocIdsBitmapResponse from endpoint %s: %s", segmentName,
            originalSegmentCrcFromTaskGenerator, endpoint, crcFromValidDocIdsBitmap);
        LOGGER.warn(message);
        continue;
      }
      if (!crcFromValidDocIdsBitmap.equals(crcFromDeepStorageSegment)) {
        // Deepstore copies might not always have the same CRC as that from the server we queried for ValidDocIdsBitmap
        // It can happen due to CRC mismatch issues due to replicas diverging, lucene index issues.
        String message = String.format("CRC mismatch for segment: %s, "
                + "expected crc from validDocIdsBitmapResponse from endpoint %s: %s, "
                + "actual crc based on deepstore / server metadata copy: %s", segmentName, endpoint,
            crcFromValidDocIdsBitmap, crcFromDeepStorageSegment);
        LOGGER.warn(message);
        continue;
      }
      validDocIds = RoaringBitmapUtils.deserialize(validDocIdsBitmapResponse.getBitmap());
    }
    if (validDocIds == null) {
      // no valid crc match found or no validDocIds obtained from all servers
      // error out the task instead of silently failing so that we can track it via task-error metrics
      LOGGER.error("No validDocIds found from all servers. They either failed to download or did not match crc from"
          + "segment copy obtained from deepstore / servers.");
      throw new IllegalStateException("No valid validDocIds found from all servers.");
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

    try (CompactedPinotSegmentRecordReader compactedRecordReader = new CompactedPinotSegmentRecordReader(indexDir,
        validDocIds)) {
      SegmentGeneratorConfig config = getSegmentGeneratorConfig(workingDir, tableConfig, segmentMetadata, segmentName,
          getSchema(tableNameWithType));
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
        Collections.singletonMap(MinionConstants.UpsertCompactionTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX,
            String.valueOf(System.currentTimeMillis())));
  }
}
