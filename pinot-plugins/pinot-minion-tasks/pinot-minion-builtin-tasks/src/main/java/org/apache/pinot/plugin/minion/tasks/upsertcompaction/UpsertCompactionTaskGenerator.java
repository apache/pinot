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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class UpsertCompactionTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskGenerator.class);
  private static final String DEFAULT_BUCKET_PERIOD = "1d";
  private static final String DEFAULT_BUFFER_PERIOD = "7d";
  private static final String DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT = "5000000";
  private static final String DEFAULT_INVALID_RECORDS_THRESHOLD = "100000";
  @Override
  public String getTaskType() {
    return MinionConstants.UpsertCompactionTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MinionConstants.UpsertCompactionTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (TableConfig tableConfig: tableConfigs) {
      if (!validate(tableConfig)) {
        continue;
      }

      String tableNameWithType = tableConfig.getTableName();
      LOGGER.info("Start generating task configs for table: {} for task: {}",
          tableNameWithType, taskType);

      Map<String, String> taskConfigs = tableConfig.getTaskConfig().getConfigsForTaskType(taskType);
      Map<String, String> compactionConfigs = getCompactionConfigs(taskConfigs);
      String bufferPeriod = compactionConfigs.getOrDefault(
          UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
      int invalidRecordsThreshold = Integer.parseInt(compactionConfigs.getOrDefault(
          UpsertCompactionTask.INVALID_RECORDS_THRESHOLD, DEFAULT_INVALID_RECORDS_THRESHOLD));
      long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
      List<SegmentZKMetadata> completedSegments = new ArrayList<>();
      List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
      for (SegmentZKMetadata segment : allSegments) {
        CommonConstants.Segment.Realtime.Status status = segment.getStatus();
        // initial segments selection based on status and age
        if (status.isCompleted() && segment.getEndTimeMs() <= (System.currentTimeMillis() - bufferMs)) {
          completedSegments.add(segment);
        }
      }
      if (completedSegments.isEmpty()) {
        LOGGER.info("No segments were selected for compaction for table: {}", tableNameWithType);
        continue;
      }

      // map each completedSegment to its partition
      Map<Integer, List<String>> partitionToSegmentNames = new HashMap<>();
      for (SegmentZKMetadata completedSegment : completedSegments) {
        String segmentName = completedSegment.getSegmentName();
        Integer partitionId = SegmentUtils.getRealtimeSegmentPartitionId(
            segmentName, tableNameWithType, completedSegment);

        Preconditions.checkState(partitionId != null,
            "Unable to find partitionId for completedSegment");

        partitionToSegmentNames.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(segmentName);
      }

      // only compact segments that exceed the invalidRecordThreshold
      Map<String, List<String>> serverToSegments = _clusterInfoAccessor.getServerToSegmentsMap(tableNameWithType);
      Map<String, String> segmentToServer = getSegmentToServer(serverToSegments);
      PinotHelixResourceManager pinotHelixResourceManager = _clusterInfoAccessor.getPinotHelixResourceManager();
      BiMap<String, String> serverToEndpoints;
      try {
        serverToEndpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
      } catch (InvalidConfigException e) {
        throw new RuntimeException(e);
      }
      Map<Integer, List<String>> selectedPartitions = new HashMap<>();
      for (Map.Entry<Integer, List<String>> partitionEntry : partitionToSegmentNames.entrySet()) {
        List<String> segments = partitionEntry.getValue();
        List<String> urls = new ArrayList<>(completedSegments.size());
        for (String segment : segments) {
          String server = segmentToServer.get(segment);
          String endpoint = serverToEndpoints.get(server);
          String url = String.format("%s/tables/%s/segments/%s/invalidRecordCount",
            endpoint, tableNameWithType, segment);
          urls.add(url);
        }
        // request the urls from the server
        CompletionServiceHelper completionServiceHelper = new CompletionServiceHelper(
              Executors.newCachedThreadPool(), new MultiThreadedHttpConnectionManager(), serverToEndpoints.inverse());
        CompletionServiceHelper.CompletionServiceResponse serviceResponse =
              completionServiceHelper.doMultiGetRequest(urls, tableNameWithType, true, 3000);
        Map<String, String> segmentToInvalidRecordCount = new HashMap<>();
        for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
          String invalidRecordCount = streamResponse.getValue();
          segmentToInvalidRecordCount.put(streamResponse.getKey(), invalidRecordCount);
        }
        for (Map.Entry<String, String> countEntry : segmentToInvalidRecordCount.entrySet()) {
          int invalidRecordCount = Integer.parseInt(countEntry.getValue());
          if (invalidRecordCount > invalidRecordsThreshold) {
            selectedPartitions.put(partitionEntry.getKey(), segments);
            break;
          }
        }
      }

      int numTaskConfigsForTable = 0;
      for (Map.Entry<Integer, List<String>> entry : selectedPartitions.entrySet()) {
        List<String> completedSegmentNames = entry.getValue();
        PinotTaskConfig pinotTaskConfig =
            getPinotTaskConfig(tableNameWithType, compactionConfigs, completedSegmentNames);
        pinotTaskConfigs.add(pinotTaskConfig);
        numTaskConfigsForTable++;
      }
      LOGGER.info("Finished generating {} tasks configs for table: {} " + "for task: {}",
          numTaskConfigsForTable, tableNameWithType, taskType);
    }
    return pinotTaskConfigs;
  }

  private Map<String, String> getSegmentToServer(Map<String, List<String>> serverToSegments) {
    Map<String, String> segmentToServer = new HashMap<>();
    for (String server : serverToSegments.keySet()) {
      List<String> segments = serverToSegments.get(server);
      for (String segment : segments) {
        if (!segmentToServer.containsKey(segment)) {
          segmentToServer.put(segment, server);
        }
      }
    }
    return segmentToServer;
  }

  private static PinotTaskConfig getPinotTaskConfig(String tableNameWithType, Map<String, String> compactionConfigs,
      List<String> completedSegmentNames) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, tableNameWithType);
    configs.put(MinionConstants.SEGMENT_NAME_KEY,
        StringUtils.join(completedSegmentNames, MinionConstants.SEGMENT_NAME_SEPARATOR));
    configs.put(UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, compactionConfigs.getOrDefault(
            UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD));
    configs.put(UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, compactionConfigs.getOrDefault(
        UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT));
    configs.put(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD, compactionConfigs.getOrDefault(
        UpsertCompactionTask.INVALID_RECORDS_THRESHOLD, DEFAULT_INVALID_RECORDS_THRESHOLD));
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(UpsertCompactionTask.TASK_TYPE, configs);
    return pinotTaskConfig;
  }

  private static final String[] VALID_CONFIG_KEYS = {
      UpsertCompactionTask.BUCKET_TIME_PERIOD_KEY,
      UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY,
      UpsertCompactionTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
      UpsertCompactionTask.INVALID_RECORDS_THRESHOLD,
  };

  private Map<String, String> getCompactionConfigs(Map<String, String> taskConfig) {
    Map<String, String> compactionConfigs = new HashMap<>();

    for (Map.Entry<String, String> entry : taskConfig.entrySet()) {
      String key = entry.getKey();
      for (String configKey : VALID_CONFIG_KEYS) {
        if (key.endsWith(configKey)) {
          compactionConfigs.put(configKey, entry.getValue());
        }
      }
    }

    return compactionConfigs;
  }

  @VisibleForTesting
  static boolean validate(TableConfig tableConfig) {
    String taskType = MinionConstants.UpsertCompactionTask.TASK_TYPE;
    String tableNameWithType = tableConfig.getTableName();
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      String message = "Skip generation task: {} for table: {}, offline table is not supported";
      LOGGER.warn(message, taskType, tableNameWithType);
      return false;
    }
    if (!tableConfig.isUpsertEnabled()) {
      String message = "Skip generation task: {} for table: {}, table without upsert enabled is not supported";
      LOGGER.warn(message, taskType, tableNameWithType);
      return false;
    }
//    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
//    if (indexingConfig == null || indexingConfig.getSegmentPartitionConfig() == null) {
//      LOGGER.warn("Skip generation task: {} for table: {}, unable to find segment partition config",
//          taskType, tableNameWithType);
//      return false;
//    }
    return true;
  }
}
