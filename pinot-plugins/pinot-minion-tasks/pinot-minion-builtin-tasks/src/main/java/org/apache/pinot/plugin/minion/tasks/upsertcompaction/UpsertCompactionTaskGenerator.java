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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.http.client.utils.URIBuilder;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class UpsertCompactionTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskGenerator.class);
  private static final String DEFAULT_BUFFER_PERIOD = "7d";
  private static final double DEFAULT_INVALID_RECORDS_THRESHOLD_PERCENT = 30.0;
  private static final long DEFAULT_MIN_RECORD_COUNT = 100_000;
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
      List<SegmentZKMetadata> completedSegments = getCompletedSegments(tableNameWithType, compactionConfigs);

      if (completedSegments.isEmpty()) {
        LOGGER.info("No completed segments were eligible for compaction for table: {}", tableNameWithType);
        continue;
      }

      // get server to segment mappings
      Map<String, List<String>> serverToSegments = _clusterInfoAccessor.getServerToSegmentsMap(tableNameWithType);
      PinotHelixResourceManager pinotHelixResourceManager = _clusterInfoAccessor.getPinotHelixResourceManager();
      BiMap<String, String> serverToEndpoints;
      try {
        serverToEndpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
      } catch (InvalidConfigException e) {
        throw new RuntimeException(e);
      }

      Map<String, SegmentZKMetadata> completedSegmentsMap = completedSegments.stream()
          .collect(Collectors.toMap(SegmentZKMetadata::getSegmentName, Function.identity()));

      List<String> validDocIdUrls;
      try {
        validDocIdUrls = getValidDocIdMetadataUrls(
            serverToSegments, serverToEndpoints, tableNameWithType, completedSegmentsMap);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }

      // request the urls from the servers
      CompletionServiceHelper completionServiceHelper = new CompletionServiceHelper(
            Executors.newCachedThreadPool(), new MultiThreadedHttpConnectionManager(), serverToEndpoints.inverse());
      CompletionServiceHelper.CompletionServiceResponse serviceResponse =
            completionServiceHelper.doMultiGetRequest(validDocIdUrls, tableNameWithType, false, 3000);

      // only compact segments that exceed the threshold
      double invalidRecordsThresholdPercent =
          Double.parseDouble(compactionConfigs.getOrDefault(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT,
              String.valueOf(DEFAULT_INVALID_RECORDS_THRESHOLD_PERCENT)));
      List<SegmentZKMetadata> segmentsForCompaction = new ArrayList<>();
      List<String> segmentsForDeletion = new ArrayList<>();
      for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
        JsonNode allValidDocIdMetadata;
        try {
          allValidDocIdMetadata = JsonUtils.stringToJsonNode(streamResponse.getValue());
        } catch (IOException e) {
          LOGGER.error("Unable to parse validDocIdMetadata response for: {}", streamResponse.getKey());
          continue;
        }
        Iterator<JsonNode> iterator = allValidDocIdMetadata.elements();
        while (iterator.hasNext()) {
          JsonNode validDocIdMetadata = iterator.next();
          double invalidRecordCount = validDocIdMetadata.get("totalInvalidDocs").asDouble();
          String segmentName = validDocIdMetadata.get("segmentName").asText();
          SegmentZKMetadata segment = completedSegmentsMap.get(segmentName);
          double invalidRecordPercent = (invalidRecordCount / segment.getTotalDocs()) * 100;
          if (invalidRecordPercent == 100.0) {
            segmentsForDeletion.add(segment.getSegmentName());
          } else if (invalidRecordPercent > invalidRecordsThresholdPercent) {
            segmentsForCompaction.add(segment);
          }
        }
      }

      if (!segmentsForDeletion.isEmpty()) {
          pinotHelixResourceManager.deleteSegments(tableNameWithType, segmentsForDeletion, "0d");
      }

      int numTasks = 0;
      int maxTasks = getMaxTasks(taskType, tableNameWithType, taskConfigs);
      for (SegmentZKMetadata segment : segmentsForCompaction) {
        if (numTasks == maxTasks) {
          break;
        }
        Map<String, String> configs = new HashMap<>();
        configs.put(MinionConstants.TABLE_NAME_KEY, tableNameWithType);
        configs.put(MinionConstants.SEGMENT_NAME_KEY, segment.getSegmentName());
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, segment.getDownloadUrl());
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
        configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, String.valueOf(segment.getCrc()));
        pinotTaskConfigs.add(new PinotTaskConfig(UpsertCompactionTask.TASK_TYPE, configs));
        numTasks++;
      }
      LOGGER.info("Finished generating {} tasks configs for table: {} " + "for task: {}",
          numTasks, tableNameWithType, taskType);
    }
    return pinotTaskConfigs;
  }

  @VisibleForTesting
  public static List<String> getValidDocIdMetadataUrls(Map<String, List<String>> serverToSegments,
      BiMap<String, String> serverToEndpoints, String tableNameWithType,
      Map<String, SegmentZKMetadata> completedSegments) throws URISyntaxException {
    List<String> urls = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      String server = entry.getKey();
      List<String> segmentNames = entry.getValue();
      URIBuilder uriBuilder = new URIBuilder(serverToEndpoints.get(server)).setPath(
          String.format("/tables/%s/validDocIdMetadata", tableNameWithType));
      int completedSegmentCountPerServer = 0;
      for (String segmentName : segmentNames) {
        if (completedSegments.containsKey(segmentName)) {
          completedSegmentCountPerServer++;
          uriBuilder.addParameter("segmentNames", segmentName);
        }
      }
      if (completedSegmentCountPerServer > 0) {
        // only add to the list if the server has completed segments
        urls.add(uriBuilder.toString());
      }
    }
    return urls;
  }

  private List<SegmentZKMetadata> getCompletedSegments(String tableNameWithType,
      Map<String, String> compactionConfigs) {
    List<SegmentZKMetadata> completedSegments = new ArrayList<>();
    String bufferPeriod = compactionConfigs.getOrDefault(
        UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
    long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
    long minRecordCount =
        Long.parseLong(compactionConfigs.getOrDefault(UpsertCompactionTask.MIN_RECORD_COUNT,
            String.valueOf(DEFAULT_MIN_RECORD_COUNT)));
    List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
    for (SegmentZKMetadata segment : allSegments) {
      CommonConstants.Segment.Realtime.Status status = segment.getStatus();
      // initial segments selection based on status, record count, and age
      if (status.isCompleted() && segment.getTotalDocs() >= minRecordCount) {
        boolean endedWithinBufferPeriod = segment.getEndTimeMs() <= (System.currentTimeMillis() - bufferMs);
        boolean endsInTheFuture = segment.getEndTimeMs() > System.currentTimeMillis();
        if (endedWithinBufferPeriod || endsInTheFuture) {
          completedSegments.add(segment);
        }
      }
    }
    return completedSegments;
  }

  @VisibleForTesting
  public static int getMaxTasks(String taskType, String tableNameWithType, Map<String, String> taskConfigs) {
    int maxTasks = Integer.MAX_VALUE;
    String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
    if (tableMaxNumTasksConfig != null) {
      try {
        maxTasks = Integer.parseInt(tableMaxNumTasksConfig);
      } catch (Exception e) {
        LOGGER.warn("MaxNumTasks have been wrongly set for table : {}, and task {}", tableNameWithType, taskType);
      }
    }
    return maxTasks;
  }

  private static final String[] VALID_CONFIG_KEYS = {
      UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY,
      UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT,
      UpsertCompactionTask.MIN_RECORD_COUNT
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
    return true;
  }
}
