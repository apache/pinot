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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.http.client.utils.URIBuilder;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
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
  private static final double DEFAULT_INVALID_RECORDS_THRESHOLD_PERCENT = 0.0;
  private static final long DEFAULT_INVALID_RECORDS_THRESHOLD_COUNT = 0;

  public static class SegmentSelectionResult {

    private List<SegmentZKMetadata> _segmentsForCompaction;

    private List<String> _segmentsForDeletion;

    SegmentSelectionResult(List<SegmentZKMetadata> segmentsForCompaction, List<String> segmentsForDeletion) {
      _segmentsForCompaction = segmentsForCompaction;
      _segmentsForDeletion = segmentsForDeletion;
    }

    public List<SegmentZKMetadata> getSegmentsForCompaction() {
      return _segmentsForCompaction;
    }

    public List<String> getSegmentsForDeletion() {
      return _segmentsForDeletion;
    }
  }

  @Override
  public String getTaskType() {
    return MinionConstants.UpsertCompactionTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MinionConstants.UpsertCompactionTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();
    for (TableConfig tableConfig : tableConfigs) {
      if (!validate(tableConfig)) {
        continue;
      }

      String tableNameWithType = tableConfig.getTableName();
      LOGGER.info("Start generating task configs for table: {} for task: {}", tableNameWithType, taskType);

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

      Map<String, SegmentZKMetadata> completedSegmentsMap =
          completedSegments.stream().collect(Collectors.toMap(SegmentZKMetadata::getSegmentName, Function.identity()));

      List<String> validDocIdUrls;
      try {
        validDocIdUrls = getValidDocIdMetadataUrls(serverToSegments, serverToEndpoints, tableNameWithType,
            completedSegmentsMap.keySet());
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }

      // request the urls from the servers
      CompletionServiceHelper completionServiceHelper =
          new CompletionServiceHelper(_clusterInfoAccessor.getExecutor(), _clusterInfoAccessor.getConnectionManager(),
              serverToEndpoints.inverse());

      CompletionServiceHelper.CompletionServiceResponse serviceResponse =
          completionServiceHelper.doMultiGetRequest(validDocIdUrls, tableNameWithType, false, 3000);

      SegmentSelectionResult segmentSelectionResult =
          processValidDocIdMetadata(compactionConfigs, completedSegmentsMap, serviceResponse._httpResponses.entrySet());

      if (!segmentSelectionResult.getSegmentsForDeletion().isEmpty()) {
        pinotHelixResourceManager.deleteSegments(tableNameWithType, segmentSelectionResult.getSegmentsForDeletion(),
            "0d");
        LOGGER.info("Deleted segments containing only invalid records for table: {} for task: {}", tableNameWithType,
            taskType);
      }

      int numTasks = 0;
      int maxTasks = getMaxTasks(taskType, tableNameWithType, taskConfigs);
      for (SegmentZKMetadata segment : segmentSelectionResult.getSegmentsForCompaction()) {
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
      LOGGER.info("Finished generating {} tasks configs for table: {} for task: {}", numTasks, tableNameWithType,
          taskType);
    }
    return pinotTaskConfigs;
  }

  @VisibleForTesting
  public static SegmentSelectionResult processValidDocIdMetadata(Map<String, String> compactionConfigs,
      Map<String, SegmentZKMetadata> completedSegmentsMap, Set<Map.Entry<String, String>> responseSet) {
    double invalidRecordsThresholdPercent = Double.parseDouble(
        compactionConfigs.getOrDefault(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT,
            String.valueOf(DEFAULT_INVALID_RECORDS_THRESHOLD_PERCENT)));
    long invalidRecordsThresholdCount = Long.parseLong(
        compactionConfigs.getOrDefault(UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT,
            String.valueOf(DEFAULT_INVALID_RECORDS_THRESHOLD_COUNT)));
    List<SegmentZKMetadata> segmentsForCompaction = new ArrayList<>();
    List<String> segmentsForDeletion = new ArrayList<>();
    for (Map.Entry<String, String> streamResponse : responseSet) {
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
        long invalidRecordCount = validDocIdMetadata.get("totalInvalidDocs").asLong();
        String segmentName = validDocIdMetadata.get("segmentName").asText();
        SegmentZKMetadata segment = completedSegmentsMap.get(segmentName);
        double invalidRecordPercent = ((double) invalidRecordCount / segment.getTotalDocs()) * 100;
        if (invalidRecordCount == segment.getTotalDocs()) {
          segmentsForDeletion.add(segment.getSegmentName());
        } else if (invalidRecordPercent > invalidRecordsThresholdPercent
            && invalidRecordCount > invalidRecordsThresholdCount) {
          segmentsForCompaction.add(segment);
        }
      }
    }
    return new SegmentSelectionResult(segmentsForCompaction, segmentsForDeletion);
  }

  @VisibleForTesting
  public static List<String> getValidDocIdMetadataUrls(Map<String, List<String>> serverToSegments,
      BiMap<String, String> serverToEndpoints, String tableNameWithType, Set<String> completedSegments)
      throws URISyntaxException {
    Set<String> remainingSegments = new HashSet<>(completedSegments);
    List<String> urls = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      if (remainingSegments.isEmpty()) {
        break;
      }
      String server = entry.getKey();
      List<String> segmentNames = entry.getValue();
      URIBuilder uriBuilder = new URIBuilder(serverToEndpoints.get(server)).setPath(
          String.format("/tables/%s/validDocIdMetadata", tableNameWithType));
      int completedSegmentCountPerServer = 0;
      for (String segmentName : segmentNames) {
        if (remainingSegments.contains(segmentName)) {
          completedSegmentCountPerServer++;
          uriBuilder.addParameter("segmentNames", segmentName);
          remainingSegments.remove(segmentName);
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
    String bufferPeriod =
        compactionConfigs.getOrDefault(UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
    long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
    List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
    for (SegmentZKMetadata segment : allSegments) {
      CommonConstants.Segment.Realtime.Status status = segment.getStatus();
      // initial segments selection based on status and age
      if (status.isCompleted()) {
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
      UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_PERCENT,
      UpsertCompactionTask.INVALID_RECORDS_THRESHOLD_COUNT
  };

  private Map<String, String> getCompactionConfigs(Map<String, String> taskConfig) {
    Map<String, String> compactionConfigs = new HashMap<>();

    for (String configKey : VALID_CONFIG_KEYS) {
      if (taskConfig.containsKey(configKey)) {
        compactionConfigs.put(configKey, taskConfig.get(configKey));
      }
    }

    return compactionConfigs;
  }

  @VisibleForTesting
  static boolean validate(TableConfig tableConfig) {
    String taskType = MinionConstants.UpsertCompactionTask.TASK_TYPE;
    String tableNameWithType = tableConfig.getTableName();
    if (tableConfig.getTableType() == TableType.OFFLINE) {
      LOGGER.warn("Skip generation task: {} for table: {}, offline table is not supported", taskType,
          tableNameWithType);
      return false;
    }
    if (!tableConfig.isUpsertEnabled()) {
      LOGGER.warn("Skip generation task: {} for table: {}, table without upsert enabled is not supported", taskType,
          tableNameWithType);
      return false;
    }
    return true;
  }
}
