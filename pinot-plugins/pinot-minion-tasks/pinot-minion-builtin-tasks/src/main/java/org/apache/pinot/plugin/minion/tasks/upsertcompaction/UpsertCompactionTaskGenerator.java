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
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
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
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@TaskGenerator
public class UpsertCompactionTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertCompactionTaskGenerator.class);
  private static final String DEFAULT_BUFFER_PERIOD = "7d";
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
      List<SegmentZKMetadata> completedSegments = getCompletedSegments(tableNameWithType, compactionConfigs);

      if (completedSegments.isEmpty()) {
        LOGGER.info("No completed segments were available for compaction for table: {}", tableNameWithType);
        continue;
      }

      // get server to segment mappings
      Map<String, List<String>> serverToSegments = _clusterInfoAccessor.getServerToSegmentsMap(tableNameWithType);
      Map<String, String> segmentToServer = getSegmentToServer(serverToSegments);
      PinotHelixResourceManager pinotHelixResourceManager = _clusterInfoAccessor.getPinotHelixResourceManager();
      BiMap<String, String> serverToEndpoints;
      try {
        serverToEndpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
      } catch (InvalidConfigException e) {
        throw new RuntimeException(e);
      }

      Map<String, SegmentZKMetadata> urlToSegment =
          getUrlToSegmentMappings(tableNameWithType, completedSegments, segmentToServer, serverToEndpoints);

      // request the urls from the servers
      CompletionServiceHelper completionServiceHelper = new CompletionServiceHelper(
            Executors.newCachedThreadPool(), new MultiThreadedHttpConnectionManager(), serverToEndpoints.inverse());
      CompletionServiceHelper.CompletionServiceResponse serviceResponse =
            completionServiceHelper.doMultiGetRequest(
                new ArrayList<>(urlToSegment.keySet()), tableNameWithType, true, 3000);

      // only compact segments that exceed the invalidRecordThreshold
      int invalidRecordsThreshold = Integer.parseInt(compactionConfigs.getOrDefault(
          UpsertCompactionTask.INVALID_RECORDS_THRESHOLD, DEFAULT_INVALID_RECORDS_THRESHOLD));
      List<SegmentZKMetadata> selectedSegments = new ArrayList<>();
      for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
        int invalidRecordCount = Integer.parseInt(streamResponse.getValue());
        if (invalidRecordCount > invalidRecordsThreshold) {
          SegmentZKMetadata segment = urlToSegment.get(streamResponse.getKey());
          selectedSegments.add(segment);
        }
      }

      int numTasks = 0;
      int maxTasks = getMaxTasks(taskType, tableNameWithType, taskConfigs);
      for (SegmentZKMetadata selectedSegment : selectedSegments) {
        if (numTasks == maxTasks) {
          break;
        }
        Map<String, String> configs = new HashMap<>();
        configs.put(MinionConstants.TABLE_NAME_KEY, tableNameWithType);
        configs.put(MinionConstants.SEGMENT_NAME_KEY, selectedSegment.getSegmentName());
        configs.put(MinionConstants.DOWNLOAD_URL_KEY, selectedSegment.getDownloadUrl());
        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
        configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, String.valueOf(selectedSegment.getCrc()));
        pinotTaskConfigs.add(new PinotTaskConfig(UpsertCompactionTask.TASK_TYPE, configs));
        numTasks++;
      }
      LOGGER.info("Finished generating {} tasks configs for table: {} " + "for task: {}",
          numTasks, tableNameWithType, taskType);
    }
    return pinotTaskConfigs;
  }

  private List<SegmentZKMetadata> getCompletedSegments(String tableNameWithType,
      Map<String, String> compactionConfigs) {
    List<SegmentZKMetadata> completedSegments = new ArrayList<>();
    String bufferPeriod = compactionConfigs.getOrDefault(
        UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_PERIOD);
    long bufferMs = TimeUtils.convertPeriodToMillis(bufferPeriod);
    List<SegmentZKMetadata> allSegments = _clusterInfoAccessor.getSegmentsZKMetadata(tableNameWithType);
    for (SegmentZKMetadata segment : allSegments) {
      CommonConstants.Segment.Realtime.Status status = segment.getStatus();
      // initial segments selection based on status and age
      if (status.isCompleted() && segment.getEndTimeMs() <= (System.currentTimeMillis() - bufferMs)) {
        completedSegments.add(segment);
      }
    }
    return completedSegments;
  }

  private static Map<String, SegmentZKMetadata> getUrlToSegmentMappings(String tableNameWithType,
      List<SegmentZKMetadata> completedSegments, Map<String, String> segmentToServer,
      BiMap<String, String> serverToEndpoints) {
    // get url to segment mappings
    Map<String, SegmentZKMetadata> urlToSegment = new HashMap<>();
    for (SegmentZKMetadata completedSegment : completedSegments) {
      String segmentName = completedSegment.getSegmentName();
      String server = segmentToServer.get(segmentName);
      String endpoint = serverToEndpoints.get(server);
      String url = String.format("%s/tables/%s/segments/%s/invalidRecordCount",
        endpoint, tableNameWithType, segmentName);
      urlToSegment.put(url, completedSegment);
    }
    return urlToSegment;
  }

  private static int getMaxTasks(String taskType, String tableNameWithType, Map<String, String> taskConfigs) {
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

  private static final String[] VALID_CONFIG_KEYS = {
      UpsertCompactionTask.BUFFER_TIME_PERIOD_KEY,
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
    return true;
  }
}
