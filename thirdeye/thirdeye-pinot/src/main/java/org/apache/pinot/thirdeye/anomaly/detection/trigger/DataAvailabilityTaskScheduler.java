/*
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

package org.apache.pinot.thirdeye.anomaly.detection.trigger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.anomaly.task.TaskInfoFactory;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to schedule detection tasks based on data availability events.
 */
public class DataAvailabilityTaskScheduler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DataAvailabilityTaskScheduler.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private ScheduledExecutorService executorService;
  private long delayInSec;
  private long fallBackTimeInSec;
  private long schedulingWindowInSec;
  private Map<Long, Long> taskLastCreateTimeMap;
  private TaskManager taskDAO;
  private DetectionConfigManager detectionConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  /**
   * Construct an instance of {@link DataAvailabilityTaskScheduler}
   * @param delayInSec delay after each run to avoid polling the database too often
   * @param fallBackTimeInSec global threshold for fallback if detection level one is not set
   */
  public DataAvailabilityTaskScheduler(long delayInSec, long fallBackTimeInSec, long schedulingWindowInSec) {
    this.delayInSec = delayInSec;
    this.fallBackTimeInSec = fallBackTimeInSec;
    this.schedulingWindowInSec = schedulingWindowInSec;
    this.taskLastCreateTimeMap = new HashMap<>();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.datasetConfigDAO = DAORegistry.getInstance().getDatasetConfigDAO();
  }

  @Override
  public void run() {
    Map<DetectionConfigDTO, Set<String>> detection2DatasetMap = new HashMap<>();
    Map<String, Pair<Long, Long>> dataset2RefreshTimeMap = new HashMap<>();
    populateDetectionMapAndDataset2RefreshTimeMap(detection2DatasetMap, dataset2RefreshTimeMap);
    Map<Long, TaskDTO> runningDetection = retrieveRunningDetectionTasks();
    int taskCount = 0;
    for (DetectionConfigDTO detectionConfig : detection2DatasetMap.keySet()) {
      try {
        long detectionConfigId = detectionConfig.getId();
        if (!runningDetection.containsKey(detectionConfigId)) {
          if (isAllDatasetUpdated(detectionConfig, detection2DatasetMap.get(detectionConfig), dataset2RefreshTimeMap)) {
            if (isWithinSchedulingWindow(detection2DatasetMap.get(detectionConfig), dataset2RefreshTimeMap)) {
              //TODO: additional check is required if detection is based on aggregated value across multiple data points
              createDetectionTask(detectionConfig);
              ThirdeyeMetricsUtil.eventScheduledTaskCounter.inc();
              taskCount++;
            } else {
              LOG.warn("Unable to schedule a task for {}, because it is out of scheduling window.", detectionConfigId);
            }
          } else if (needFallback(detectionConfig)) {
            LOG.info("Scheduling a task for detection {} due to the fallback mechanism.", detectionConfigId);
            createDetectionTask(detectionConfig);
            ThirdeyeMetricsUtil.eventScheduledTaskFallbackCounter.inc();
            taskCount++;
          }
        } else {
          LOG.info("Skipping creating detection task for detection {} because task {} is not finished.",
              detectionConfigId, runningDetection.get(detectionConfigId));
        }
      } catch (Exception e) {
        LOG.error("Error in scheduling a detection...", e);
      }
    }
    LOG.info("Schedule {} tasks in this run...", taskCount);
  }

  public void start() {
    executorService.scheduleWithFixedDelay(this, 0, delayInSec, TimeUnit.SECONDS);
  }

  public void close() {
    executorService.shutdownNow();
  }

  private void populateDetectionMapAndDataset2RefreshTimeMap(
      Map<DetectionConfigDTO, Set<String>> dataset2DetectionMap, Map<String, Pair<Long, Long>> dataset2RefreshTimeMap) {
    Map<Long, Set<String>> metricCache = new HashMap<>();
    List<DetectionConfigDTO> detectionConfigs = detectionConfigDAO.findAllActive()
        .stream().filter(DetectionConfigBean::isDataAvailabilitySchedule).collect(Collectors.toList());
    for (DetectionConfigDTO detectionConfig : detectionConfigs) {
      List<String> metricUrns = DetectionConfigFormatter
          .extractMetricUrnsFromProperties(detectionConfig.getProperties());
      Set<String> datasets = new HashSet<>();
      for (String urn : metricUrns) {
        MetricEntity me = MetricEntity.fromURN(urn);
        if (!metricCache.containsKey(me.getId())) {
          datasets.addAll(ThirdEyeUtils.getDatasetConfigsFromMetricUrn(urn)
              .stream().map(DatasetConfigDTO::getDataset).collect(Collectors.toList()));
          // cache the mapping in memory to avoid duplicate retrieval
          metricCache.put(me.getId(), datasets);
        } else {
          // retrieve dataset mapping from memory
          datasets.addAll(metricCache.get(me.getId()));
        }
      }
      if (datasets.isEmpty()) {
        LOG.error("No valid dataset is found for detection {}.", detectionConfig.getId());
        continue;
      }
      dataset2DetectionMap.put(detectionConfig, datasets);
      for (String dataset : datasets) {
        if (!dataset2RefreshTimeMap.containsKey(dataset)) {
          DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
          dataset2RefreshTimeMap.put(dataset,
              new ImmutablePair<>(datasetConfig.getLastRefreshTime(), datasetConfig.getLastRefreshEventTime()));
        }
      }
    }
  }

  private Map<Long, TaskDTO> retrieveRunningDetectionTasks() {
    List<TaskConstants.TaskStatus> statusList = new ArrayList<>();
    statusList.add(TaskConstants.TaskStatus.WAITING);
    statusList.add(TaskConstants.TaskStatus.RUNNING);
    List<TaskDTO> tasks = taskDAO.findByStatusesAndTypeWithinDays(statusList, TaskConstants.TaskType.DETECTION,
        (int) TimeUnit.MILLISECONDS.toDays(ThirdEyeUtils.DETECTION_TASK_MAX_LOOKBACK_WINDOW));
    Map<Long, TaskDTO> res = new HashMap<>(tasks.size());
    for (TaskDTO task : tasks) {
      res.put(ThirdEyeUtils.getDetectionIdFromJobName(task.getJobName()), task);
    }
    return res;
  }

  private long createDetectionTask(DetectionConfigDTO detectionConfig) throws JsonProcessingException {
    long detectionConfigId = detectionConfig.getId();
    // Make sure start time is not out of DETECTION_TASK_MAX_LOOKBACK_WINDOW
    long end = System.currentTimeMillis();
    long start = Math.max(detectionConfig.getLastTimestamp(),
        end - ThirdEyeUtils.DETECTION_TASK_MAX_LOOKBACK_WINDOW);
    DetectionPipelineTaskInfo taskInfo = new DetectionPipelineTaskInfo(detectionConfigId, start, end);
    String taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION);
    taskDTO.setJobName(TaskConstants.TaskType.DETECTION.toString() + "_" + detectionConfigId);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskInfo(taskInfoJson);
    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created detection pipeline task {} with taskId {}", taskDTO, taskId);
    taskLastCreateTimeMap.put(detectionConfigId, end);
    return taskId;
  }

  private void loadLatestTaskCreateTime(DetectionConfigDTO detectionConfig) throws Exception {
    long detectionConfigId = detectionConfig.getId();
    List<TaskDTO> tasks = taskDAO.findByNameOrderByCreateTime(TaskConstants.TaskType.DETECTION.toString() +
        "_" + detectionConfigId, 1,false);
    if (tasks.size() == 0) {
      taskLastCreateTimeMap.put(detectionConfigId, 0L);
    } else {
      DetectionPipelineTaskInfo taskInfo = (DetectionPipelineTaskInfo) TaskInfoFactory.getTaskInfoFromTaskType(
          TaskConstants.TaskType.DETECTION, tasks.get(0).getTaskInfo());
      taskLastCreateTimeMap.put(detectionConfigId, taskInfo.getEnd());
    }
  }

  private boolean isAllDatasetUpdated(DetectionConfigDTO detectionConfig, Set<String> datasets,
      Map<String, Pair<Long, Long>> dataset2RefreshTimeMap) {
    long lastTimestamp = detectionConfig.getLastTimestamp();
    return datasets.stream().allMatch(d -> dataset2RefreshTimeMap.get(d).getLeft() > lastTimestamp);
  }

  private boolean needFallback(DetectionConfigDTO detectionConfig) throws Exception {
    long detectionConfigId = detectionConfig.getId();
    long notRunThreshold = ((detectionConfig.getTaskTriggerFallBackTimeInSec() == 0) ?
        fallBackTimeInSec : detectionConfig.getTaskTriggerFallBackTimeInSec()) * 1000;
    if (!taskLastCreateTimeMap.containsKey(detectionConfigId)) {
      loadLatestTaskCreateTime(detectionConfig);
    }
    long lastRunTime = taskLastCreateTimeMap.get(detectionConfigId);
    return (System.currentTimeMillis() - lastRunTime >= notRunThreshold);
  }

  private boolean isWithinSchedulingWindow(Set<String> datasets, Map<String, Pair<Long, Long>> dataset2RefreshTimeMap) {
    long maxEventTime = datasets.stream()
        .map(dataset -> dataset2RefreshTimeMap.get(dataset).getRight())
        .reduce((v1, v2) -> (v1 > v2) ? v1 : v2).orElse(0L);
    return System.currentTimeMillis() <= maxEventTime + schedulingWindowInSec * 1000;
  }
}
