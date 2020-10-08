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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.TaskUtils;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.TaskUtils.*;


/**
 * This class is to schedule detection tasks based on data availability events.
 */
public class DataAvailabilityTaskScheduler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DataAvailabilityTaskScheduler.class);
  private ScheduledExecutorService executorService;
  private long sleepPerRunInSec;
  private long fallBackTimeInSec;
  private long schedulingWindowInSec;
  private long scheduleDelayInSec;

  // Maintains mapping from each detection to the detection end time of it's last run.
  // Fallback runs based on the last task run (successful or not).
  private Map<Long, Long> detectionIdToLastTaskEndTimeMap;

  private TaskManager taskDAO;
  private DetectionConfigManager detectionConfigDAO;
  private DatasetConfigManager datasetConfigDAO;

  /**
   * Construct an instance of {@link DataAvailabilityTaskScheduler}
   * @param sleepPerRunInSec delay after each run to avoid polling the database too often
   * @param fallBackTimeInSec global threshold for fallback if detection level one is not set
   */
  public DataAvailabilityTaskScheduler(long sleepPerRunInSec, long fallBackTimeInSec, long schedulingWindowInSec, long scheduleDelayInSec) {
    this.sleepPerRunInSec = sleepPerRunInSec;
    this.fallBackTimeInSec = fallBackTimeInSec;
    this.schedulingWindowInSec = schedulingWindowInSec;
    this.scheduleDelayInSec = scheduleDelayInSec;
    this.detectionIdToLastTaskEndTimeMap = new HashMap<>();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.datasetConfigDAO = DAORegistry.getInstance().getDatasetConfigDAO();
  }

  /**
   * Runs every @{link DataAvailabilitySchedulingConfiguration.scheduleDelayInSec}
   */
  @Override
  public void run() {
    Map<DetectionConfigDTO, Set<String>> detection2DatasetMap = new HashMap<>();
    Map<String, DatasetConfigDTO> datasetConfigMap = new HashMap<>();
    populateDetectionMapAndDatasetConfigMap(detection2DatasetMap, datasetConfigMap);
    Map<Long, TaskDTO> runningDetection = retrieveRunningDetectionTasks();
    int taskCount = 0;
    long detectionEndTime = System.currentTimeMillis();
    for (DetectionConfigDTO detectionConfig : detection2DatasetMap.keySet()) {
      try {
        long detectionConfigId = detectionConfig.getId();
        DetectionPipelineTaskInfo taskInfo = TaskUtils.buildTaskInfoFromDetectionConfig(detectionConfig, detectionEndTime);
        if (!runningDetection.containsKey(detectionConfigId)) {
          if (isAllDatasetUpdated(detectionConfig, detection2DatasetMap.get(detectionConfig), datasetConfigMap)) {
            if (isWithinSchedulingWindow(detection2DatasetMap.get(detectionConfig), datasetConfigMap)) {
              //TODO: additional check is required if detection is based on aggregated value across multiple data points
              createDetectionTask(taskInfo);
              detectionIdToLastTaskEndTimeMap.put(detectionConfig.getId(), taskInfo.getEnd());
              ThirdeyeMetricsUtil.eventScheduledTaskCounter.inc();
              taskCount++;
            } else {
              LOG.warn("Unable to schedule a task for {}, because it is out of scheduling window.", detectionConfigId);
            }
          }

          // Note: Fallback SLA & Data availability SLA are independent of each other.
          // For example, if an event doesn't arrive within 24 hours, do a fallback.
          // On the other hand, a user can setup an SLA alert if there is no data for 3 days.
          if (needFallback(detectionConfig)) {
            LOG.info("Scheduling a task for detection {} due to the fallback mechanism.", detectionConfigId);
            createDetectionTask(taskInfo);
            if (DetectionUtils.isDataQualityCheckEnabled(detectionConfig)) {
              createDataQualityTask(taskInfo);
              LOG.info("Scheduling a task for data sla check on detection config {} due to the fallback mechanism.",
                  detectionConfigId);
            }

            detectionIdToLastTaskEndTimeMap.put(detectionConfig.getId(), taskInfo.getEnd());
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
    executorService.scheduleWithFixedDelay(this, 0, sleepPerRunInSec, TimeUnit.SECONDS);
  }

  public void close() {
    executorService.shutdownNow();
  }

  private void populateDetectionMapAndDatasetConfigMap(
      Map<DetectionConfigDTO, Set<String>> dataset2DetectionMap,
      Map<String, DatasetConfigDTO> datasetConfigMap) {
    Map<Long, Set<String>> metricCache = new HashMap<>();
    List<DetectionConfigDTO> detectionConfigs = detectionConfigDAO.findAllActive()
        .stream().filter(DetectionConfigBean::isDataAvailabilitySchedule).collect(Collectors.toList());
    for (DetectionConfigDTO detectionConfig : detectionConfigs) {
      Set<String> metricUrns = DetectionConfigFormatter
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
        if (!datasetConfigMap.containsKey(dataset)) {
          DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
          datasetConfigMap.put(dataset, datasetConfig);
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

  private void loadLatestTaskCreateTime(DetectionConfigDTO detectionConfig) throws Exception {
    long detectionConfigId = detectionConfig.getId();
    List<TaskDTO> tasks = taskDAO.findByNameOrderByCreateTime(TaskConstants.TaskType.DETECTION.toString() +
        "_" + detectionConfigId, 1,false);
    if (tasks.size() == 0) {
      detectionIdToLastTaskEndTimeMap.put(detectionConfigId, detectionConfig.getLastTimestamp());
    } else {
      // Load the watermark
      DetectionPipelineTaskInfo taskInfo = (DetectionPipelineTaskInfo) TaskInfoFactory.getTaskInfoFromTaskType(
          TaskConstants.TaskType.DETECTION, tasks.get(0).getTaskInfo());
      detectionIdToLastTaskEndTimeMap.put(detectionConfigId, taskInfo.getEnd());
    }
  }

  private boolean isAllDatasetUpdated(DetectionConfigDTO detectionConfig, Set<String> datasets,
      Map<String, DatasetConfigDTO> datasetConfigMap) {
    long lastTimestamp = detectionConfig.getLastTimestamp();
    long curr = System.currentTimeMillis();
    return datasets.stream().allMatch(d -> datasetConfigMap.get(d).getLastRefreshTime() > lastTimestamp
        && curr - datasetConfigMap.get(d).getLastRefreshEventTime() >= TimeUnit.SECONDS.toMillis(scheduleDelayInSec));
  }

  /* check if the fallback cron need to be triggered if the detection has not been run for long time */
  private boolean needFallback(DetectionConfigDTO detectionConfig) throws Exception {
    long detectionConfigId = detectionConfig.getId();
    long notRunThreshold = ((detectionConfig.getTaskTriggerFallBackTimeInSec() == 0) ?
        fallBackTimeInSec : detectionConfig.getTaskTriggerFallBackTimeInSec()) * 1000;
    if (!detectionIdToLastTaskEndTimeMap.containsKey(detectionConfigId)) {
      loadLatestTaskCreateTime(detectionConfig);
    }
    long lastRunTime = detectionIdToLastTaskEndTimeMap.get(detectionConfigId);
    return (System.currentTimeMillis() - lastRunTime >= notRunThreshold);
  }

  /*
  check if the current time is within scheduling window to avoid repeating scheduling the same task if
  detection watermark is not moving forward */
  private boolean isWithinSchedulingWindow(Set<String> datasets,
      Map<String, DatasetConfigDTO> datasetConfigMap) {
    long maxEventTime = datasets.stream()
        .map(dataset -> datasetConfigMap.get(dataset).getLastRefreshEventTime())
        .max(Comparator.naturalOrder()).orElse(0L);
    return System.currentTimeMillis() <= maxEventTime + schedulingWindowInSec * 1000;
  }
}
