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

package org.apache.pinot.thirdeye.detection.alert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection alert job that run by the cron scheduler.
 * This job put detection alert task into database which can be picked up by works later.
 */
public class DetectionAlertJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertJob.class);
  private DetectionAlertConfigManager alertConfigDAO;
  private TaskManager taskDAO;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private MergedAnomalyResultManager anomalyDAO;

  public DetectionAlertJob() {
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.debug("Running " + jobExecutionContext.getJobDetail().getKey().getName());
    String jobKey = jobExecutionContext.getJobDetail().getKey().getName();
    long detectionAlertConfigId = getIdFromJobKey(jobKey);
    DetectionAlertConfigDTO configDTO = alertConfigDAO.findById(detectionAlertConfigId);
    if (configDTO == null) {
      LOG.error("Subscription config {} does not exist", detectionAlertConfigId);
    }

    DetectionAlertTaskInfo taskInfo = new DetectionAlertTaskInfo(detectionAlertConfigId);

    // check if a task for this detection alerter is already scheduled
    String jobName = String.format("%s_%d", TaskConstants.TaskType.DETECTION_ALERT, detectionAlertConfigId);
    List<TaskDTO> scheduledTasks = taskDAO.findByPredicate(Predicate.AND(
        Predicate.EQ("name", jobName),
        Predicate.OR(
            Predicate.EQ("status", TaskConstants.TaskStatus.RUNNING.toString()),
            Predicate.EQ("status", TaskConstants.TaskStatus.WAITING.toString())
        ))
    );
    if (!scheduledTasks.isEmpty()){
      // if a task is pending and not time out yet, don't schedule more
      LOG.info("Skip scheduling subscription task {}. Already queued.", jobName);
      return;
    }

    if (configDTO != null && !needNotification(configDTO)) {
      LOG.info("Skip scheduling subscription task {}. No anomaly to notify.", jobName);
      return;
    }

    String taskInfoJson = null;
    try {
      taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
    } catch (JsonProcessingException e) {
      LOG.error("Exception when converting AlertTaskInfo {} to jsonString", taskInfo, e);
    }

    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION_ALERT);
    taskDTO.setJobName(jobName);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskInfo(taskInfoJson);

    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created subscription task {} with settings {}", taskId, taskDTO);
  }

  /**
   * Check if we need to create a notification task.
   * If there is no anomaly generated (by looking at anomaly start_time) between last notification time
   * till now (left inclusive, right exclusive) then no need to create this task.
   *
   * The reason we use start_time is end_time is not accurate due to anomaly merge.
   * The timestamp stored in vectorLock is anomaly end_time, which should be fine.
   * For example, if previous anomaly is from t1 to t2, then the timestamp in vectorLock is t2.
   * If there is a new anomaly generated from t2 to t3 then we can still get this anomaly.
   *
   * @param configDTO The DetectionAlert Configuration.
   * @return true if it needs notification task. false otherwise.
   */
  private boolean needNotification(DetectionAlertConfigDTO configDTO) {
    Map<Long, Long> vectorLocks = configDTO.getVectorClocks();
    for (Map.Entry<Long, Long> vectorLock : vectorLocks.entrySet()) {
      long configId = vectorLock.getKey();
      long lastNotifiedTime = vectorLock.getValue();
      if (anomalyDAO.findByStartTimeInRangeAndDetectionConfigId(lastNotifiedTime, System.currentTimeMillis(), configId)
          .stream().anyMatch(x -> !x.isChild())) {
        return true;
      }
    }
    return false;
  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }
}
