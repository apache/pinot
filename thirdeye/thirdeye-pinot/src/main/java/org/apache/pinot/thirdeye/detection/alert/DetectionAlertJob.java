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
import org.apache.pinot.thirdeye.datalayer.bao.AnomalySubscriptionGroupNotificationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalySubscriptionGroupNotificationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.TaskUtils;
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
  private final MergedAnomalyResultManager anomalyDAO;
  private final AnomalySubscriptionGroupNotificationManager anomalySubscriptionGroupNotificationDAO;

  public DetectionAlertJob() {
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.anomalySubscriptionGroupNotificationDAO = DAORegistry.getInstance().getAnomalySubscriptionGroupNotificationManager();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.debug("Running " + jobExecutionContext.getJobDetail().getKey().getName());
    String jobKey = jobExecutionContext.getJobDetail().getKey().getName();
    long detectionAlertConfigId = TaskUtils.getIdFromJobKey(jobKey);
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

    TaskDTO taskDTO = TaskUtils.buildTask(detectionAlertConfigId, taskInfoJson, TaskConstants.TaskType.DETECTION_ALERT);
    long taskId = taskDAO.save(taskDTO);
    LOG.info("Created {} task {} with settings {}", TaskConstants.TaskType.DETECTION_ALERT, taskId, taskDTO);
  }

  /**
   * Check if we need to create a subscription task.
   * If there is no anomaly generated (by looking at anomaly create_time) between last notification time
   * till now (left inclusive, right exclusive) then no need to create this task.
   *
   * Even if an anomaly gets merged (end_time updated) it will not renotify this anomaly as the create_time is not
   * modified.
   * For example, if previous anomaly is from t1 to t2 generated at t3, then the timestamp in vectorLock is t3.
   * If there is a new anomaly from t2 to t4 generated at t5, then we can still get this anomaly as t5 > t3.
   *
   * Also, check if there is any anomaly that needs re-notifying
   *
   * @param configDTO The Subscription Configuration.
   * @return true if it needs notification task. false otherwise.
   */
  private boolean needNotification(DetectionAlertConfigDTO configDTO) {
    Map<Long, Long> vectorLocks = configDTO.getVectorClocks();
    for (Map.Entry<Long, Long> vectorLock : vectorLocks.entrySet()) {
      long configId = vectorLock.getKey();
      long lastNotifiedTime = vectorLock.getValue();
      if (anomalyDAO.findByCreatedTimeInRangeAndDetectionConfigId(lastNotifiedTime, System.currentTimeMillis(), configId)
          .stream().anyMatch(x -> !x.isChild())) {
        return true;
      }
    }
    // in addition to checking the watermarks, check if any anomalies need to be re-notified by querying the anomaly subscription group notification table
    List<AnomalySubscriptionGroupNotificationDTO> anomalySubscriptionGroupNotifications =
        this.anomalySubscriptionGroupNotificationDAO.findByPredicate(
            Predicate.IN("detectionConfigId", vectorLocks.keySet().toArray()));
    return !anomalySubscriptionGroupNotifications.isEmpty();
  }
}
