/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.detection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class DetectionJobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobRunner.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TaskGenerator taskGenerator;
  long functionId;

  public DetectionJobRunner() {
    taskGenerator = new TaskGenerator();
  }


  /**
   * Creates anomaly detection job and tasks, depending on the information in context
   * @param detectionJobContext
   * @return
   */
  public Long run(DetectionJobContext detectionJobContext) {

    functionId = detectionJobContext.getAnomalyFunctionId();
    AnomalyFunctionDTO anomalyFunction = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);

    List<DateTime> monitoringWindowStartTimes = new ArrayList<>();
    List<DateTime> monitoringWindowEndTimes = new ArrayList<>();

    List<Long> startTimes = detectionJobContext.getStartTimes();
    List<Long> endTimes = detectionJobContext.getEndTimes();

    for (Long startTime : startTimes) {
      DateTime monitoringWindowStartTime = new DateTime(startTime);
      monitoringWindowStartTime = alignTimestampsToDataTimezone(monitoringWindowStartTime, anomalyFunction.getCollection());
      monitoringWindowStartTimes.add(monitoringWindowStartTime);
    }
    for (Long endTime : endTimes) {
      DateTime monitoringWindowEndTime = new DateTime(endTime);
      monitoringWindowEndTime = alignTimestampsToDataTimezone(monitoringWindowEndTime, anomalyFunction.getCollection());
      monitoringWindowEndTimes.add(monitoringWindowEndTime);
    }


    // write to anomaly_jobs
    Long jobExecutionId = createJob(detectionJobContext.getJobName(), monitoringWindowStartTimes.get(0), monitoringWindowEndTimes.get(0));
    detectionJobContext.setJobExecutionId(jobExecutionId);

    // write to anomaly_tasks
    List<Long> taskIds = createTasks(detectionJobContext, monitoringWindowStartTimes, monitoringWindowEndTimes);

    return jobExecutionId;
  }


  private DateTime alignTimestampsToDataTimezone(DateTime inputDateTime, String collection) {

    try {
      DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(collection);
      TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
      TimeGranularity dataGranularity = datasetConfig.bucketTimeGranularity();
      String timeFormat = timespec.getFormat();
      if (dataGranularity.getUnit().equals(TimeUnit.DAYS)) {
        DateTimeZone dataTimeZone = Utils.getDataTimeZone(collection);
        DateTimeFormatter inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(dataTimeZone);

        long inputMillis = inputDateTime.getMillis();
        String inputDateTimeString = inputDataDateTimeFormatter.print(inputMillis);
        long timeZoneOffsetMillis = inputDataDateTimeFormatter.parseMillis(inputDateTimeString);
        inputDateTime = new DateTime(timeZoneOffsetMillis);
      }
    } catch (Exception e) {
      LOG.error("Exception in aligning timestamp to data time zone", e);
    }
    return inputDateTime;
  }

  private long createJob(String jobName, DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    Long jobExecutionId = null;
    try {
      JobDTO jobSpec = new JobDTO();
      jobSpec.setJobName(jobName);
      jobSpec.setWindowStartTime(monitoringWindowStartTime.getMillis());
      jobSpec.setWindowEndTime(monitoringWindowEndTime.getMillis());
      jobSpec.setScheduleStartTime(System.currentTimeMillis());
      jobSpec.setStatus(JobStatus.SCHEDULED);
      jobSpec.setTaskType(TaskType.ANOMALY_DETECTION);
      jobSpec.setConfigId(functionId);
      jobExecutionId = DAO_REGISTRY.getJobDAO().save(jobSpec);

      LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", jobSpec, jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating detection job", e);
    }

    return jobExecutionId;
  }


  private List<Long> createTasks(DetectionJobContext detectionJobContext, List<DateTime> monitoringWindowStartTimes,
      List<DateTime> monitoringWindowEndTimes) {
    List<Long> taskIds = new ArrayList<>();
    try {

      List<DetectionTaskInfo> tasks =
          taskGenerator.createDetectionTasks(detectionJobContext, monitoringWindowStartTimes, monitoringWindowEndTimes);

      for (DetectionTaskInfo taskInfo : tasks) {

        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting DetectionTaskInfo {} to jsonString", taskInfo, e);
        }
        TaskDTO taskSpec = new TaskDTO();
        taskSpec.setTaskType(TaskType.ANOMALY_DETECTION);
        taskSpec.setJobName(detectionJobContext.getJobName());
        taskSpec.setStatus(TaskStatus.WAITING);
        taskSpec.setStartTime(System.currentTimeMillis());
        taskSpec.setTaskInfo(taskInfoJson);
        taskSpec.setJobId(detectionJobContext.getJobExecutionId());
        long taskId = DAO_REGISTRY.getTaskDAO().save(taskSpec);
        taskIds.add(taskId);
        LOG.info("Created anomalyTask {} with taskId {}", taskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating detection tasks", e);
    }
    return taskIds;
  }




}
