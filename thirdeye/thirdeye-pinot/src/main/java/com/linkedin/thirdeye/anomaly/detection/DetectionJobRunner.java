package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.client.DAORegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class DetectionJobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final static DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private TaskGenerator taskGenerator;

  public DetectionJobRunner() {
    taskGenerator = new TaskGenerator();
  }


  public Long run(DetectionJobContext detectionJobContext) {

    long functionId = detectionJobContext.getAnomalyFunctionId();
    AnomalyFunctionDTO anomalyFunction = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);
    DateTime monitoringWindowStartTime = null;
    DateTime monitoringWindowEndTime = null;
    Long startTime = detectionJobContext.getStartTime();
    Long endTime = detectionJobContext.getEndTime();

    if (endTime == null) {
      long delayMillis = 0;
      if (anomalyFunction.getWindowDelay() != null) {
        delayMillis = TimeUnit.MILLISECONDS.convert(anomalyFunction.getWindowDelay(), anomalyFunction.getWindowDelayUnit());
      }
      monitoringWindowEndTime = new DateTime().minus(delayMillis);
    } else {
      monitoringWindowEndTime = new DateTime(endTime);
    }
    if (startTime == null) {
      int windowSize = anomalyFunction.getWindowSize();
      TimeUnit windowUnit = anomalyFunction.getWindowUnit();
      long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
      monitoringWindowStartTime = monitoringWindowEndTime.minus(windowMillis);
    } else {
      monitoringWindowStartTime = new DateTime(startTime);
    }

    monitoringWindowStartTime = alignTimestampsToDataTimezone(monitoringWindowStartTime, anomalyFunction.getCollection());
    monitoringWindowEndTime = alignTimestampsToDataTimezone(monitoringWindowEndTime, anomalyFunction.getCollection());

    // write to anomaly_jobs
    Long jobExecutionId = createJob(detectionJobContext.getJobName(), monitoringWindowStartTime, monitoringWindowEndTime);
    detectionJobContext.setJobExecutionId(jobExecutionId);

    // write to anomaly_tasks
    List<Long> taskIds = createTasks(detectionJobContext, monitoringWindowStartTime, monitoringWindowEndTime);

    return jobExecutionId;
  }


  private DateTime alignTimestampsToDataTimezone(DateTime inputDateTime, String collection) {

    try {
      DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(collection);
      TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
      TimeGranularity dataGranularity = timespec.getDataGranularity();
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
      jobExecutionId = DAO_REGISTRY.getJobDAO().save(jobSpec);

      LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", jobSpec, jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating detection job", e);
    }

    return jobExecutionId;
  }


  private List<Long> createTasks(DetectionJobContext detectionJobContext, DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    List<Long> taskIds = new ArrayList<>();
    try {

      List<DetectionTaskInfo> tasks =
          taskGenerator.createDetectionTasks(detectionJobContext, monitoringWindowStartTime, monitoringWindowEndTime);

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
