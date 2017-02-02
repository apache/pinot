package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.client.DAORegistry;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
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
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

public class DetectionJobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String DETECTION_JOB_CONTEXT = "DETECTION_JOB_CONTEXT";
  public static final String DETECTION_JOB_MONITORING_WINDOW_START_TIME = "DETECTION_JOB_MONITORING_WINDOW_START_TIME";
  public static final String DETECTION_JOB_MONITORING_WINDOW_END_TIME = "DETECTION_JOB_MONITORING_WINDOW_END_TIME";
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final static DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private DetectionJobContext detectionJobContext;

  private TaskGenerator taskGenerator;

  public DetectionJobRunner() {
    taskGenerator = new TaskGenerator();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());

    detectionJobContext = (DetectionJobContext) jobExecutionContext.getJobDetail().getJobDataMap()
        .get(DETECTION_JOB_CONTEXT);
    long anomalyFunctionId = detectionJobContext.getAnomalyFunctionId();

    AnomalyFunctionDTO anomalyFunctionSpec = getAnomalyFunctionSpec(anomalyFunctionId);
    if (anomalyFunctionSpec == null) {
      LOG.error("AnomalyFunction with id {} does not exist.. Exiting from job execution", anomalyFunctionId);
    } else {
      detectionJobContext.setAnomalyFunctionSpec(anomalyFunctionSpec);

      DateTime monitoringWindowStartTime =
          (DateTime) jobExecutionContext.getJobDetail().getJobDataMap().get(DETECTION_JOB_MONITORING_WINDOW_START_TIME);
      DateTime monitoringWindowEndTime =
          (DateTime) jobExecutionContext.getJobDetail().getJobDataMap().get(DETECTION_JOB_MONITORING_WINDOW_END_TIME);

      // Compute window end
      if (monitoringWindowEndTime == null) {
        long delayMillis = 0;
        if (anomalyFunctionSpec.getWindowDelay() != null) {
          delayMillis = TimeUnit.MILLISECONDS.convert(anomalyFunctionSpec.getWindowDelay(), anomalyFunctionSpec.getWindowDelayUnit());
        }
        Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
        monitoringWindowEndTime = new DateTime(scheduledFireTime).minus(delayMillis);
      }

      // Compute window start according to window end
      if (monitoringWindowStartTime == null) {
        int windowSize = anomalyFunctionSpec.getWindowSize();
        TimeUnit windowUnit = anomalyFunctionSpec.getWindowUnit();
        long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
        monitoringWindowStartTime = monitoringWindowEndTime.minus(windowMillis);
      }

      monitoringWindowStartTime = alignTimestampsToDataTimezone(monitoringWindowStartTime, anomalyFunctionSpec.getCollection());
      monitoringWindowEndTime = alignTimestampsToDataTimezone(monitoringWindowEndTime, anomalyFunctionSpec.getCollection());

      // write to anomaly_jobs
      Long jobExecutionId = createJob(monitoringWindowStartTime, monitoringWindowEndTime);
      detectionJobContext.setJobExecutionId(jobExecutionId);

      // write to anomaly_tasks
      List<Long> taskIds = createTasks(monitoringWindowStartTime, monitoringWindowEndTime);
    }

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

  private long createJob(DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    Long jobExecutionId = null;
    try {
      JobDTO jobSpec = new JobDTO();
      jobSpec.setJobName(detectionJobContext.getJobName());
      jobSpec.setWindowStartTime(monitoringWindowStartTime.getMillis());
      jobSpec.setWindowEndTime(monitoringWindowEndTime.getMillis());
      jobSpec.setScheduleStartTime(System.currentTimeMillis());
      jobSpec.setStatus(JobStatus.SCHEDULED);
      jobExecutionId = DAO_REGISTRY.getJobDAO().save(jobSpec);

      LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", jobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating detection job", e);
    }

    return jobExecutionId;
  }

  private List<Long> createTasks(DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
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

  private AnomalyFunctionDTO getAnomalyFunctionSpec(Long anomalyFunctionId) {
    AnomalyFunctionDTO anomalyFunctionSpec = null;
    try {
      anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(anomalyFunctionId);
    } catch (Exception e)  {
      LOG.error("Exception in getting anomalyFunctionSpec by id", e);
    }
    return anomalyFunctionSpec;
  }

}
