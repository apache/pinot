package com.linkedin.thirdeye.anomaly.detection;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;

public class DetectionJobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String DETECTION_JOB_CONTEXT = "DETECTION_JOB_CONTEXT";
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private JobManager anomalyJobSpecDAO;
  private TaskManager anomalyTasksSpecDAO;
  private AnomalyFunctionManager anomalyFunctionSpecDAO;
  private long anomalyFunctionId;
  private DateTime windowStartTime;
  private DateTime windowEndTime;
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
    anomalyJobSpecDAO = detectionJobContext.getAnomalyJobDAO();
    anomalyTasksSpecDAO = detectionJobContext.getAnomalyTaskDAO();
    anomalyFunctionSpecDAO = detectionJobContext.getAnomalyFunctionDAO();
    anomalyFunctionId = detectionJobContext.getAnomalyFunctionId();

    AnomalyFunctionDTO anomalyFunctionSpec = getAnomalyFunctionSpec(anomalyFunctionId);
    if (anomalyFunctionSpec == null) {
      LOG.error("AnomalyFunction with id {} does not exist.. Exiting from job execution", anomalyFunctionId);
    } else {
      detectionJobContext.setAnomalyFunctionSpec(anomalyFunctionSpec);

      windowStartTime = detectionJobContext.getWindowStartTime();
      windowEndTime = detectionJobContext.getWindowEndTime();

      // Compute window end
      if (windowEndTime == null) {
        long delayMillis = 0;
        if (anomalyFunctionSpec.getWindowDelay() != null) {
          delayMillis = TimeUnit.MILLISECONDS.convert(anomalyFunctionSpec.getWindowDelay(),
              anomalyFunctionSpec.getWindowDelayUnit());
        }
        Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
        windowEndTime = new DateTime(scheduledFireTime).minus(delayMillis);
      }

      // Compute window start
      if (windowStartTime == null) {
        int windowSize = anomalyFunctionSpec.getWindowSize();
        TimeUnit windowUnit = anomalyFunctionSpec.getWindowUnit();
        long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
        windowStartTime = windowEndTime.minus(windowMillis);
      }

      windowStartTime = alignTimestampsToDataTimezone(windowStartTime, anomalyFunctionSpec.getCollection());
      windowEndTime = alignTimestampsToDataTimezone(windowEndTime, anomalyFunctionSpec.getCollection());
      detectionJobContext.setWindowStartTime(windowStartTime);
      detectionJobContext.setWindowEndTime(windowEndTime);

      // write to anomaly_jobs
      Long jobExecutionId = createJob();
      detectionJobContext.setJobExecutionId(jobExecutionId);

      // write to anomaly_tasks
      List<Long> taskIds = createTasks();
    }

  }

  private DateTime alignTimestampsToDataTimezone(DateTime inputDateTime, String collection) {

    try {
      CollectionSchema collectionSchema = CACHE_REGISTRY_INSTANCE.getCollectionSchemaCache().get(collection);
      TimeGranularity dataGranularity = collectionSchema.getTime().getDataGranularity();
      String timeFormat = collectionSchema.getTime().getFormat();
      if (dataGranularity.getUnit().equals(TimeUnit.DAYS)) {
        DateTimeZone dataTimeZone = Utils.getDataTimeZone(collection);
        DateTimeFormatter inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(dataTimeZone);

        long inputMillis = inputDateTime.getMillis();
        String inputDateTimeString = inputDataDateTimeFormatter.print(inputMillis);
        long timeZoneOffsetMillis = inputDataDateTimeFormatter.parseMillis(inputDateTimeString);
        inputDateTime = new DateTime(timeZoneOffsetMillis);
      }
    } catch (ExecutionException e) {
      LOG.error("Exception in aligning timestamp to data time zone", e);
    }
    return inputDateTime;
  }

  private long createJob() {
    Long jobExecutionId = null;
    try {
      JobDTO anomalyJobSpec = new JobDTO();
      anomalyJobSpec.setJobName(detectionJobContext.getJobName());
      anomalyJobSpec.setWindowStartTime(detectionJobContext.getWindowStartTime().getMillis());
      anomalyJobSpec.setWindowEndTime(detectionJobContext.getWindowEndTime().getMillis());
      anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis());
      anomalyJobSpec.setStatus(JobStatus.SCHEDULED);
      jobExecutionId = anomalyJobSpecDAO.save(anomalyJobSpec);

      LOG.info("Created anomalyJobSpec {} with jobExecutionId {}", anomalyJobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating detection job", e);
    }

    return jobExecutionId;
  }

  private List<Long> createTasks() {
    List<Long> taskIds = new ArrayList<>();
    try {

      List<DetectionTaskInfo> tasks = taskGenerator.createDetectionTasks(detectionJobContext);

      for (DetectionTaskInfo taskInfo : tasks) {
        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting DetectionTaskInfo {} to jsonString", taskInfo, e);
        }
        TaskDTO anomalyTaskSpec = new TaskDTO();
        anomalyTaskSpec.setTaskType(TaskType.ANOMALY_DETECTION);
        anomalyTaskSpec.setJobName(detectionJobContext.getJobName());
        anomalyTaskSpec.setStatus(TaskStatus.WAITING);
        anomalyTaskSpec.setStartTime(System.currentTimeMillis());
        anomalyTaskSpec.setTaskInfo(taskInfoJson);
        JobDTO anomalyJobSpec = anomalyJobSpecDAO.findById(detectionJobContext.getJobExecutionId());
        anomalyTaskSpec.setJob(anomalyJobSpec);
        long taskId = anomalyTasksSpecDAO.save(anomalyTaskSpec);
        taskIds.add(taskId);
        LOG.info("Created anomalyTask {} with taskId {}", anomalyTaskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating detection tasks", e);
    }
    return taskIds;
  }

  private AnomalyFunctionDTO getAnomalyFunctionSpec(Long anomalyFunctionId) {
    AnomalyFunctionDTO anomalyFunctionSpec = null;
    try {
      anomalyFunctionSpec = anomalyFunctionSpecDAO.findById(anomalyFunctionId);
    } catch (Exception e)  {
      LOG.error("Exception in getting anomalyFunctionSpec by id", e);
    }
    return anomalyFunctionSpec;
  }

}
