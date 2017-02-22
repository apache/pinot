package com.linkedin.thirdeye.anomaly.detectionex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
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


public class DetectionExJobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionExJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String DETECTION_EX_JOB_CONTEXT = "DETECTION_EX_JOB_CONTEXT";
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final static DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private DetectionExJobContext detectionJobContext;

  private TaskGenerator taskGenerator;

  public DetectionExJobRunner() {
    taskGenerator = new TaskGenerator();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());

    detectionJobContext =
        (DetectionExJobContext) jobExecutionContext.getJobDetail().getJobDataMap().get(DETECTION_EX_JOB_CONTEXT);
    long anomalyFunctionId = detectionJobContext.getAnomalyFunctionId();

    AnomalyFunctionExDTO spec = getAnomalyFunctionSpec(anomalyFunctionId);
    if (spec == null) {
      LOG.error("AnomalyFunction with id {} does not exist.. Exiting from job execution", anomalyFunctionId);
    } else {
      detectionJobContext.setAnomalyFunctionExSpec(spec);

      try {
        // write to anomaly_jobs
        JobDTO jobSpec = createJob();
        DAO_REGISTRY.getJobDAO().save(jobSpec);

        detectionJobContext.setJobExecutionId(jobSpec.getId());
      } catch (Exception e) {
        throw new JobExecutionException("Error creating job", e);
      }

      try {
        // write to anomaly_tasks
        TaskDTO taskSpec = createTask();
        DAO_REGISTRY.getTaskDAO().save(taskSpec);
      } catch (Exception e) {
        throw new JobExecutionException("Error creating task", e);
      }
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

  private JobDTO createJob() throws Exception {
    JobDTO jobSpec = new JobDTO();
    jobSpec.setJobName(detectionJobContext.getJobName());
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setStatus(JobStatus.SCHEDULED);
    return jobSpec;
  }

  private TaskDTO createTask() throws Exception {
    DetectionExTaskInfo taskInfo = new DetectionExTaskInfo();
    taskInfo.setAnomalyFunctionExSpec(detectionJobContext.getAnomalyFunctionExSpec());
    taskInfo.setJobExecutionId(detectionJobContext.getAnomalyFunctionId());

    String taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);

    TaskDTO taskSpec = new TaskDTO();
    taskSpec.setTaskType(TaskType.ANOMALY_DETECTION_EX);
    taskSpec.setJobName(detectionJobContext.getJobName());
    taskSpec.setStatus(TaskStatus.WAITING);
    taskSpec.setStartTime(System.currentTimeMillis());
    taskSpec.setTaskInfo(taskInfoJson);
    taskSpec.setJobId(detectionJobContext.getJobExecutionId());
    return taskSpec;
  }

  private AnomalyFunctionExDTO getAnomalyFunctionSpec(Long anomalyFunctionId) {
    AnomalyFunctionExDTO spec = null;
    try {
      spec = DAO_REGISTRY.getAnomalyFunctionExDAO().findById(anomalyFunctionId);
    } catch (Exception e) {
      LOG.error("Exception in getting anomalyFunctionSpec by id", e);
    }
    return spec;
  }
}
