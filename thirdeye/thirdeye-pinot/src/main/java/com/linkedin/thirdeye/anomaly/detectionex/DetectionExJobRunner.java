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

  private final static DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private DetectionExJobContext context;

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());

    context = (DetectionExJobContext) jobExecutionContext.getJobDetail().getJobDataMap().get(DETECTION_EX_JOB_CONTEXT);
    long anomalyFunctionId = context.getAnomalyFunctionId();

    AnomalyFunctionExDTO spec = DAO_REGISTRY.getAnomalyFunctionExDAO().findById(anomalyFunctionId);
    if (spec == null) {
      LOG.error("AnomalyFunction with id {} does not exist.. Exiting from job execution", anomalyFunctionId);
    } else {
      context.setAnomalyFunctionExSpec(spec);

      try {
        // write to anomaly_jobs
        JobDTO jobSpec = createJob();
        DAO_REGISTRY.getJobDAO().save(jobSpec);

        context.setJobExecutionId(jobSpec.getId());
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

  private JobDTO createJob() throws Exception {
    JobDTO jobSpec = new JobDTO();
    jobSpec.setJobName(context.getJobName());
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setStatus(JobStatus.SCHEDULED);
    return jobSpec;
  }

  private TaskDTO createTask() throws Exception {
    AnomalyFunctionExDTO spec = context.getAnomalyFunctionExSpec();

    DetectionExTaskInfo taskInfo = new DetectionExTaskInfo();
    taskInfo.setAnomalyFunctionExSpec(spec);
    taskInfo.setJobExecutionId(spec.getId());
    taskInfo.setMergeWindow(context.getMergeWindow());

    switch(context.getType()) {
      case ONLINE:
        populateTimeForOnline(taskInfo);
        break;
      case BACKFILL:
        populateTimeForBackfill(taskInfo);
        break;
      default:
        throw new IllegalStateException(String.format("Unknown job type '%s'", context.getType()));
    }

    String taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);

    TaskDTO taskSpec = new TaskDTO();
    taskSpec.setTaskType(TaskType.ANOMALY_DETECTION_EX);
    taskSpec.setJobName(context.getJobName());
    taskSpec.setStatus(TaskStatus.WAITING);
    taskSpec.setStartTime(System.currentTimeMillis());
    taskSpec.setTaskInfo(taskInfoJson);
    taskSpec.setJobId(context.getJobExecutionId());
    return taskSpec;
  }

  private DetectionExTaskInfo populateTimeForOnline(DetectionExTaskInfo t) {
    AnomalyFunctionExDTO spec = context.getAnomalyFunctionExSpec();
    long timestamp = DateTime.now(DateTimeZone.UTC).getMillis();
    long alignedEnd = timestamp / spec.getMonitoringWindowAlignment() * spec.getMonitoringWindowAlignment();
    long alignedStart = alignedEnd - spec.getMonitoringWindowLookback();

    t.setMonitoringWindowStart(alignedStart);
    t.setMonitoringWindowEnd(alignedEnd);
    return t;
  }

  private DetectionExTaskInfo populateTimeForBackfill(DetectionExTaskInfo t) {
    t.setMonitoringWindowStart(context.getMonitoringWindowStart());
    t.setMonitoringWindowEnd(context.getMonitoringWindowEnd());
    return t;
  }
}
