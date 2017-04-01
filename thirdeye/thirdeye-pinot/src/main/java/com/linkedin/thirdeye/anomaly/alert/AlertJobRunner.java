package com.linkedin.thirdeye.anomaly.alert;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
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
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;

public class AlertJobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(AlertJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String ALERT_JOB_CONTEXT = "ALERT_JOB_CONTEXT";
  public static final String ALERT_JOB_MONITORING_WINDOW_START_TIME = "ALERT_JOB_MONITORING_WINDOW_START_TIME";
  public static final String ALERT_JOB_MONITORING_WINDOW_END_TIME = "ALERT_JOB_MONITORING_WINDOW_END_TIME";

  private JobManager jobDAO;
  private TaskManager taskDAO;
  private AlertJobContext alertJobContext;

  private TaskGenerator taskGenerator;

  public AlertJobRunner() {
    taskGenerator = new TaskGenerator();
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    LOG.info("Running " + jobExecutionContext.getJobDetail().getKey().toString());

    alertJobContext = (AlertJobContext) jobExecutionContext.getJobDetail().getJobDataMap()
        .get(ALERT_JOB_CONTEXT);
    jobDAO = alertJobContext.getJobDAO();
    taskDAO = alertJobContext.getTaskDAO();
    EmailConfigurationManager emailConfigurationDAO = alertJobContext.getEmailConfigurationDAO();
    long alertConfigId = alertJobContext.getAlertConfigId();

    EmailConfigurationDTO alertConfig = emailConfigurationDAO.findById(alertConfigId);
    if (alertConfig == null) {
      LOG.error("Alert config with id {} does not exist", alertConfigId);
    } else {
      alertJobContext.setAlertConfig(alertConfig);

      DateTime monitoringWindowStartTime =
          (DateTime) jobExecutionContext.getJobDetail().getJobDataMap().get(ALERT_JOB_MONITORING_WINDOW_START_TIME);
      DateTime monitoringWindowEndTime =
          (DateTime) jobExecutionContext.getJobDetail().getJobDataMap().get(ALERT_JOB_MONITORING_WINDOW_END_TIME);

      // Compute window end
      if (monitoringWindowEndTime == null) {
        long delayMillis = 0;
        if (alertConfig.getWindowDelay() != null) {
          delayMillis = TimeUnit.MILLISECONDS.convert(alertConfig.getWindowDelay(),
              alertConfig.getWindowDelayUnit());
        }
        Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
        monitoringWindowEndTime = new DateTime(scheduledFireTime).minus(delayMillis);
      }

      // Compute window start according to window end
      if (monitoringWindowStartTime == null) {
        int windowSize = alertConfig.getWindowSize();
        TimeUnit windowUnit = alertConfig.getWindowUnit();
        long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
        monitoringWindowStartTime = monitoringWindowEndTime.minus(windowMillis);
      }

      // write to alert_jobs
      Long jobExecutionId = createJob(monitoringWindowStartTime, monitoringWindowEndTime);
      alertJobContext.setJobExecutionId(jobExecutionId);

      // write to alert_tasks
      List<Long> taskIds = createTasks(monitoringWindowStartTime, monitoringWindowEndTime);
    }

  }

  private long createJob(DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    Long jobExecutionId = null;
    try {
      JobDTO jobSpec = new JobDTO();
      jobSpec.setJobName(alertJobContext.getJobName());
      jobSpec.setWindowStartTime(monitoringWindowStartTime.getMillis());
      jobSpec.setWindowEndTime(monitoringWindowEndTime.getMillis());
      jobSpec.setScheduleStartTime(System.currentTimeMillis());
      jobSpec.setStatus(JobStatus.SCHEDULED);
      jobSpec.setTaskType(TaskType.ALERT);
      jobExecutionId = jobDAO.save(jobSpec);

      LOG.info("Created alert job {} with jobExecutionId {}", jobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating alert job", e);
    }

    return jobExecutionId;
  }

  private List<Long> createTasks(DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    List<Long> taskIds = new ArrayList<>();
    try {

      List<AlertTaskInfo> tasks =
          taskGenerator.createAlertTasks(alertJobContext, monitoringWindowStartTime, monitoringWindowEndTime);

      for (AlertTaskInfo taskInfo : tasks) {

        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting AlertTaskInfo {} to jsonString", taskInfo, e);
        }
        TaskDTO taskSpec = new TaskDTO();
        taskSpec.setTaskType(TaskType.ALERT);
        taskSpec.setJobName(alertJobContext.getJobName());
        taskSpec.setStatus(TaskStatus.WAITING);
        taskSpec.setStartTime(System.currentTimeMillis());
        taskSpec.setTaskInfo(taskInfoJson);
        taskSpec.setJobId(alertJobContext.getJobExecutionId());
        long taskId = taskDAO.save(taskSpec);
        taskIds.add(taskId);
        LOG.info("Created alert task {} with taskId {}", taskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating alert tasks", e);
    }
    return taskIds;
  }


}
