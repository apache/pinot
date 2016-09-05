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
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.anomaly.task.TaskGenerator;

public class AlertJobRunner implements Job {

  private static final Logger LOG = LoggerFactory.getLogger(AlertJobRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String ALERT_JOB_CONTEXT = "ALERT_JOB_CONTEXT";

  private JobManager anomalyJobSpecDAO;
  private TaskManager anomalyTasksSpecDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private long alertConfigId;
  private DateTime windowStartTime;
  private DateTime windowEndTime;
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
    anomalyJobSpecDAO = alertJobContext.getAnomalyJobDAO();
    anomalyTasksSpecDAO = alertJobContext.getAnomalyTaskDAO();
    emailConfigurationDAO = alertJobContext.getEmailConfigurationDAO();
    alertConfigId = alertJobContext.getAlertConfigId();

    EmailConfigurationDTO alertConfig = emailConfigurationDAO.findById(alertConfigId);
    alertJobContext.setAlertConfig(alertConfig);

    windowEndTime = alertJobContext.getWindowEndTime();
    windowStartTime = alertJobContext.getWindowStartTime();

    // Compute window end
    if (windowEndTime == null) {
      long delayMillis = 0;
      if (alertConfig.getWindowDelay() != null) {
        delayMillis = TimeUnit.MILLISECONDS.convert(alertConfig.getWindowDelay(),
            alertConfig.getWindowDelayUnit());
      }
      Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
      windowEndTime = new DateTime(scheduledFireTime).minus(delayMillis);
    }

    // Compute window start
    if (windowStartTime == null) {
      int windowSize = alertConfig.getWindowSize();
      TimeUnit windowUnit = alertConfig.getWindowUnit();
      long windowMillis = TimeUnit.MILLISECONDS.convert(windowSize, windowUnit);
      windowStartTime = windowEndTime.minus(windowMillis);
    }
    alertJobContext.setWindowStartTime(windowStartTime);
    alertJobContext.setWindowEndTime(windowEndTime);

    // write to anomaly_jobs
    Long jobExecutionId = createJob();
    alertJobContext.setJobExecutionId(jobExecutionId);

    // write to anomaly_tasks
    List<Long> taskIds = createTasks();

  }

  private long createJob() {
    Long jobExecutionId = null;
    try {
      JobDTO anomalyJobSpec = new JobDTO();
      anomalyJobSpec.setJobName(alertJobContext.getJobName());
      anomalyJobSpec.setWindowStartTime(alertJobContext.getWindowStartTime().getMillis());
      anomalyJobSpec.setWindowEndTime(alertJobContext.getWindowEndTime().getMillis());
      anomalyJobSpec.setScheduleStartTime(System.currentTimeMillis());
      anomalyJobSpec.setStatus(JobStatus.SCHEDULED);
      jobExecutionId = anomalyJobSpecDAO.save(anomalyJobSpec);

      LOG.info("Created alert job {} with jobExecutionId {}", anomalyJobSpec,
          jobExecutionId);
    } catch (Exception e) {
      LOG.error("Exception in creating alert job", e);
    }

    return jobExecutionId;
  }

  private List<Long> createTasks() {
    List<Long> taskIds = new ArrayList<>();
    try {

      List<AlertTaskInfo> tasks = taskGenerator.createAlertTasks(alertJobContext);

      for (AlertTaskInfo taskInfo : tasks) {
        String taskInfoJson = null;
        try {
          taskInfoJson = OBJECT_MAPPER.writeValueAsString(taskInfo);
        } catch (JsonProcessingException e) {
          LOG.error("Exception when converting AlertTaskInfo {} to jsonString", taskInfo, e);
        }
        TaskDTO anomalyTaskSpec = new TaskDTO();
        anomalyTaskSpec.setTaskType(TaskType.ALERT);
        anomalyTaskSpec.setJobName(alertJobContext.getJobName());
        anomalyTaskSpec.setStatus(TaskStatus.WAITING);
        anomalyTaskSpec.setTaskStartTime(System.currentTimeMillis());
        anomalyTaskSpec.setTaskInfo(taskInfoJson);
        JobDTO anomalyJobSpec = anomalyJobSpecDAO.findById(alertJobContext.getJobExecutionId());
        anomalyTaskSpec.setJob(anomalyJobSpec);
        long taskId = anomalyTasksSpecDAO.save(anomalyTaskSpec);
        taskIds.add(taskId);
        LOG.info("Created alert task {} with taskId {}", anomalyTaskSpec, taskId);
      }
    } catch (Exception e) {
      LOG.error("Exception in creating alert tasks", e);
    }
    return taskIds;
  }


}
