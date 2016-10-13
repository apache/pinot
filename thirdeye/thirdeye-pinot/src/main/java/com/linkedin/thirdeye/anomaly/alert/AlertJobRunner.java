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

  private JobManager anomalyJobSpecDAO;
  private TaskManager anomalyTasksSpecDAO;
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
    EmailConfigurationManager emailConfigurationDAO = alertJobContext.getEmailConfigurationDAO();
    long alertConfigId = alertJobContext.getAlertConfigId();

    EmailConfigurationDTO alertConfig = emailConfigurationDAO.findById(alertConfigId);
    if (alertConfig == null) {
      LOG.error("Alert config with id {} does not exist", alertConfigId);
    } else {
      alertJobContext.setAlertConfig(alertConfig);

      // originalMonitoringWindowStart and EndTime are used to restore the original state of the JobContext at the
      // end of this method.
      // Details: This method modifies the variables, windowStartTime and windowEndTime, of alertJobContext, which is
      //   an undesirable behavior: Once start and end time are set (non-null), this function stops computing the latest
      //   start and end time for the current job. Consequently, the current job would work on the same monitoring
      //   window as the last job. Hence, we need to ensure the original state of JobContext is restored.
      DateTime originalMonitoringWindowEndTime = alertJobContext.getWindowEndTime();
      DateTime originalMonitoringWindowStartTime = alertJobContext.getWindowStartTime();

      DateTime monitoringWindowStartTime = originalMonitoringWindowStartTime;
      DateTime monitoringWindowEndTime = originalMonitoringWindowEndTime;

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

      alertJobContext.setWindowStartTime(monitoringWindowStartTime);
      alertJobContext.setWindowEndTime(monitoringWindowEndTime);

      // write to alert_jobs
      Long jobExecutionId = createJob();
      alertJobContext.setJobExecutionId(jobExecutionId);

      // write to alert_tasks
      List<Long> taskIds = createTasks();

      // restore the original state of the job; otherwise, it will work on the same monitoring window afterwards
      alertJobContext.setWindowStartTime(originalMonitoringWindowStartTime);
      alertJobContext.setWindowEndTime(originalMonitoringWindowEndTime);
    }

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
        anomalyTaskSpec.setStartTime(System.currentTimeMillis());
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
