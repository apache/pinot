package com.linkedin.thirdeye.anomaly.alert;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.anomaly.job.JobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

/**
 * Scheduler for anomaly detection jobs
 */
public class AlertJobScheduler implements JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(AlertJobScheduler.class);
  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private EmailConfigurationManager emailConfigurationDAO;

  public AlertJobScheduler(JobManager anomalyJobDAO, TaskManager anomalyTaskDAO,
      EmailConfigurationManager emailConfigurationDAO) {
    this.anomalyJobDAO = anomalyJobDAO;
    this.anomalyTaskDAO = anomalyTaskDAO;
    this.emailConfigurationDAO = emailConfigurationDAO;

    schedulerFactory = new StdSchedulerFactory();
    try {
      quartzScheduler = schedulerFactory.getScheduler();
    } catch (SchedulerException e) {
      LOG.error("Exception while starting quartz scheduler", e);
    }
  }

  public List<String> getActiveJobs() throws SchedulerException {
    List<String> activeJobKeys = new ArrayList<>();
    for (String groupName : quartzScheduler.getJobGroupNames()) {
      for (JobKey jobKey : quartzScheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
        activeJobKeys.add(jobKey.getName());
      }
    }
    return activeJobKeys;
  }

  public void start() throws SchedulerException {
    quartzScheduler.start();

    // start all active alert functions
    List<EmailConfigurationDTO> alertConfigs = emailConfigurationDAO.findAll();
    for (EmailConfigurationDTO alertConfig : alertConfigs) {
      if (alertConfig.getIsActive()) {
        AlertJobContext alertJobContext = new AlertJobContext();
        alertJobContext.setAnomalyJobDAO(anomalyJobDAO);
        alertJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
        alertJobContext.setEmailConfigurationDAO(emailConfigurationDAO);
        alertJobContext.setAlertConfigId(alertConfig.getId());
        String jobKey = getJobKey(alertConfig.getId());
        alertJobContext.setJobName(jobKey);
        scheduleJob(alertJobContext, alertConfig);
      }
    }
  }

  public void stop() throws SchedulerException {
    quartzScheduler.shutdown();
  }

  public void startJob(Long id) throws SchedulerException {
    EmailConfigurationDTO alertConfig = emailConfigurationDAO.findById(id);
    if (alertConfig == null) {
      throw new IllegalArgumentException("No alert config with id " + id);
    }
    if (!alertConfig.getIsActive()) {
      throw new IllegalStateException("Alert config with id " + id + " is not active");
    }
    String jobKey = getJobKey(alertConfig.getId());
    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Alert config with id " + id + " is already scheduled");
    }

    AlertJobContext alertJobContext = new AlertJobContext();
    alertJobContext.setAnomalyJobDAO(anomalyJobDAO);
    alertJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
    alertJobContext.setEmailConfigurationDAO(emailConfigurationDAO);
    alertJobContext.setAlertConfigId(id);
    alertJobContext.setJobName(jobKey);

    scheduleJob(alertJobContext, alertConfig);
  }

  public void stopJob(Long id) throws SchedulerException {
    String jobKey = getJobKey(id);
    if (!quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Cannot stop alert config with id " + id + ", it has not been scheduled");
    }
    quartzScheduler.deleteJob(JobKey.jobKey(jobKey));
    LOG.info("Stopped alert config {}", id);
  }

  public void runAdHoc(Long id, DateTime windowStartTime, DateTime windowEndTime) {
    EmailConfigurationDTO alertConfig = emailConfigurationDAO.findById(id);
    if (alertConfig == null) {
      throw new IllegalArgumentException("No alert config with id " + id);
    }
    String triggerKey = String.format("alert_adhoc_trigger_%d", id);
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    String jobKey = "adhoc_" + getJobKey(id);
    JobDetail job = JobBuilder.newJob(AlertJobRunner.class).withIdentity(jobKey).build();

    AlertJobContext alertJobContext = new AlertJobContext();
    alertJobContext.setAnomalyJobDAO(anomalyJobDAO);
    alertJobContext.setAnomalyTaskDAO(anomalyTaskDAO);
    alertJobContext.setEmailConfigurationDAO(emailConfigurationDAO);
    alertJobContext.setAlertConfigId(id);
    alertJobContext.setJobName(jobKey);
    alertJobContext.setWindowStartTime(windowStartTime);
    alertJobContext.setWindowEndTime(windowEndTime);

    job.getJobDataMap().put(AlertJobRunner.ALERT_JOB_CONTEXT, alertJobContext);

    try {
      quartzScheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling job", e);
    }

    LOG.info("Started {}: {}", jobKey, alertConfig);
  }


  private void scheduleJob(JobContext jobContext, EmailConfigurationDTO alertConfig) {

    LOG.info("Starting {}", jobContext.getJobName());
    String triggerKey = String.format("alert_scheduler_trigger_%d", alertConfig.getId());
    CronTrigger trigger =
        TriggerBuilder.newTrigger().withIdentity(triggerKey)
            .withSchedule(CronScheduleBuilder.cronSchedule(alertConfig.getCron())).build();

    String jobKey = jobContext.getJobName();
    JobDetail job = JobBuilder.newJob(AlertJobRunner.class).withIdentity(jobKey).build();

    job.getJobDataMap().put(AlertJobRunner.ALERT_JOB_CONTEXT, jobContext);

    try {
      quartzScheduler.scheduleJob(job, trigger);
    } catch (SchedulerException e) {
      LOG.error("Exception while scheduling alert job", e);
    }

    LOG.info("Started {}: {}", jobKey, alertConfig);
  }

  private String getJobKey(Long id) {
    String jobKey = String.format("%s_%d", TaskType.ALERT, id);
    return jobKey;
  }
}
