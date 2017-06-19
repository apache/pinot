package com.linkedin.thirdeye.anomaly.alert;

import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
import com.linkedin.thirdeye.datasource.DAORegistry;

/**
 * Scheduler for anomaly detection jobs
 */
@Deprecated
// By Alert Job Scheduler V2
public class AlertJobScheduler implements JobScheduler, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(AlertJobScheduler.class);
  public static final int DEFAULT_ALERT_DELAY = 10;
  public static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private ScheduledExecutorService scheduledExecutorService;
  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public AlertJobScheduler() {
    this.anomalyJobDAO = DAO_REGISTRY.getJobDAO();
    this.anomalyTaskDAO = DAO_REGISTRY.getTaskDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getEmailConfigurationDAO();

    schedulerFactory = new StdSchedulerFactory();
    try {
      quartzScheduler = schedulerFactory.getScheduler();
    } catch (SchedulerException e) {
      LOG.error("Exception while starting quartz scheduler", e);
    }
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  public List<String> getScheduledJobs() throws SchedulerException {
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

    scheduledExecutorService.scheduleWithFixedDelay(this, 0, DEFAULT_ALERT_DELAY, DEFAULT_ALERT_DELAY_UNIT);
  }

  public void run() {
    try {
      // read all alert configs
      LOG.info("Reading all alert configs..");
      List<EmailConfigurationDTO> alertConfigs = emailConfigurationDAO.findAll();

      // get active jobs
      List<String> scheduledJobs = getScheduledJobs();
      LOG.info("Scheduled jobs {}", scheduledJobs);

      for (EmailConfigurationDTO alertConfig : alertConfigs) {
        Long id = alertConfig.getId();
        String jobKey = getJobKey(id);
        boolean isActive = alertConfig.isActive();
        boolean isScheduled = scheduledJobs.contains(jobKey);

        // for all jobs with isActive, but not in scheduled jobs,
        // schedule them with quartz, as function is newly created, or newly activated
        if (isActive && !isScheduled) {
          LOG.info("Found active but not scheduled {}", id);
          startJob(alertConfig, jobKey);
        }
        // for all jobs with not isActive, but in scheduled jobs,
        // remove them from quartz, as function is newly deactivated
        else if (!isActive && isScheduled) {
          LOG.info("Found inactive but scheduled {}", id);
          stopJob(jobKey);
        }
        // for all jobs with isActive, and isScheduled,
        // updates to a function will be picked up automatically by the next run
        // but check for cron updates
        else if (isActive && isScheduled) {
          String cronInDatabase = alertConfig.getCron();
          List<Trigger> triggers = (List<Trigger>) quartzScheduler.getTriggersOfJob(JobKey.jobKey(jobKey));
          CronTrigger cronTrigger = (CronTrigger) triggers.get(0);
          String cronInSchedule = cronTrigger.getCronExpression();
          // cron expression has been updated, restart this job
          if (!cronInDatabase.equals(cronInSchedule)) {
            LOG.info("Cron expression for config {} with jobKey {} has been changed from {}  to {}. "
                + "Restarting schedule", id, jobKey, cronInSchedule, cronInDatabase);
            stopJob(jobKey);
            startJob(alertConfig, jobKey);
          }
        }

        // for all jobs with not isActive, and not isScheduled, no change required
      }

      // for any scheduled jobs, not having a function in the database,
      // stop the schedule, as function has been deleted
      for (String scheduledJobKey : scheduledJobs) {
        Long configId = getIdFromJobKey(scheduledJobKey);
        EmailConfigurationDTO alertConfigSpec = emailConfigurationDAO.findById(configId);
        if (alertConfigSpec == null) {
          LOG.info("Found scheduled, but not in database {}", configId);
          stopJob(scheduledJobKey);
        }
      }
    } catch (SchedulerException e) {
      LOG.error("Exception in reading active jobs", e);
    }
  }

  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    quartzScheduler.shutdown();
  }

  public void startJob(Long id) throws SchedulerException {
    EmailConfigurationDTO alertConfig = emailConfigurationDAO.findById(id);
    if (alertConfig == null) {
      throw new IllegalArgumentException("No alert config with id " + id);
    }
    if (!alertConfig.isActive()) {
      throw new IllegalStateException("Alert config with id " + id + " is not active");
    }
    String jobKey = getJobKey(alertConfig.getId());
    startJob(alertConfig, jobKey);
  }

  private void startJob(EmailConfigurationDTO alertConfig, String jobKey) throws SchedulerException {

    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Alert config  " + jobKey + " is already scheduled");
    }

    AlertJobContext alertJobContext = new AlertJobContext();
    alertJobContext.setJobDAO(anomalyJobDAO);
    alertJobContext.setTaskDAO(anomalyTaskDAO);
    alertJobContext.setEmailConfigurationDAO(emailConfigurationDAO);
    alertJobContext.setAlertConfigId(alertConfig.getId());
    alertJobContext.setAlertConfig(alertConfig);
    alertJobContext.setJobName(jobKey);
    scheduleJob(alertJobContext, alertConfig);
  }

  public void stopJob(Long id) throws SchedulerException {
    String jobKey = getJobKey(id);
    stopJob(jobKey);
  }

  public void stopJob(String jobKey) throws SchedulerException {
    if (!quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Cannot stop alert config " + jobKey + ", it has not been scheduled");
    }
    quartzScheduler.deleteJob(JobKey.jobKey(jobKey));
    LOG.info("Stopped alert config {}", jobKey);
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
    alertJobContext.setJobDAO(anomalyJobDAO);
    alertJobContext.setTaskDAO(anomalyTaskDAO);
    alertJobContext.setEmailConfigurationDAO(emailConfigurationDAO);
    alertJobContext.setAlertConfigId(id);
    alertJobContext.setJobName(jobKey);

    job.getJobDataMap().put(AlertJobRunner.ALERT_JOB_CONTEXT, alertJobContext);
    job.getJobDataMap().put(AlertJobRunner.ALERT_JOB_MONITORING_WINDOW_START_TIME, windowStartTime);
    job.getJobDataMap().put(AlertJobRunner.ALERT_JOB_MONITORING_WINDOW_END_TIME, windowEndTime);

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

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }
}
