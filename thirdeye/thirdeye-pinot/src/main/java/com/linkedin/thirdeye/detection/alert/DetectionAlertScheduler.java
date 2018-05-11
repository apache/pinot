package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.alert.v2.AlertJobSchedulerV2;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection alert scheduler. Schedule new detection alert jobs or update existing detection alert jobs
 * in the cron scheduler.
 */
public class DetectionAlertScheduler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(AlertJobSchedulerV2.class);
  private static final int DEFAULT_ALERT_DELAY = 10;
  private static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  final Scheduler scheduler;
  private ScheduledExecutorService scheduledExecutorService;
  private DetectionAlertConfigManager alertConfigDAO;

  public DetectionAlertScheduler() throws SchedulerException {
    this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
  }

  private List<String> getScheduledJobs() throws SchedulerException {
    List<String> activeJobKeys = new ArrayList<>();
    for (String groupName : scheduler.getJobGroupNames()) {
      for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
        activeJobKeys.add(jobKey.getName());
      }
    }
    return activeJobKeys;
  }

  public void start() throws SchedulerException {
    this.scheduler.start();
    this.scheduledExecutorService.scheduleWithFixedDelay(this, 0, DEFAULT_ALERT_DELAY, DEFAULT_ALERT_DELAY_UNIT);
  }

  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    this.scheduler.shutdown();
  }

  private void stopJob(String jobKey) throws SchedulerException {
    if (!scheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Cannot stop alert config " + jobKey + ", it has not been scheduled");
    }
    scheduler.deleteJob(JobKey.jobKey(jobKey));
    LOG.info("Stopped alert config {}", jobKey);
  }

  public void run() {
    try {
      // read all alert configs
      LOG.info("Reading all alert configs..");
      List<DetectionAlertConfigDTO> alertConfigs = alertConfigDAO.findAll();

      // get active jobs
      List<String> scheduledJobs = getScheduledJobs();
      LOG.info("Scheduled jobs {}", scheduledJobs);

      for (DetectionAlertConfigDTO alertConfig : alertConfigs) {
        try {
          createOrUpdateAlertJob(scheduledJobs, alertConfig);
        } catch (Exception e) {
          LOG.error("Could not write job for alert config id {}. Skipping. {}", alertConfig.getId(), alertConfig, e);
        }
      }

      // for any scheduled jobs, not having a function in the database,
      // stop the schedule, as function has been deleted
      for (String scheduledJobKey : scheduledJobs) {
        try {
          deleteAlertJob(scheduledJobKey);
        } catch (Exception e) {
          LOG.error("Could not delete alert job '{}'. Skipping.", scheduledJobKey, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Error running scheduler", e);
    }
  }

  private String getJobKey(Long id) {
    String jobKey = String.format("%s_%d", TaskConstants.TaskType.DETECTION_ALERT, id);
    return jobKey;
  }

  private void deleteAlertJob(String scheduledJobKey) throws SchedulerException {
    Long configId = getIdFromJobKey(scheduledJobKey);
    DetectionAlertConfigDTO alertConfigSpec = alertConfigDAO.findById(configId);
    if (alertConfigSpec == null) {
      LOG.info("Found scheduled, but not in database {}", configId);
      stopJob(scheduledJobKey);
    }
  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }

  private void startJob(DetectionAlertConfigDTO config, JobKey key) throws SchedulerException {
    Trigger trigger =
        TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(config.getCronExpression())).build();
    JobDetail job = JobBuilder.newJob(DetectionAlertJob.class).withIdentity(key).build();
    this.scheduler.scheduleJob(job, trigger);
    LOG.info(String.format("scheduled detection pipeline job %s.", key.getName()));
  }

  private void createOrUpdateAlertJob(List<String> scheduledJobs, DetectionAlertConfigDTO alertConfig)
      throws SchedulerException {
    Long id = alertConfig.getId();
    String jobKey = getJobKey(id);
    boolean isActive = alertConfig.isActive();
    boolean isScheduled = scheduledJobs.contains(jobKey);

    if (isActive) {
      if (isScheduled) {
        String cronInDatabase = alertConfig.getCronExpression();

        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(JobKey.jobKey(jobKey));
        CronTrigger cronTrigger = (CronTrigger) triggers.get(0);
        String cronInSchedule = cronTrigger.getCronExpression();
        // cron expression has been updated, restart this job
        if (!cronInDatabase.equals(cronInSchedule)) {
          LOG.info(
              "Cron expression for config {} with jobKey {} has been changed from {}  to {}. " + "Restarting schedule",
              id, jobKey, cronInSchedule, cronInDatabase);
          stopJob(jobKey);
          startJob(alertConfig, new JobKey(jobKey));
        }
      } else {
        LOG.info("Found active but not scheduled {}", id);
        startJob(alertConfig, new JobKey(jobKey));
      }
    } else {
      if (isScheduled) {
        LOG.info("Found inactive but scheduled {}", id);
        stopJob(jobKey);
      }
      // for all jobs with not isActive, and not isScheduled, no change required
    }
  }
}
