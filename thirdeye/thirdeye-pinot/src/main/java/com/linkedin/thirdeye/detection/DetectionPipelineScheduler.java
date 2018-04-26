package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Collection;
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


public class DetectionPipelineScheduler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipelineScheduler.class);
  public static final int DEFAULT_DETECTION_DELAY = 5;
  public static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  final DetectionConfigManager detectionDAO;
  final Scheduler scheduler;
  private ScheduledExecutorService scheduledExecutorService;

  public DetectionPipelineScheduler(DetectionConfigManager detectionDAO) throws Exception {
    this.detectionDAO = detectionDAO;
    this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  public void start() throws SchedulerException {
    scheduler.start();
    scheduledExecutorService.scheduleWithFixedDelay(this, 0, DEFAULT_DETECTION_DELAY, DEFAULT_ALERT_DELAY_UNIT);
  }

  @Override
  public void run() {
    try {
      Collection<DetectionConfigDTO> configs = this.detectionDAO.findAll();

      // add or update
      for (DetectionConfigDTO config : configs) {
        JobKey key = new JobKey(String.valueOf(config.getId()));

        if (scheduler.checkExists(key)) {
          LOG.warn("Detection config  " + key + " is already scheduled.");
          if (jobUpdated(config, key)){
            // restart job
            stopJob(key);
            startJob(config, key);
          }
        } else {
          startJob(config, key);
        }
      }

      // remove
      List<JobKey> scheduledJobs = getScheduledJobs();
      for (JobKey jobKey : scheduledJobs) {
        try {
          Long id = Long.parseLong(jobKey.getName());
          DetectionConfigDTO detectionDTO = detectionDAO.findById(id);
          if (detectionDTO == null) {
            LOG.info("Found a scheduled detection pipeline, but not found in the database {}", id);
            stopJob(jobKey);
          }
        } catch (NumberFormatException e) {
          LOG.error("Error job key {}", jobKey);
        }
      }
    } catch (SchedulerException e) {
      LOG.error("Error while scheduling detection pipeline", e);
    }
  }

  private boolean jobUpdated(DetectionConfigDTO config, JobKey key) throws SchedulerException {
    List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(key);
    CronTrigger cronTrigger = (CronTrigger) triggers.get(0);
    String cronInSchedule = cronTrigger.getCronExpression();

    if (!config.getCron().equals(cronInSchedule)) {
      LOG.info("Cron expression for detection pipeline {} has been changed from {}  to {}. " + "Restarting schedule",
          config.getId(), cronInSchedule, config.getCron());
      return true;
    }
    return false;
  }

  private void startJob(DetectionConfigDTO config, JobKey key) throws SchedulerException {
    Trigger trigger =
        TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(config.getCron())).build();
    JobDetail job = JobBuilder.newJob(DetectionPipelineJob.class).withIdentity(key).build();
    this.scheduler.scheduleJob(job, trigger);
    LOG.info(String.format("scheduled detection pipeline job %s.", key.getName()));
  }

  private void stopJob(JobKey jobKey) throws SchedulerException {
    if (!this.scheduler.checkExists(jobKey)) {
      throw new IllegalStateException(
          "Cannot stop detection pipeline " + jobKey.getName() + ", it has not been scheduled");
    }
    this.scheduler.deleteJob(jobKey);
    LOG.info("Stopped detection pipeline {}", jobKey.getName());
  }

  public List<JobKey> getScheduledJobs() throws SchedulerException {
    List<JobKey> activeJobKeys = new ArrayList<>();
    for (String groupName : this.scheduler.getJobGroupNames()) {
      for (JobKey jobKey : this.scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
        activeJobKeys.add(jobKey);
      }
    }
    return activeJobKeys;
  }

  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    scheduler.shutdown();
  }
}
