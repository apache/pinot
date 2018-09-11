/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.alert.v2.AlertJobSchedulerV2;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
  private static final int DEFAULT_ALERT_DELAY = 1;
  private static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  final Scheduler scheduler;
  private ScheduledExecutorService scheduledExecutorService;
  private DetectionAlertConfigManager alertConfigDAO;

  public DetectionAlertScheduler() throws SchedulerException {
    this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
  }

  private Set<JobKey> getScheduledJobs() throws SchedulerException {
    return scheduler.getJobKeys(GroupMatcher.jobGroupEquals(TaskConstants.TaskType.DETECTION_ALERT.toString()));
  }

  public void start() throws SchedulerException {
    this.scheduler.start();
    this.scheduledExecutorService.scheduleWithFixedDelay(this, 0, DEFAULT_ALERT_DELAY, DEFAULT_ALERT_DELAY_UNIT);
  }

  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    this.scheduler.shutdown();
  }

  private void stopJob(JobKey key) throws SchedulerException {
    if (!scheduler.checkExists(key)) {
      throw new IllegalStateException("Cannot stop alert config " + key + ", it has not been scheduled");
    }
    scheduler.deleteJob(key);
    LOG.info("Stopped alert config {}", key);
  }

  public void run() {
    try {
      // read all alert configs
      LOG.info("Reading all alert configs..");
      List<DetectionAlertConfigDTO> alertConfigs = alertConfigDAO.findAll();

      // get active jobs
      Set<JobKey> scheduledJobs = getScheduledJobs();
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
      for (JobKey scheduledJobKey : scheduledJobs) {
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

  private void deleteAlertJob(JobKey scheduledJobKey) throws SchedulerException {
    Long configId = getIdFromJobKey(scheduledJobKey);
    DetectionAlertConfigDTO alertConfigSpec = alertConfigDAO.findById(configId);
    if (alertConfigSpec == null) {
      LOG.info("Found scheduled, but not in database {}", configId);
      stopJob(scheduledJobKey);
    }
  }

  private Long getIdFromJobKey(JobKey jobKey) {
    String[] tokens = jobKey.getName().split("_");
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

  private void createOrUpdateAlertJob(Set<JobKey> scheduledJobs, DetectionAlertConfigDTO alertConfig)
      throws SchedulerException {
    Long id = alertConfig.getId();
    boolean isActive = alertConfig.isActive();

    JobKey key = new JobKey(getJobKey(id), TaskConstants.TaskType.DETECTION_ALERT.toString());
    boolean isScheduled = scheduledJobs.contains(key);

    if (isActive) {
      if (isScheduled) {
        String cronInDatabase = alertConfig.getCronExpression();

        List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(key);
        CronTrigger cronTrigger = (CronTrigger) triggers.get(0);
        String cronInSchedule = cronTrigger.getCronExpression();
        // cron expression has been updated, restart this job
        if (!cronInDatabase.equals(cronInSchedule)) {
          LOG.info(
              "Cron expression for config {} with jobKey {} has been changed from {}  to {}. " + "Restarting schedule",
              id, key, cronInSchedule, cronInDatabase);
          stopJob(key);
          startJob(alertConfig, key);
        }
      } else {
        LOG.info("Found active but not scheduled {}", id);
        startJob(alertConfig, key);
      }
    } else {
      if (isScheduled) {
        LOG.info("Found inactive but scheduled {}", id);
        stopJob(key);
      }
      // for all jobs with not isActive, and not isScheduled, no change required
    }
  }
}
