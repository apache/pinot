/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.scheduler;

import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AbstractBean;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionAlertConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.detection.TaskUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertJob;
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
import org.quartz.utils.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection alert scheduler. Schedule new detection alert jobs or update existing detection alert jobs
 * in the cron scheduler.
 */
public class SubscriptionCronScheduler implements ThirdEyeCronScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(SubscriptionCronScheduler.class);

  private static final int DEFAULT_ALERT_DELAY = 1;
  private static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  final Scheduler scheduler;
  private ScheduledExecutorService scheduledExecutorService;
  private DetectionAlertConfigManager alertConfigDAO;

  public SubscriptionCronScheduler() throws SchedulerException {
    this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.alertConfigDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
  }

  @Override
  public void start() throws SchedulerException {
    this.scheduler.start();
    this.scheduledExecutorService.scheduleWithFixedDelay(this, 0, DEFAULT_ALERT_DELAY, DEFAULT_ALERT_DELAY_UNIT);
  }

  @Override
  public void run() {
    try {
      // read all alert configs
      LOG.info("Scheduling all the subscription configs");
      List<DetectionAlertConfigDTO> alertConfigs = alertConfigDAO.findAll();

      // get active jobs
      Set<JobKey> scheduledJobs = getScheduledJobs();
      LOG.info("Scheduled jobs {}", scheduledJobs.stream().map(Key::getName).collect(Collectors.toList()));

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

  @Override
  public Set<JobKey> getScheduledJobs() throws SchedulerException {
    return scheduler.getJobKeys(GroupMatcher.jobGroupEquals(TaskConstants.TaskType.DETECTION_ALERT.toString()));
  }

  @Override
  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    this.scheduler.shutdown();
  }

  @Override
  public void startJob(AbstractBean config, JobKey key) throws SchedulerException {
    Trigger trigger = TriggerBuilder.newTrigger().withSchedule(
        CronScheduleBuilder.cronSchedule(((DetectionAlertConfigBean) config).getCronExpression())).build();
    JobDetail job = JobBuilder.newJob(DetectionAlertJob.class).withIdentity(key).build();
    this.scheduler.scheduleJob(job, trigger);
    LOG.info(String.format("scheduled subscription pipeline job %s", key.getName()));
  }

  @Override
  public void stopJob(JobKey key) throws SchedulerException {
    if (!scheduler.checkExists(key)) {
      throw new IllegalStateException("Cannot stop alert config " + key + ", it has not been scheduled");
    }
    scheduler.deleteJob(key);
    LOG.info("Stopped alert config {}", key);
  }

  @Override
  public String getJobKey(Long id) {
    return String.format("%s_%d", TaskConstants.TaskType.DETECTION_ALERT, id);
  }

  private void deleteAlertJob(JobKey scheduledJobKey) throws SchedulerException {
    Long configId = TaskUtils.getIdFromJobKey(scheduledJobKey.getName());
    DetectionAlertConfigDTO alertConfigSpec = alertConfigDAO.findById(configId);
    if (alertConfigSpec == null) {
      LOG.info("Found scheduled, but not in database {}", configId);
      stopJob(scheduledJobKey);
    }
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
