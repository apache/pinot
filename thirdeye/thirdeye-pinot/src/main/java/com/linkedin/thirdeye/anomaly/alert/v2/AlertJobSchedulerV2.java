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

package com.linkedin.thirdeye.anomaly.alert.v2;

import com.linkedin.thirdeye.anomaly.alert.AlertJobContext;
import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.anomaly.job.JobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
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

public class AlertJobSchedulerV2 implements JobScheduler, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(AlertJobSchedulerV2.class);
  public static final int DEFAULT_ALERT_DELAY = 10;
  public static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  private SchedulerFactory schedulerFactory;
  private Scheduler quartzScheduler;
  private ScheduledExecutorService scheduledExecutorService;
  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private AlertConfigManager alertConfigDAO;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public AlertJobSchedulerV2() {
    this.anomalyJobDAO = DAO_REGISTRY.getJobDAO();
    this.anomalyTaskDAO = DAO_REGISTRY.getTaskDAO();
    this.alertConfigDAO = DAO_REGISTRY.getAlertConfigDAO();
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
      List<AlertConfigDTO> alertConfigs = alertConfigDAO.findAll();

      // get active jobs
      List<String> scheduledJobs = getScheduledJobs();
      LOG.info("Scheduled jobs {}", scheduledJobs);

      for (AlertConfigDTO alertConfig : alertConfigs) {
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
        } catch(Exception e) {
          LOG.error("Could not delete alert job '{}'. Skipping.", scheduledJobKey, e);
        }
      }

    } catch (Exception e) {
      LOG.error("Error running scheduler", e);
    }
  }

  private void deleteAlertJob(String scheduledJobKey) throws SchedulerException {
    Long configId = getIdFromJobKey(scheduledJobKey);
    AlertConfigDTO alertConfigSpec = alertConfigDAO.findById(configId);
    if (alertConfigSpec == null) {
      LOG.info("Found scheduled, but not in database {}", configId);
      stopJob(scheduledJobKey);
    }
  }

  private void createOrUpdateAlertJob(List<String> scheduledJobs, AlertConfigDTO alertConfig) throws SchedulerException {
    Long id = alertConfig.getId();
    String jobKey = getJobKey(id);
    boolean isActive = alertConfig.isActive();
    boolean isScheduled = scheduledJobs.contains(jobKey);

    if (isActive) {
      if (isScheduled) {
        String cronInDatabase = alertConfig.getCronExpression();
        if (!StringUtils.isBlank(alertConfig.getHolidayCronExpression())) {
          LOG.info("Using holiday mode for alert config id '{}' with expression '{}'", id, alertConfig.getHolidayCronExpression());
          cronInDatabase = alertConfig.getHolidayCronExpression();
        }

        List<Trigger> triggers = (List<Trigger>) quartzScheduler.getTriggersOfJob(JobKey.jobKey(jobKey));
        CronTrigger cronTrigger = (CronTrigger) triggers.get(0);
        String cronInSchedule = cronTrigger.getCronExpression();
        // cron expression has been updated, restart this job
        if (!cronInDatabase.equals(cronInSchedule)) {
          LOG.info(
              "Cron expression for config {} with jobKey {} has been changed from {}  to {}. "
                  + "Restarting schedule", id, jobKey, cronInSchedule, cronInDatabase);
          stopJob(jobKey);
          startJob(alertConfig, jobKey);
        }
      } else {
        LOG.info("Found active but not scheduled {}", id);
        startJob(alertConfig, jobKey);
      }
    } else {
      if (isScheduled) {
        LOG.info("Found inactive but scheduled {}", id);
        stopJob(jobKey);
      }
      // for all jobs with not isActive, and not isScheduled, no change required
    }
  }

  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    quartzScheduler.shutdown();
  }

  public void startJob(Long id) throws SchedulerException {
    AlertConfigDTO alertConfig = alertConfigDAO.findById(id);
    if (alertConfig == null) {
      throw new IllegalArgumentException("No alert config with id " + id);
    }
    if (!alertConfig.isActive()) {
      throw new IllegalStateException("Alert config with id " + id + " is not active");
    }
    String jobKey = getJobKey(alertConfig.getId());
    startJob(alertConfig, jobKey);
  }

  private void startJob(AlertConfigDTO alertConfig, String jobKey) throws SchedulerException {

    if (quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Alert config  " + jobKey + " is already scheduled");
    }
    AlertJobContext alertJobContext = new AlertJobContext();
    alertJobContext.setJobDAO(anomalyJobDAO);
    alertJobContext.setTaskDAO(anomalyTaskDAO);
    alertJobContext.setAlertConfigId(alertConfig.getId());
    alertJobContext.setAlertConfigDTO(alertConfig);
    alertJobContext.setJobName(jobKey);
    scheduleJob(alertJobContext, alertConfig);
  }

  public void stopJob(Long id) throws SchedulerException {
    String jobKey = getJobKey(id);
    stopJob(jobKey);
  }

  private void stopJob(String jobKey) throws SchedulerException {
    if (!quartzScheduler.checkExists(JobKey.jobKey(jobKey))) {
      throw new IllegalStateException("Cannot stop alert config " + jobKey + ", it has not been scheduled");
    }
    quartzScheduler.deleteJob(JobKey.jobKey(jobKey));
    LOG.info("Stopped alert config {}", jobKey);
  }

  private void scheduleJob(JobContext jobContext, AlertConfigDTO alertConfig) throws SchedulerException {
    LOG.info("Starting {}", jobContext.getJobName());
    String triggerKey = String.format("alert_scheduler_trigger_%d", alertConfig.getId());

    String cronSchedule = alertConfig.getCronExpression();
    if (!StringUtils.isBlank(alertConfig.getHolidayCronExpression())) {
      cronSchedule = alertConfig.getHolidayCronExpression();
    }

    CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
        .withSchedule(CronScheduleBuilder.cronSchedule(cronSchedule)).build();
    String jobKey = jobContext.getJobName();
    JobDetail job = JobBuilder.newJob(AlertJobRunnerV2.class).withIdentity(jobKey).build();
    job.getJobDataMap().put(AlertJobRunnerV2.ALERT_JOB_CONTEXT_V2, jobContext);
    quartzScheduler.scheduleJob(job, trigger);
    LOG.info("Started {}: {}", jobKey, alertConfig);
  }

  private String getJobKey(Long id) {
    String jobKey = String.format("%s_%d", TaskConstants.TaskType.ALERT2, id);
    return jobKey;
  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }
}
