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

package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Collection;
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
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionPipelineScheduler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipelineScheduler.class);
  public static final int DEFAULT_DETECTION_DELAY = 1;
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
        JobKey key = new JobKey(getJobKey(config.getId()), TaskConstants.TaskType.DETECTION.toString());
        if (!config.isActive()) {
          LOG.info("Detection config  " + key + " is inactive. Skipping.");
          continue;
        }

        try {
          if (scheduler.checkExists(key)) {
            LOG.info("Detection config  " + key + " is already scheduled.");
            if (jobUpdated(config, key)) {
              // restart job
              stopJob(key);
              startJob(config, key);
            }
          } else {
            startJob(config, key);
          }
        } catch (Exception e) {
          LOG.error("Error creating/updating job key {}", key);
        }
      }

      // remove
      Set<JobKey> scheduledJobs = getScheduledJobs();
      for (JobKey jobKey : scheduledJobs) {
        try {
          Long id = getIdFromJobKey(jobKey.getName());
          DetectionConfigDTO detectionDTO = detectionDAO.findById(id);
          if (detectionDTO == null) {
            LOG.info("Found a scheduled detection pipeline, but not found in the database {}", id);
            stopJob(jobKey);

          } else if (!detectionDTO.isActive()) {
            LOG.info("Found a scheduled pipeline, but has been deactivated");
            stopJob(jobKey);
          }
        } catch (Exception e) {
          LOG.error("Error removing job key {}", jobKey);
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

  public Set<JobKey> getScheduledJobs() throws SchedulerException {
    return this.scheduler.getJobKeys(GroupMatcher.jobGroupEquals(TaskConstants.TaskType.DETECTION.toString()));
  }

  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    scheduler.shutdown();
  }

  private String getJobKey(Long id) {
    return String.format("%s_%d", TaskConstants.TaskType.DETECTION, id);
  }

  private Long getIdFromJobKey(String jobKey) {
    String[] tokens = jobKey.split("_");
    String id = tokens[tokens.length - 1];
    return Long.valueOf(id);
  }
}
