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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.anomaly.utils.AnomalyUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.datalayer.pojo.AbstractBean;
import org.apache.pinot.thirdeye.datalayer.pojo.DetectionConfigBean;
import org.apache.pinot.thirdeye.detection.DetectionDataSLAJob;
import org.apache.pinot.thirdeye.detection.DetectionPipelineJob;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.TaskUtils;
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


public class DetectionCronScheduler implements ThirdEyeCronScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionCronScheduler.class);

  public static final int DEFAULT_DETECTION_DELAY = 1;
  public static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  final DetectionConfigManager detectionDAO;
  final Scheduler scheduler;
  final ScheduledExecutorService executorService;

  public DetectionCronScheduler(DetectionConfigManager detectionDAO) throws Exception {
    this.detectionDAO = detectionDAO;
    this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() throws SchedulerException {
    this.scheduler.start();
    this.executorService.scheduleWithFixedDelay(this, 0, DEFAULT_DETECTION_DELAY, DEFAULT_ALERT_DELAY_UNIT);
  }

  @Override
  public void run() {
    try {
      Collection<DetectionConfigDTO> configs = this.detectionDAO.findAll();

      // add or update
      for (DetectionConfigDTO config : configs) {
        JobKey key = new JobKey(getJobKey(config.getId()), TaskConstants.TaskType.DETECTION.toString());
        if (!config.isActive()) {
          LOG.debug("Detection config " + key + " is inactive. Skipping.");
          continue;
        }
        if (DetectionUtils.isDataAvailabilityCheckEnabled(config)) {
          LOG.debug("Detection config " + key + " is enabled for data availability scheduling. Skipping.");
          continue;
        }
        try {
          if (scheduler.checkExists(key)) {
            LOG.info("Detection config  " + key.getName() + " is already scheduled");
            if (isJobUpdated(config, key)) {
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

      Set<JobKey> scheduledJobs = getScheduledJobs();
      for (JobKey jobKey : scheduledJobs) {
        try {
          Long id = TaskUtils.getIdFromJobKey(jobKey.getName());
          DetectionConfigDTO detectionDTO = detectionDAO.findById(id);
          if (detectionDTO == null) {
            LOG.info("Found a scheduled detection config task, but not found in the database {}", id);
            stopJob(jobKey);
            continue;
          } else if (!detectionDTO.isActive()) {
            LOG.info("Found a scheduled detection config task, but has been deactivated {}", id);
            stopJob(jobKey);
            continue;
          }
          if (detectionDTO.isDataAvailabilitySchedule()) {
            LOG.info("Found a scheduled pipeline enabled for data availability scheduling.");
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

  @Override
  public Set<JobKey> getScheduledJobs() throws SchedulerException {
    return this.scheduler.getJobKeys(GroupMatcher.jobGroupEquals(TaskConstants.TaskType.DETECTION.toString()));
  }

  @Override
  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(executorService, this.getClass());
    scheduler.shutdown();
  }

  @Override
  public void startJob(AbstractBean config, JobKey key) throws SchedulerException {
    Trigger trigger = TriggerBuilder.newTrigger().withSchedule(
        CronScheduleBuilder.cronSchedule(((DetectionConfigBean) config).getCron())).build();
    JobDetail detectionJob = JobBuilder.newJob(DetectionPipelineJob.class).withIdentity(key).build();

    this.scheduler.scheduleJob(detectionJob, trigger);
    LOG.info(String.format("scheduled detection pipeline job %s", detectionJob.getKey().getName()));

    // Data SLA alerts will be scheduled only when enabled by the user.
    if (DetectionUtils.isDataAvailabilityCheckEnabled((DetectionConfigDTO) config)) {
      JobDetail dataSLAJob = JobBuilder.newJob(DetectionDataSLAJob.class).withIdentity(key).build();
      this.scheduler.scheduleJob(dataSLAJob, trigger);
      LOG.info(String.format("scheduled data sla jobs %s", dataSLAJob.getKey().getName()));
    }
  }

  @Override
  public void stopJob(JobKey jobKey) throws SchedulerException {
    if (!this.scheduler.checkExists(jobKey)) {
      throw new IllegalStateException(
          "Cannot stop detection pipeline " + jobKey.getName() + ", it has not been scheduled");
    }
    this.scheduler.deleteJob(jobKey);
    LOG.info("Stopped detection pipeline {}", jobKey.getName());
  }

  @Override
  public String getJobKey(Long id) {
    return String.format("%s_%d", TaskConstants.TaskType.DETECTION, id);
  }

  private boolean isJobUpdated(DetectionConfigDTO config, JobKey key) throws SchedulerException {
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
}
