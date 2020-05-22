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
import java.util.HashSet;
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
import org.apache.pinot.thirdeye.detection.dataquality.DataQualityPipelineJob;
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
  public static final String QUARTZ_DETECTION_GROUPER = TaskConstants.TaskType.DETECTION.toString();

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
        if (!config.isActive()) {
          LOG.debug("Detection config " + config.getId() + " is inactive. Skipping.");
          continue;
        }
        if (config.isDataAvailabilitySchedule()) {
          LOG.debug("Detection config " + config.getId() + " is enabled for data availability scheduling. Skipping.");
          continue;
        }

        try {
          // Schedule detection jobs
          JobKey detectionJobKey = new JobKey(getJobKey(config.getId(), TaskConstants.TaskType.DETECTION),
              QUARTZ_DETECTION_GROUPER);
          JobDetail detectionJob = JobBuilder.newJob(DetectionPipelineJob.class).withIdentity(detectionJobKey).build();
          if (scheduler.checkExists(detectionJobKey)) {
            LOG.info("Detection config " + detectionJobKey.getName() + " is already scheduled for detection");
            if (isJobUpdated(config, detectionJobKey)) {
              restartJob(config, detectionJob);
            }
          } else {
            startJob(config, detectionJob);
          }

          // Schedule data quality jobs
          JobKey dataQualityJobKey = new JobKey(getJobKey(config.getId(), TaskConstants.TaskType.DATA_QUALITY),
              QUARTZ_DETECTION_GROUPER);
          JobDetail dataQualityJob = JobBuilder.newJob(DataQualityPipelineJob.class).withIdentity(dataQualityJobKey).build();
          if (scheduler.checkExists(dataQualityJobKey)) {
            LOG.info("Detection config " + dataQualityJobKey.getName() + " is already scheduled for data quality");
            if (isJobUpdated(config, dataQualityJobKey)) {
              restartJob(config, dataQualityJob);
            }
          } else {
            if (DetectionUtils.isDataQualityCheckEnabled(config)) {
              startJob(config, dataQualityJob);
            }
          }
        } catch (Exception e) {
          LOG.error("Error creating/updating job key for detection config {}", config.getId());
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

  private void restartJob(DetectionConfigDTO config, JobDetail job) throws SchedulerException {
    stopJob(job.getKey());
    startJob(config, job);
  }

  @Override
  public Set<JobKey> getScheduledJobs() throws SchedulerException {
    return this.scheduler.getJobKeys(GroupMatcher.jobGroupEquals(QUARTZ_DETECTION_GROUPER));
  }

  @Override
  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(executorService, this.getClass());
    scheduler.shutdown();
  }

  @Override
  public void startJob(AbstractBean config, JobDetail job) throws SchedulerException {
    Trigger trigger = TriggerBuilder.newTrigger().withSchedule(
        CronScheduleBuilder.cronSchedule(((DetectionConfigBean) config).getCron())).build();
    this.scheduler.scheduleJob(job, trigger);
    LOG.info(String.format("scheduled detection pipeline job %s", job.getKey().getName()));
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
  public String getJobKey(Long id, TaskConstants.TaskType taskType) {
    return String.format("%s_%d", taskType, id);
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
