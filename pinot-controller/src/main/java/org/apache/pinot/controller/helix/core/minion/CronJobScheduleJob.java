/**
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
package org.apache.pinot.controller.helix.core.minion;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.controller.LeadControllerManager;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CronJobScheduleJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(CronJobScheduleJob.class);

  public CronJobScheduleJob() {
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext)
      throws JobExecutionException {
    PinotTaskManager pinotTaskManager = (PinotTaskManager) jobExecutionContext.getJobDetail().getJobDataMap()
        .get(PinotTaskManager.PINOT_TASK_MANAGER_KEY);
    LeadControllerManager leadControllerManager =
        (LeadControllerManager) jobExecutionContext.getJobDetail().getJobDataMap()
            .get(PinotTaskManager.LEAD_CONTROLLER_MANAGER_KEY);
    Boolean skipLateCronSchedule =
        (Boolean) jobExecutionContext.getJobDetail().getJobDataMap().get(PinotTaskManager.SKIP_LATE_CRON_SCHEDULE);
    int maxDelayInSeconds = (Integer) jobExecutionContext.getJobDetail().getJobDataMap()
        .get(PinotTaskManager.MAX_CRON_SCHEDULE_DELAY_IN_SECONDS);
    String table = jobExecutionContext.getJobDetail().getKey().getName();
    String taskType = jobExecutionContext.getJobDetail().getKey().getGroup();
    pinotTaskManager.getControllerMetrics().addMeteredTableValue(PinotTaskManager.getCronJobName(table, taskType),
        ControllerMeter.CRON_SCHEDULER_JOB_TRIGGERED, 1L);
    if (leadControllerManager.isLeaderForTable(table)) {
      Date fireTime = jobExecutionContext.getFireTime();
      LOGGER.info("Execute CronJob: table - {}, task - {} at {}", table, taskType, fireTime);
      Date scheduledFireTime = jobExecutionContext.getScheduledFireTime();
      if (skipLateCronSchedule && isCronScheduleLate(fireTime, scheduledFireTime, maxDelayInSeconds)) {
        LOGGER.warn(
            "Skip late CronJob: table - {}, task - {} fired at {} but expected at {} with allowed delayInSeconds: {}",
            table, taskType, fireTime, scheduledFireTime, maxDelayInSeconds);
        pinotTaskManager.getControllerMetrics().addMeteredTableValue(PinotTaskManager.getCronJobName(table, taskType),
            ControllerMeter.CRON_SCHEDULER_JOB_SKIPPED, 1L);
        return;
      }
      TaskSchedulingContext context = new TaskSchedulingContext(table, taskType)
          .setTriggeredBy(PinotTaskManager.Triggers.CRON_TRIGGER.name());
      long jobStartTime = System.currentTimeMillis();
      pinotTaskManager.scheduleTasks(context);
      LOGGER.info("Finished CronJob: table - {}, task - {}, next runtime is {}", table, taskType,
          jobExecutionContext.getNextFireTime());
      pinotTaskManager.getControllerMetrics().addTimedTableValue(PinotTaskManager.getCronJobName(table, taskType),
          ControllerTimer.CRON_SCHEDULER_JOB_EXECUTION_TIME_MS, (System.currentTimeMillis() - jobStartTime),
          TimeUnit.MILLISECONDS);
    } else {
      LOGGER.info("Not Lead, skip processing CronJob: table - {}, task - {}", table, taskType);
    }
  }

  private boolean isCronScheduleLate(Date fireTime, Date scheduledFireTime, long maxDelayInSeconds) {
    return fireTime.getTime() - scheduledFireTime.getTime() > maxDelayInSeconds * 1000;
  }
}
