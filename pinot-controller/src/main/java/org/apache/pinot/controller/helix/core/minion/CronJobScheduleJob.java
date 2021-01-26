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
    String table = jobExecutionContext.getJobDetail().getKey().getName();
    String taskType = jobExecutionContext.getJobDetail().getKey().getGroup();
    if (leadControllerManager.isLeaderForTable(table)) {
      LOGGER.info("Execute CronJob: table - {}, task - {} at {}", table, taskType, jobExecutionContext.getFireTime());
      pinotTaskManager.scheduleTask(taskType, table);
      LOGGER.info("Finished CronJob: table - {}, task - {}, next runtime is {}", table, taskType,
          jobExecutionContext.getNextFireTime());
    } else {
      LOGGER.info("Not Lead, skip processing CronJob: table - {}, task - {}", table, taskType);
    }
  }
}
