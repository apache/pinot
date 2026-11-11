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
package org.apache.pinot.core.periodictask;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@DisallowConcurrentExecution
public class PeriodicTaskCronJob implements Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskCronJob.class);
  public static final String PERIODIC_TASK_KEY = "PeriodicTask";

  public PeriodicTaskCronJob() {
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext)
      throws JobExecutionException {
    PeriodicTask periodicTask = (PeriodicTask) jobExecutionContext
        .getJobDetail()
        .getJobDataMap()
        .get(PERIODIC_TASK_KEY);

    if (periodicTask != null) {
      try {
        periodicTask.run();
      } catch (Exception e) {
        LOGGER.warn("Caught exception while running Task: {}", periodicTask.getTaskName(), e);
      }
    }
  }
}
