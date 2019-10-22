/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.apache.pinot.thirdeye.scheduler;

import java.util.Set;
import org.apache.pinot.thirdeye.datalayer.pojo.AbstractBean;
import org.quartz.JobKey;
import org.quartz.SchedulerException;


/**
 * Interface for ThirdEye's scheduling components
 */
public interface ThirdEyeScheduler extends Runnable {

  // Initialize and prepare the scheduler
  void start() throws SchedulerException;

  // Safely bring down the scheduler
  void shutdown() throws SchedulerException;

  // Trigger the scheduler to start creating jobs.
  void startJob(AbstractBean config, JobKey key) throws SchedulerException;

  // Stop the scheduler from scheduling more jobs.
  void stopJob(JobKey key) throws SchedulerException;

  // Retrieve all the scheduled jobs
  Set<JobKey> getScheduledJobs() throws SchedulerException;

  // Get the key for the scheduling job
  String getJobKey(Long id);
}
