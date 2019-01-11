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

/**
 * An interface to describe the functionality of periodic task. Periodic tasks will be added to a list, scheduled
 * and run in the periodic task scheduler with the fixed interval time.
 */
public interface PeriodicTask extends Runnable {

  /**
   * Initialize the task before running the task.
   */
  void init();

  /**
   * Get the interval time of running the same task.
   * @return the interval time in seconds.
   */
  long getIntervalInSeconds();

  /**
   * Get the initial delay of the fist run.
   * @return initial delay in seconds.
   */
  long getInitialDelayInSeconds();

  /**
   * Get the periodic task name.
   * @return task name.
   */
  String getTaskName();

  /**
   * Stop the periodic task
   */
  void stop();
}
