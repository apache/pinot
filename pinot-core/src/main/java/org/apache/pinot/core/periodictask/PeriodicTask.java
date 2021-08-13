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

import java.util.Properties;
import javax.annotation.concurrent.ThreadSafe;


/**
 * An interface to describe the functionality of periodic task. Periodic tasks will be added to a list, scheduled
 * and run in the periodic task scheduler with the fixed interval time.
 */
@ThreadSafe
public interface PeriodicTask extends Runnable {

  // PeriodicTask objects may take a {@link Properties} object. Define all the keys property keys here.
  String PROPERTY_KEY_REQUEST_ID = "requestid";
  String PROPERTY_KEY_TABLE_NAME = "tablename";

  /**
   * Returns the periodic task name.
   * @return task name.
   */
  String getTaskName();

  /**
   * Returns the interval time of running the same task.
   * @return the interval time in seconds.
   */
  long getIntervalInSeconds();

  /**
   * Returns the initial delay of the fist run.
   * @return initial delay in seconds.
   */
  long getInitialDelayInSeconds();

  /**
   * Performs necessary setups and starts the periodic task. Should be called before scheduling the periodic task. Can
   * be called after calling {@link #stop()} to restart the periodic task.
   */
  void start();

  /**
   * Executes the task. This method should be called only after {@link #start()} getting called but before
   * {@link #stop()} getting called.
   */
  @Override
  void run();

  /**
   * Execute the task once. This method will calls the {@link #run} method.
   * @param periodicTaskProperties Properties used by {@link PeriodicTask} during execution.
   */
  void run(Properties periodicTaskProperties);

  /**
   * Stops the periodic task and performs necessary cleanups. Should be called after removing the periodic task from the
   * scheduler. Should be called after {@link #start()} getting called.
   */
  void stop();
}
