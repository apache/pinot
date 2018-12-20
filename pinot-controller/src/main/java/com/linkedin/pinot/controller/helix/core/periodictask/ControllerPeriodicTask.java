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
package com.linkedin.pinot.controller.helix.core.periodictask;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.periodictask.BasePeriodicTask;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The base periodic task for pinot controller only. It uses <code>PinotHelixResourceManager</code> to determine
 * which table resources should be managed by this Pinot controller.
 */
public abstract class ControllerPeriodicTask extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerPeriodicTask.class);
  private static final Random RANDOM = new Random();

  public static final int MIN_INITIAL_DELAY_IN_SECONDS = 120;
  public static final int MAX_INITIAL_DELAY_IN_SECONDS = 300;

  private static final long MAX_CONTROLLER_PERIODIC_TASK_STOP_TIME_MILLIS = 30_000L;

  protected final PinotHelixResourceManager _pinotHelixResourceManager;

  private volatile boolean _stopPeriodicTask = false;
  private volatile boolean _periodicTaskInProgress = false;

  public ControllerPeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds,
      PinotHelixResourceManager pinotHelixResourceManager) {
    super(taskName, runFrequencyInSeconds, initialDelayInSeconds);
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  public ControllerPeriodicTask(String taskName, long runFrequencyInSeconds,
      PinotHelixResourceManager pinotHelixResourceManager) {
    this(taskName, runFrequencyInSeconds, getRandomInitialDelayInSeconds(), pinotHelixResourceManager);
  }

  private static long getRandomInitialDelayInSeconds() {
    return MIN_INITIAL_DELAY_IN_SECONDS + RANDOM.nextInt(MAX_INITIAL_DELAY_IN_SECONDS - MIN_INITIAL_DELAY_IN_SECONDS);
  }

  @Override
  public void init() {
  }

  @Override
  public void run() {
    _periodicTaskInProgress = true;
    List<String> tableNamesWithType = _pinotHelixResourceManager.getAllTables();
    long startTime = System.currentTimeMillis();
    int numTables = tableNamesWithType.size();
    LOGGER.info("Start processing {} tables in periodic task: {}", numTables, _taskName);
    process(tableNamesWithType);
    LOGGER.info("Finish processing {} tables in periodic task: {} in {}ms", numTables, _taskName,
        (System.currentTimeMillis() - startTime));
    _periodicTaskInProgress = false;
  }


  @Override
  public void stop() {
    _stopPeriodicTask = true;

    LOGGER.info("Waiting for periodic task {} to finish, maxWaitTimeMillis = {}", _taskName,
        MAX_CONTROLLER_PERIODIC_TASK_STOP_TIME_MILLIS);
    long millisToWait = MAX_CONTROLLER_PERIODIC_TASK_STOP_TIME_MILLIS;
    while (_periodicTaskInProgress && millisToWait > 0) {
      try {
        long thisWait = 1000;
        if (millisToWait < thisWait) {
          thisWait = millisToWait;
        }
        Thread.sleep(thisWait);
        millisToWait -= thisWait;
      } catch (InterruptedException e) {
        LOGGER.info("Interrupted: Remaining wait time {} (out of {})", millisToWait,
            MAX_CONTROLLER_PERIODIC_TASK_STOP_TIME_MILLIS);
        break;
      }
    }
    LOGGER.info("Wait completed. _periodicTaskInProgress = {}", _periodicTaskInProgress);

    cleanup();
  }


  /**
   * Processes the task on the given tables.
   *
   * @param tableNamesWithType List of table names
   */
  protected void process(List<String> tableNamesWithType) {
    if (!isStopPeriodicTask()) {
      preprocess();
      for (String table : tableNamesWithType) {
        if (isStopPeriodicTask()) {
          break;
        }
        processTable(table);
      }
      postprocess();
    }
  }

  /**
   * This method runs before processing all tables
   */
  protected abstract void preprocess();

  /**
   * Execute the controller periodic task for the given table
   * @param tableNameWithType
   */
  protected abstract void processTable(String tableNameWithType);

  /**
   * This method runs after processing all tables
   */
  protected abstract void postprocess();

  @VisibleForTesting
  protected boolean isStopPeriodicTask() {
    return _stopPeriodicTask;
  }

  protected abstract void cleanup();
}
