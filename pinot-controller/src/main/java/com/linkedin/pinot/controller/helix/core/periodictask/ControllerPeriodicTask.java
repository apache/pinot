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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The base periodic task for pinot controller only. It uses <code>PinotHelixResourceManager</code> to determine
 * which table resources should be managed by this Pinot controller.
 */
public abstract class ControllerPeriodicTask extends BasePeriodicTask {
  public static final Logger LOGGER = LoggerFactory.getLogger(ControllerPeriodicTask.class);
  protected final PinotHelixResourceManager _pinotHelixResourceManager;
  private boolean _amILeader;
  private static final int DEFAULT_INITIAL_DELAY_IN_SECOND = 120;

  public ControllerPeriodicTask(String taskName, long runFrequencyInSeconds,
      PinotHelixResourceManager pinotHelixResourceManager) {
    this(taskName, runFrequencyInSeconds, DEFAULT_INITIAL_DELAY_IN_SECOND, pinotHelixResourceManager);
  }

  public ControllerPeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds,
      PinotHelixResourceManager pinotHelixResourceManager) {
    super(taskName, runFrequencyInSeconds, initialDelayInSeconds);
    _pinotHelixResourceManager = pinotHelixResourceManager;
    setAmILeader(false);
  }

  @Override
  public void init() {
  }

  @Override
  public void run() {
    if (!_pinotHelixResourceManager.isLeader()) {
      skipLeaderTask();
    } else {
      List<String> allTableNames = _pinotHelixResourceManager.getAllTables();
      processLeaderTask(allTableNames);
    }
  }

  private void skipLeaderTask() {
    if (getAmILeader()) {
      LOGGER.info("Current pinot controller lost leadership.");
      onBecomeNotLeader();
    }
    setAmILeader(false);
    LOGGER.info("Skip running periodic task: {} on non-leader controller", getTaskName());
  }

  private void processLeaderTask(List<String> allTableNames) {
    if (!getAmILeader()) {
      LOGGER.info("Current pinot controller became leader. Starting {} with running frequency of {} seconds.",
          getTaskName(), getIntervalInSeconds());
      onBecomeLeader();
    }
    setAmILeader(true);
    long startTime = System.currentTimeMillis();
    LOGGER.info("Starting to process {} tables in periodic task: {}", allTableNames.size(), getTaskName());
    process(allTableNames);
    LOGGER.info("Finished processing {} tables in periodic task: {} in {}ms", allTableNames.size(), getTaskName(),
        (System.currentTimeMillis() - startTime));
  }

  @VisibleForTesting
  public boolean getAmILeader() {
    return _amILeader;
  }

  @VisibleForTesting
  public void setAmILeader(boolean amILeader) {
    _amILeader = amILeader;
  }

  /**
   * Does the following logic when losing the leadership. This should be done only once during leadership transition.
   */
  public void onBecomeNotLeader() {
  }

  /**
   * Does the following logic when becoming lead controller. This should be done only once during leadership transition.
   */
  public void onBecomeLeader() {
  }

  /**
   * Processes the periodic task as lead controller.
   * @param allTableNames List of all the table names
   */
  public abstract void process(List<String> allTableNames);
}
