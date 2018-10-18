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

import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.periodictask.BasePeriodicTask;
import java.util.List;


/**
 * The base periodic task for pinot controller only. It uses <code>PinotHelixResourceManager</code> to determine
 * which table resources should be managed by this Pinot controller.
 */
public abstract class ControllerPeriodicTask extends BasePeriodicTask {
  protected final PinotHelixResourceManager _pinotHelixResourceManager;
  private static final int DEFAULT_INITIAL_DELAY_IN_SECOND = 120;

  public ControllerPeriodicTask(String taskName, long runFrequencyInSeconds,
      PinotHelixResourceManager pinotHelixResourceManager) {
    this(taskName, runFrequencyInSeconds, DEFAULT_INITIAL_DELAY_IN_SECOND, pinotHelixResourceManager);
  }

  public ControllerPeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds,
      PinotHelixResourceManager pinotHelixResourceManager) {
    super(taskName, runFrequencyInSeconds, initialDelayInSeconds);
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public void init() {
  }

  @Override
  public void run() {
    if (!_pinotHelixResourceManager.isLeader()) {
      nonLeaderCleanUp();
      return;
    }
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();
    process(allTableNames);
  }

  /**
   * Does the following logic when not being a lead controller.
   */
  public abstract void nonLeaderCleanUp();

  /**
   * Processes the periodic task as lead controller.
   * @param allTableNames List of all the table names
   */
  public abstract void process(List<String> allTableNames);
}
