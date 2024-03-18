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
package org.apache.pinot.controller.helix.core.periodictask;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.periodictask.BasePeriodicTask;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The base periodic task for pinot controller only. It uses <code>PinotHelixResourceManager</code> to determine
 * which table resources should be managed by this Pinot controller.
 *
 * @param <C> the context type
 */
@ThreadSafe
public abstract class ControllerPeriodicTask<C> extends BasePeriodicTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerPeriodicTask.class);

  protected final PinotHelixResourceManager _pinotHelixResourceManager;
  protected final LeadControllerManager _leadControllerManager;
  protected final ControllerMetrics _controllerMetrics;
  protected Set<String> _prevLeaderOfTables = new HashSet<>();

  public ControllerPeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds,
      PinotHelixResourceManager pinotHelixResourceManager, LeadControllerManager leadControllerManager,
      ControllerMetrics controllerMetrics) {
    super(taskName, runFrequencyInSeconds, initialDelayInSeconds);
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _leadControllerManager = leadControllerManager;
    _controllerMetrics = controllerMetrics;
  }

  @Override
  protected final void runTask(Properties periodicTaskProperties) {
    _controllerMetrics.addMeteredTableValue(_taskName, ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN, 1L);
    try {
      // Check if we have a specific table against which this task needs to be run.
      String propTableNameWithType = (String) periodicTaskProperties.get(PeriodicTask.PROPERTY_KEY_TABLE_NAME);
      // Process the tables that are managed by this controller
      List<String> allTables = propTableNameWithType == null
          ? _pinotHelixResourceManager.getAllTables()
          : Collections.singletonList(propTableNameWithType);

      Set<String> currentLeaderOfTables = allTables.stream()
          .filter(_leadControllerManager::isLeaderForTable)
          .collect(Collectors.toSet());

      if (!currentLeaderOfTables.isEmpty()) {
        processTables(new ArrayList<>(currentLeaderOfTables), periodicTaskProperties);
      }

      Set<String> nonLeaderForTables = Sets.difference(_prevLeaderOfTables, currentLeaderOfTables);
      if (!nonLeaderForTables.isEmpty()) {
        nonLeaderCleanup(new ArrayList<>(nonLeaderForTables));
      }
      _prevLeaderOfTables = currentLeaderOfTables;
    } catch (Exception e) {
      LOGGER.error("Caught exception while running task: {}", _taskName, e);
      _controllerMetrics.addMeteredTableValue(_taskName, ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR, 1L);
    }
  }

  public final ControllerMetrics getControllerMetrics() {
    return _controllerMetrics;
  }

  /**
   * Processes the given list of tables lead by the current controller, and returns the number of tables processed.
   * <p>
   * Override one of this method, {@link #processTable(String)} or {@link #processTable(String, C)}.
   * <p/>
   * Note: This method is called each time the task is executed <b>if and only if</b> the current controller is the
   * leader of at least one table. A corollary is that it won't be called every time the task is executed.
   */
  protected void processTables(List<String> tableNamesWithType, Properties periodicTaskProperties) {
    int numTables = tableNamesWithType.size();
    LOGGER.info("Processing {} tables in task: {}", numTables, _taskName);
    C context = preprocess(periodicTaskProperties);
    int numTablesProcessed = 0;
    for (String tableNameWithType : tableNamesWithType) {
      if (!isStarted()) {
        LOGGER.info("Task: {} is stopped, early terminate the task", _taskName);
        break;
      }
      try {
        processTable(tableNameWithType, context);
      } catch (Exception e) {
        LOGGER.error("Caught exception while processing table: {} in task: {}", tableNameWithType, _taskName, e);
        _controllerMetrics.addMeteredTableValue(tableNameWithType + "." + _taskName,
            ControllerMeter.PERIODIC_TASK_ERROR, 1L);
      }
      numTablesProcessed++;
    }
    postprocess(context);
    _controllerMetrics
        .setValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, _taskName, numTablesProcessed);
    LOGGER.info("Finish processing {}/{} tables in task: {}", numTablesProcessed, numTables, _taskName);
  }

  /**
   * Can be overridden to provide context before processing the tables lead by the current controller.
   */
  protected C preprocess(Properties periodicTaskProperties) {
    return null;
  }

  /**
   * Processes the given table lead by the current controller.
   * <p>
   * Override one of this method, {@link #processTable(String)} or {@link #processTables(List, Properties)}.
   */
  protected void processTable(String tableNameWithType, C context) {
    processTable(tableNameWithType);
  }

  /**
   * Processes the given table lead by the current controller.
   * <p>
   * Override one of this method, {@link #processTable(String, C)} or {@link #processTables(List, Properties)}.
   */
  protected void processTable(String tableNameWithType) {
  }

  /**
   * Can be overridden to perform cleanups after processing the tables lead by the current controller.
   */
  protected void postprocess(C context) {
    postprocess();
  }

  /**
   * Can be overridden to perform cleanups after processing the tables lead by the current controller.
   */
  protected void postprocess() {
  }

  /**
   * Can be overridden to perform cleanups for tables the current controller lost the leadership.
   * <p/>
   * Note: This method is only being called when there is at least one table in the given list. A corollary is that it
   * won't be called every time the task is executed.
   *
   * @param tableNamesWithType the table names that the current controller isn't the leader for
   */
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
  }
}
