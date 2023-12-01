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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.helix.HelixProperty;
import org.apache.helix.model.IdealState;
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
      List<String> tablesToProcess = new ArrayList<>();
      List<String> nonLeaderForTables = new ArrayList<>();
      if (propTableNameWithType == null) {
        // Table name is not available, so task should run on all tables for which this controller is the lead.
        for (String tableNameWithType : _pinotHelixResourceManager.getAllTables()) {
          if (_leadControllerManager.isLeaderForTable(tableNameWithType)) {
            tablesToProcess.add(tableNameWithType);
          } else {
            nonLeaderForTables.add(tableNameWithType);
          }
        }
      } else {
        // Table name is available, so task should run only on the specified table.
        if (_leadControllerManager.isLeaderForTable(propTableNameWithType)) {
          tablesToProcess.add(propTableNameWithType);
        }
      }

      if (!tablesToProcess.isEmpty()) {
        processTables(tablesToProcess, periodicTaskProperties);
      }
      if (!nonLeaderForTables.isEmpty()) {
        nonLeaderCleanup(nonLeaderForTables);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while running task: {}", _taskName, e);
      _controllerMetrics.addMeteredTableValue(_taskName, ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR, 1L);
    }
  }

  public final ControllerMetrics getControllerMetrics() {
    return _controllerMetrics;
  }

  /**
   * Processes the given list of tables, and returns the number of tables processed.
   * <p>
   * Override one of this method, {@link #processTable(String)} or {@link #processTable(String, C)}.
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
   * Can be overridden to provide context before processing the tables.
   */
  protected C preprocess(Properties periodicTaskProperties) {
    return null;
  }

  /**
   * Processes the given table.
   * <p>
   * Override one of this method, {@link #processTable(String)} or {@link #processTables(List, Properties)}.
   */
  protected void processTable(String tableNameWithType, C context) {
    processTable(tableNameWithType);
  }

  /**
   * Processes the given table.
   * <p>
   * Override one of this method, {@link #processTable(String, C)} or {@link #processTables(List, Properties)}.
   */
  protected void processTable(String tableNameWithType) {
  }

  /**
   * Can be overridden to perform cleanups after processing the tables.
   */
  protected void postprocess(C context) {
    postprocess();
  }

  /**
   * Can be overridden to perform cleanups after processing the tables.
   */
  protected void postprocess() {
  }

  /**
   * Can be overridden to perform cleanups for tables that the current controller isn't the leader.
   *
   * @param tableNamesWithType the table names that the current controller isn't the leader for
   */
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
  }
}
