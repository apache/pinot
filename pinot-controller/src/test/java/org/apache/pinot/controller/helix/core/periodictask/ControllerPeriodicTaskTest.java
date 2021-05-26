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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ControllerPeriodicTaskTest {
  private static final long RUN_FREQUENCY_IN_SECONDS = 30;
  private final ControllerConf _controllerConf = new ControllerConf();

  private final PinotHelixResourceManager _resourceManager = mock(PinotHelixResourceManager.class);
  private final LeadControllerManager _leadControllerManager = mock(LeadControllerManager.class);
  private final ControllerMetrics _controllerMetrics =
      new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  private final AtomicBoolean _startTaskCalled = new AtomicBoolean();
  private final AtomicBoolean _stopTaskCalled = new AtomicBoolean();
  private final AtomicBoolean _processTablesCalled = new AtomicBoolean();
  private final AtomicInteger _tablesProcessed = new AtomicInteger();
  private final int _numTables = 3;
  private static final String TASK_NAME = "TestTask";

  private final ControllerPeriodicTask _task = new ControllerPeriodicTask<Void>(TASK_NAME, RUN_FREQUENCY_IN_SECONDS,
      _controllerConf.getPeriodicTaskInitialDelayInSeconds(), _resourceManager, _leadControllerManager,
      _controllerMetrics) {

    @Override
    public String getTaskDescription() {
      return "Test Description";
    }

    @Override
    protected void setUpTask() {
      _startTaskCalled.set(true);
    }

    @Override
    public void cleanUpTask() {
      _stopTaskCalled.set(true);
    }

    @Override
    public void processTables(List<String> tableNamesWithType) {
      _processTablesCalled.set(true);
      super.processTables(tableNamesWithType);
    }

    @Override
    public void processTable(String tableNameWithType) {
      _tablesProcessed.getAndIncrement();
    }
  };

  @BeforeTest
  public void beforeTest() {
    List<String> tables = new ArrayList<>(_numTables);
    IntStream.range(0, _numTables).forEach(i -> tables.add("table_" + i + " _OFFLINE"));
    when(_resourceManager.getAllTables()).thenReturn(tables);
    when(_leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
  }

  private void resetState() {
    _startTaskCalled.set(false);
    _stopTaskCalled.set(false);
    _processTablesCalled.set(false);
    _tablesProcessed.set(0);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME, 0);
  }

  @Test
  public void testRandomInitialDelay() {
    assertTrue(
        _task.getInitialDelayInSeconds() >= ControllerConf.ControllerPeriodicTasksConf.MIN_INITIAL_DELAY_IN_SECONDS);
    assertTrue(
        _task.getInitialDelayInSeconds() < ControllerConf.ControllerPeriodicTasksConf.MAX_INITIAL_DELAY_IN_SECONDS);

    assertEquals(_task.getIntervalInSeconds(), RUN_FREQUENCY_IN_SECONDS);
  }

  @Test
  public void testControllerPeriodicTaskCalls() {
    // Start periodic task - leadership gained
    resetState();
    _task.start();
    assertTrue(_startTaskCalled.get());
    assertFalse(_processTablesCalled.get());
    assertEquals(_tablesProcessed.get(), 0);
    assertFalse(_stopTaskCalled.get());
    assertTrue(_task.isStarted());
    assertEquals(
        _controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME), 0);

    // Run periodic task with leadership
    resetState();
    _task.run();
    assertFalse(_startTaskCalled.get());
    assertTrue(_processTablesCalled.get());
    assertEquals(_tablesProcessed.get(), _numTables);
    assertEquals(
        _controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME),
        _numTables);
    assertFalse(_stopTaskCalled.get());
    assertTrue(_task.isStarted());

    // Stop periodic task - leadership lost
    resetState();
    _task.stop();
    assertFalse(_startTaskCalled.get());
    assertFalse(_processTablesCalled.get());
    assertEquals(_tablesProcessed.get(), 0);
    assertEquals(
        _controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME), 0);
    assertTrue(_stopTaskCalled.get());
    assertFalse(_task.isStarted());

    // Run periodic task without leadership
    resetState();
    _task.run();
    assertFalse(_startTaskCalled.get());
    assertFalse(_processTablesCalled.get());
    assertEquals(_tablesProcessed.get(), 0);
    assertEquals(
        _controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME), 0);
    assertFalse(_stopTaskCalled.get());
    assertFalse(_task.isStarted());

    // Restart periodic task - leadership re-gained
    resetState();
    _task.start();
    assertTrue(_startTaskCalled.get());
    assertFalse(_processTablesCalled.get());
    assertEquals(_tablesProcessed.get(), 0);
    assertFalse(_stopTaskCalled.get());
    assertTrue(_task.isStarted());
    assertEquals(
        _controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME), 0);

    // Run periodic task with leadership
    resetState();
    _task.run();
    assertFalse(_startTaskCalled.get());
    assertTrue(_processTablesCalled.get());
    assertEquals(_tablesProcessed.get(), _numTables);
    assertEquals(
        _controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME),
        _numTables);
    assertFalse(_stopTaskCalled.get());
    assertTrue(_task.isStarted());
  }
}
