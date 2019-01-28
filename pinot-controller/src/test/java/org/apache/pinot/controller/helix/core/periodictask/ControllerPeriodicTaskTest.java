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

import com.yammer.metrics.core.MetricsRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ControllerPeriodicTaskTest {
  private static final long RUN_FREQUENCY_IN_SECONDS = 30;
  private final ControllerConf _controllerConf = new ControllerConf();

  private final PinotHelixResourceManager _resourceManager = mock(PinotHelixResourceManager.class);
  private final ControllerMetrics _controllerMetrics = new ControllerMetrics(new MetricsRegistry());
  private final AtomicBoolean _stopTaskCalled = new AtomicBoolean();
  private final AtomicBoolean _initTaskCalled = new AtomicBoolean();
  private final AtomicBoolean _processCalled = new AtomicBoolean();
  private final AtomicInteger _tablesProcessed = new AtomicInteger();
  private final int _numTables = 3;
  private static final String TASK_NAME = "TestTask";

  private final MockControllerPeriodicTask _task = new MockControllerPeriodicTask(TASK_NAME, RUN_FREQUENCY_IN_SECONDS,
      _controllerConf.getPeriodicTaskInitialDelayInSeconds(), _resourceManager, _controllerMetrics) {

    @Override
    protected void initTask() {
      _initTaskCalled.set(true);
    }

    @Override
    public void stopTask() {
      _stopTaskCalled.set(true);
    }

    @Override
    public void process(List<String> tableNamesWithType) {
      _processCalled.set(true);
      super.process(tableNamesWithType);
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
  }

  private void resetState() {
    _initTaskCalled.set(false);
    _stopTaskCalled.set(false);
    _processCalled.set(false);
    _tablesProcessed.set(0);
    _controllerMetrics.setValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME,0);
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
    // Initial state
    resetState();
    _task.init();
    assertTrue(_initTaskCalled.get());
    assertFalse(_processCalled.get());
    assertEquals(_tablesProcessed.get(), 0);
    assertFalse(_stopTaskCalled.get());
    assertFalse(_task.shouldStopPeriodicTask());
    assertEquals(_controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME),
        0);

    // run task - leadership gained
    resetState();
    _task.run();
    assertFalse(_initTaskCalled.get());
    assertTrue(_processCalled.get());
    assertEquals(_tablesProcessed.get(), _numTables);
    assertEquals(_controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME),
        _numTables);
    assertFalse(_stopTaskCalled.get());
    assertFalse(_task.shouldStopPeriodicTask());

    // stop periodic task - leadership lost
    resetState();
    _task.stop();
    assertFalse(_initTaskCalled.get());
    assertFalse(_processCalled.get());
    assertEquals(_tablesProcessed.get(), 0);
    assertEquals(_controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME),
        0);
    assertTrue(_stopTaskCalled.get());
    assertTrue(_task.shouldStopPeriodicTask());

    // call to run after periodic task stop invoked - leadership gained back on same controller
    resetState();
    _task.run();
    assertFalse(_task.shouldStopPeriodicTask());
    assertFalse(_initTaskCalled.get());
    assertTrue(_processCalled.get());
    assertEquals(_tablesProcessed.get(), _numTables);
    assertEquals(_controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, TASK_NAME),
        _numTables);
    assertFalse(_stopTaskCalled.get());

  }

  private class MockControllerPeriodicTask extends ControllerPeriodicTask {

    public MockControllerPeriodicTask(String taskName, long runFrequencyInSeconds, long initialDelayInSeconds,
        PinotHelixResourceManager pinotHelixResourceManager, ControllerMetrics controllerMetrics) {
      super(taskName, runFrequencyInSeconds, initialDelayInSeconds, pinotHelixResourceManager, controllerMetrics);
    }

    @Override
    protected void initTask() {

    }

    @Override
    protected void preprocess() {
    }

    @Override
    protected void processTable(String tableNameWithType) {

    }

    @Override
    public void postprocess() {
    }

    @Override
    public void exceptionHandler(String tableNameWithType, Exception e) {

    }

    @Override
    public void stopTask() {

    }
  }
}
