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

import com.google.common.collect.Lists;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ControllerPeriodicTaskTest {
  private static final long RUN_FREQUENCY_IN_SECONDS = 30;

  private final PinotHelixResourceManager _resourceManager = mock(PinotHelixResourceManager.class);
  private final AtomicBoolean _stopTaskCalled = new AtomicBoolean();
  private final AtomicBoolean _initTaskCalled = new AtomicBoolean();
  private final AtomicBoolean _processCalled = new AtomicBoolean();
  private final AtomicBoolean _processTableCalled = new AtomicBoolean();

  private final MockControllerPeriodicTask _task =
      new MockControllerPeriodicTask("TestTask", RUN_FREQUENCY_IN_SECONDS, _resourceManager) {

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
          _processTableCalled.set(true);
        }

      };

  @BeforeTest
  public void beforeTest() {
    when(_resourceManager.getAllTables()).thenReturn(Lists.newArrayList("table1_OFFLINE", "table2_REALTIME"));
  }

  private void resetState() {
    _initTaskCalled.set(false);
    _stopTaskCalled.set(false);
    _processCalled.set(false);
    _processTableCalled.set(false);
  }

  @Test
  public void testRandomInitialDelay() {
    assertTrue(_task.getInitialDelayInSeconds() >= ControllerPeriodicTask.MIN_INITIAL_DELAY_IN_SECONDS);
    assertTrue(_task.getInitialDelayInSeconds() < ControllerPeriodicTask.MAX_INITIAL_DELAY_IN_SECONDS);

    assertEquals(_task.getIntervalInSeconds(), RUN_FREQUENCY_IN_SECONDS);
  }

  @Test
  public void testControllerPeriodicTaskCalls() {
    // Initial state
    resetState();
    _task.init();
    assertTrue(_initTaskCalled.get());
    assertFalse(_processCalled.get());
    assertFalse(_processTableCalled.get());
    assertFalse(_stopTaskCalled.get());
    assertFalse(_task.shouldStopPeriodicTask());

    // run task after init
    resetState();
    _task.run();
    assertFalse(_initTaskCalled.get());
    assertTrue(_processCalled.get());
    assertTrue(_processTableCalled.get());
    assertFalse(_stopTaskCalled.get());
    assertFalse(_task.shouldStopPeriodicTask());

    // stop periodic task
    resetState();
    _task.stop();
    assertFalse(_initTaskCalled.get());
    assertFalse(_processCalled.get());
    assertFalse(_processTableCalled.get());
    assertTrue(_stopTaskCalled.get());
    assertTrue(_task.shouldStopPeriodicTask());

    // call to run after periodic task stop invoked, table not processed
    resetState();
    _task.run();
    assertFalse(_initTaskCalled.get());
    assertTrue(_processCalled.get());
    assertFalse(_processTableCalled.get());
    assertFalse(_stopTaskCalled.get());
    assertTrue(_task.shouldStopPeriodicTask());

    // init then run
    resetState();
    _task.init();
    assertFalse(_task.shouldStopPeriodicTask());
    _task.run();
    assertTrue(_initTaskCalled.get());
    assertTrue(_processCalled.get());
    assertTrue(_processTableCalled.get());
    assertFalse(_stopTaskCalled.get());

  }

  private class MockControllerPeriodicTask extends ControllerPeriodicTask {

    public MockControllerPeriodicTask(String taskName, long runFrequencyInSeconds,
        PinotHelixResourceManager pinotHelixResourceManager) {
      super(taskName, runFrequencyInSeconds, pinotHelixResourceManager);
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
    public void stopTask() {

    }
  }
}
