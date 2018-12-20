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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ControllerPeriodicTaskTest {
  private static final long RUN_FREQUENCY_IN_SECONDS = 30;

  private final PinotHelixResourceManager _resourceManager = mock(PinotHelixResourceManager.class);
  private final AtomicBoolean _cleanupCalled = new AtomicBoolean();
  private final AtomicBoolean _processCalled = new AtomicBoolean();
  private final AtomicBoolean _processTableCalled = new AtomicBoolean();

  private final MockControllerPeriodicTask _task =
      new MockControllerPeriodicTask("TestTask", RUN_FREQUENCY_IN_SECONDS, _resourceManager) {

        @Override
        public void cleanup() {
          _cleanupCalled.set(true);
        }

        @Override
        public void process(List<String> tableNamesWithType) {
          _processCalled.set(true);
        }

        @Override
        public void processTable(String tableNameWithType) { _processTableCalled.set(true);}

      };

  private void resetState() {
    _cleanupCalled.set(false);
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
    assertFalse(_cleanupCalled.get());
    assertFalse(_processCalled.get());
    assertFalse(_processTableCalled.get());

    // run task
    resetState();
    _task.run();
    assertFalse(_cleanupCalled.get());
    assertTrue(_processCalled.get());
    assertFalse(_processTableCalled.get());

    // stop periodic task flag set, task will not run
    resetState();
    _task.setStopPeriodicTask(true);
    _task.run();
    assertFalse(_cleanupCalled.get());
    assertTrue(_processCalled.get());
    assertFalse(_processTableCalled.get());

    // stop periodic task
    resetState();
    _task.stop();
    assertTrue(_cleanupCalled.get());
    assertFalse(_processCalled.get());
    assertFalse(_processTableCalled.get());

  }

  private class MockControllerPeriodicTask extends ControllerPeriodicTask {

    private boolean _isStopPeriodicTask = false;
    public MockControllerPeriodicTask(String taskName, long runFrequencyInSeconds,
        PinotHelixResourceManager pinotHelixResourceManager) {
      super(taskName, runFrequencyInSeconds, pinotHelixResourceManager);
    }

    @Override
    protected void process(List<String> tableNamesWithType) {

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
    protected boolean isStopPeriodicTask() {
      return _isStopPeriodicTask;
    }

    void setStopPeriodicTask(boolean isStopPeriodicTask) {
      _isStopPeriodicTask = isStopPeriodicTask;
    }

    @Override
    public void cleanup() {

    }
  }
}
