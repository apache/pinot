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
  private final AtomicBoolean _onBecomeLeaderCalled = new AtomicBoolean();
  private final AtomicBoolean _onBecomeNonLeaderCalled = new AtomicBoolean();
  private final AtomicBoolean _processCalled = new AtomicBoolean();

  private final MockControllerPeriodicTask _task =
      new MockControllerPeriodicTask("TestTask", RUN_FREQUENCY_IN_SECONDS, _resourceManager) {
        @Override
        public void onBecomeLeader() {
          _onBecomeLeaderCalled.set(true);
        }

        @Override
        public void onBecomeNotLeader() {
          _onBecomeNonLeaderCalled.set(true);
        }

        @Override
        public void process(List<String> tables) {
          _processCalled.set(true);
        }

      };

  private void resetState() {
    _onBecomeLeaderCalled.set(false);
    _onBecomeNonLeaderCalled.set(false);
    _processCalled.set(false);
  }

  @Test
  public void testRandomInitialDelay() {
    assertTrue(_task.getInitialDelayInSeconds() >= ControllerPeriodicTask.MIN_INITIAL_DELAY_IN_SECONDS);
    assertTrue(_task.getInitialDelayInSeconds() < ControllerPeriodicTask.MAX_INITIAL_DELAY_IN_SECONDS);

    assertEquals(_task.getIntervalInSeconds(), RUN_FREQUENCY_IN_SECONDS);
  }

  @Test
  public void testChangeLeadership() {
    // Initial state
    resetState();
    _task.setLeader(false);
    _task.init();
    assertFalse(_onBecomeLeaderCalled.get());
    assertFalse(_onBecomeNonLeaderCalled.get());
    assertFalse(_processCalled.get());

    // From non-leader to non-leader
    resetState();
    _task.run();
    assertFalse(_onBecomeLeaderCalled.get());
    assertFalse(_onBecomeNonLeaderCalled.get());
    assertFalse(_processCalled.get());

    // From non-leader to leader
    resetState();
    _task.setLeader(true);
    _task.run();
    assertTrue(_onBecomeLeaderCalled.get());
    assertFalse(_onBecomeNonLeaderCalled.get());
    assertTrue(_processCalled.get());

    // From leader to leader
    resetState();
    _task.run();
    assertFalse(_onBecomeLeaderCalled.get());
    assertFalse(_onBecomeNonLeaderCalled.get());
    assertTrue(_processCalled.get());

    // From leader to non-leader
    resetState();
    _task.setLeader(false);
    _task.run();
    assertFalse(_onBecomeLeaderCalled.get());
    assertTrue(_onBecomeNonLeaderCalled.get());
    assertFalse(_processCalled.get());
  }

  private class MockControllerPeriodicTask extends ControllerPeriodicTask {

    private boolean _isLeader = true;
    public MockControllerPeriodicTask(String taskName, long runFrequencyInSeconds,
        PinotHelixResourceManager pinotHelixResourceManager) {
      super(taskName, runFrequencyInSeconds, pinotHelixResourceManager);
    }

    @Override
    protected void process(List<String> tables) {

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
    protected boolean isLeader() {
      return _isLeader;
    }

    void setLeader(boolean isLeader) {
      _isLeader = isLeader;
    }
  }
}
