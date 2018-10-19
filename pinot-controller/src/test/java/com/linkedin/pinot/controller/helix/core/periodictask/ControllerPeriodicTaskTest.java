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
import com.linkedin.pinot.core.periodictask.PeriodicTask;
import com.linkedin.pinot.core.periodictask.PeriodicTaskScheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class ControllerPeriodicTaskTest {
  private PinotHelixResourceManager helixResourceManager;
  private AtomicInteger numOfProcessingMessages;

  @BeforeTest
  public void setUp() {
    numOfProcessingMessages = new AtomicInteger(0);
    helixResourceManager = mock(PinotHelixResourceManager.class);
    List<String> allTableNames = new ArrayList<>();
    allTableNames.add("testTable_REALTIME");
    allTableNames.add("testTable_OFFLINE");
    when(helixResourceManager.getAllTables()).thenReturn(allTableNames);
  }

  @Test
  public void testWhenControllerIsLeader() throws InterruptedException {
    long totalRunTimeInMilliseconds = 3_500L;
    long runFrequencyInSeconds = 1L;
    long initialDelayInSeconds = 1L;
    when(helixResourceManager.isLeader()).thenReturn(true);

    PeriodicTask periodicTask = createMockPeriodicTask(runFrequencyInSeconds, initialDelayInSeconds);

    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler();
    periodicTaskScheduler.start(Collections.singletonList(periodicTask));
    Thread.sleep(totalRunTimeInMilliseconds);
    periodicTaskScheduler.stop();
    Assert.assertEquals(totalRunTimeInMilliseconds / 1000L, numOfProcessingMessages.get());
  }

  @Test
  public void testWhenControllerIsNotLeader() throws InterruptedException {
    long totalRunTimeInMilliseconds = 3_500L;
    long runFrequencyInSeconds = 1L;
    long initialDelayInSeconds = 1L;

    when(helixResourceManager.isLeader()).thenReturn(false);
    PeriodicTask periodicTask = createMockPeriodicTask(runFrequencyInSeconds, initialDelayInSeconds);

    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler();
    periodicTaskScheduler.start(Collections.singletonList(periodicTask));
    Thread.sleep(totalRunTimeInMilliseconds);
    periodicTaskScheduler.stop();
    Assert.assertEquals(0, numOfProcessingMessages.get());
  }

  private PeriodicTask createMockPeriodicTask(long runFrequencyInSeconds, long initialDelayInSeconds) {
    return new ControllerPeriodicTask("Task", runFrequencyInSeconds, initialDelayInSeconds, helixResourceManager) {
      public void init() {
        numOfProcessingMessages.set(0);
      }

      @Override
      public void process(List<String> allTableNames) {
        numOfProcessingMessages.incrementAndGet();
      }
    };
  }
}
