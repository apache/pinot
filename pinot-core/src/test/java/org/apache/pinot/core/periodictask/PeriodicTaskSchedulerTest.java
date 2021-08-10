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
package org.apache.pinot.core.periodictask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class PeriodicTaskSchedulerTest {

  @Test
  public void testTaskWithInvalidInterval()
      throws Exception {
    AtomicBoolean startCalled = new AtomicBoolean();
    AtomicBoolean runCalled = new AtomicBoolean();
    AtomicBoolean stopCalled = new AtomicBoolean();

    List<PeriodicTask> periodicTasks = Collections.singletonList(new BasePeriodicTask("TestTask", 0L/*Invalid*/, 0L) {
      @Override
      protected void setUpTask() {
        startCalled.set(true);
      }

      @Override
      protected void runTask(List<String> filters) {
        runCalled.set(true);
      }

      @Override
      protected void cleanUpTask() {
        stopCalled.set(true);
      }
    });

    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);
    taskScheduler.start();
    Thread.sleep(100L);
    taskScheduler.stop();

    assertFalse(startCalled.get());
    assertFalse(runCalled.get());
    assertFalse(stopCalled.get());
  }

  @Test
  public void testScheduleMultipleTasks()
      throws Exception {
    int numTasks = 3;
    AtomicInteger numTimesStartCalled = new AtomicInteger();
    AtomicInteger numTimesRunCalled = new AtomicInteger();
    AtomicInteger numTimesStopCalled = new AtomicInteger();

    List<PeriodicTask> periodicTasks = new ArrayList<>(numTasks);
    for (int i = 0; i < numTasks; i++) {
      periodicTasks.add(new BasePeriodicTask("TestTask", 1L, 0L) {
        @Override
        protected void setUpTask() {
          numTimesStartCalled.getAndIncrement();
        }

        @Override
        protected void runTask(List<String> filters) {
          numTimesRunCalled.getAndIncrement();
        }

        @Override
        protected void cleanUpTask() {
          numTimesStopCalled.getAndIncrement();
        }
      });
    }

    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);
    taskScheduler.start();
    Thread.sleep(1100L);
    taskScheduler.stop();

    assertEquals(numTimesStartCalled.get(), numTasks);
    assertEquals(numTimesRunCalled.get(), numTasks * 2);
    assertEquals(numTimesStopCalled.get(), numTasks);
  }


  /** Test that {@link PeriodicTaskScheduler} does not run the same task more than once at any time. */
  @Test
  public void testConcurrentExecutionOfSameTask() throws Exception {
    // Count how many tasks were run.
    final AtomicInteger counter = new AtomicInteger();

    // Count how many attempts were made to run task
    final AtomicInteger attempts = new AtomicInteger();


    // Create periodic task.
    PeriodicTask task = new BasePeriodicTask("TestTask", 1L, 0L) {
      private volatile boolean isRunning = false;
      @Override
      protected void runTask(List<String> filters) {
        try {
          if (isRunning) {
            Assert.fail("More than one thread attempting to execute task at the same time.");
          }
          isRunning = true;
          counter.incrementAndGet();
          Thread.sleep(250);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        } finally {
          isRunning = false;
        }
      }
    };

    // Start scheduler with periodic task.
    List<PeriodicTask> periodicTasks = new ArrayList<>();
    periodicTasks.add(task);

    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);
    taskScheduler.start();

    // Create multiple "execute" threads that try to run the same task that is already being run by scheduler
    // on a periodic basis.
    final int threadCount = 20;
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
          attempts.incrementAndGet();
          taskScheduler.scheduleNow("TestTask", null);
      });

      threads[i].start();
      try {
        threads[i].join();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }

    //  Run for 3 seconds to let as many "execute" threads finish as possible.
    Thread.sleep(3000);

    // Wait for scheduler to stop preset periodic task from running.
    taskScheduler.stop();
    Thread.sleep(500);

    // Confirm that all the "execute" threads ran.
    Assert.assertEquals(attempts.get(), threadCount);

    // Confirm that only some of the "execute" threads could run their task.
    Assert.assertTrue(counter.get() > 0);

    // A named task can exuecte only once at any given time hence it won't be possible for all threads to execute the
    // same task in the available time.
    Assert.assertTrue(attempts.get() > counter.get());
  }
}
