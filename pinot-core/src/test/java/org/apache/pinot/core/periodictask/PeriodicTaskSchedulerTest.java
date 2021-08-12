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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
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
      protected void runTask() {
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
        protected void runTask() {
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

  /**
   * Test that {@link PeriodicTaskScheduler} is thread safe and does not run the same task more than once at any time.
   * This is done by attempting to run the same task object in 20 different threads at the same time. While the test
   * case launches 20 threads to keep {@link PeriodicTaskScheduler} busy, it waits for only around half of them to
   * complete. The test case then checks whether the threads that did not complete execution were waiting to execute
   * (i.e they had requested execution, but had not executed yet). This "waiting" indicates that task execution was
   * being properly synchronized (otherwise all the tasks would have just run immediately). 'isRunning' variable within
   * the task is used to check that the task is not executing more than once at any given time.
   */
  @Test
  public void testConcurrentExecutionOfSameTask() throws Exception {
    // Number of threads to run
    final int numThreads = 20;

    // Count number of threads that requested execution.
    final AtomicInteger attempts = new AtomicInteger();

    // Countdown latch to ensure that this test case will wait only for around half the tasks to complete.
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads/2);

    // Create periodic task.
    PeriodicTask task = new BasePeriodicTask("TestTask", 1L, 0L) {
      private volatile boolean isRunning = false;
      @Override
      protected void runTask() {
        try {
          if (isRunning) {
            // fail since task is already running in another thread.
            Assert.fail("More than one thread attempting to execute task at the same time.");
          }
          isRunning = true;
          Thread.sleep(200);
          countDownLatch.countDown();
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
    Thread[] threads = new Thread[numThreads];
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

    // Wait for around half the threads to finish running.
    countDownLatch.await();

    // stop task scheduler.
    taskScheduler.stop();

    // Confirm that all threads requested execution, even though only half the threads completed execution.
    Assert.assertEquals(attempts.get(), numThreads);
  }
}
