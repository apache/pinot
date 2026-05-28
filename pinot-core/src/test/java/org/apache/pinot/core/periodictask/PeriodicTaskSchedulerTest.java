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
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PeriodicTaskSchedulerTest {

  @Test
  public void testTaskWithInvalidInterval()
      throws Exception {
    AtomicBoolean startCalled = new AtomicBoolean();
    AtomicBoolean runCalled = new AtomicBoolean();
    AtomicBoolean stopCalled = new AtomicBoolean();

    List<PeriodicTask> periodicTasks = List.of(new BasePeriodicTask("TestTask", 0L/*Invalid*/, 0L) {
      @Override
      protected void setUpTask() {
        startCalled.set(true);
      }

      @Override
      protected void runTask(Properties periodicTaskProperties) {
        runCalled.set(true);
      }

      @Override
      protected void cleanUpTask() {
        stopCalled.set(true);
      }
    });

    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);
    assertTrue(taskScheduler.hasTask("TestTask"));
    assertEquals(taskScheduler.getTaskNames(), List.of("TestTask"));

    taskScheduler.start();
    Thread.sleep(100L);
    taskScheduler.stop();

    assertTrue(startCalled.get());
    assertFalse(runCalled.get());
    assertTrue(stopCalled.get());
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "Duplicate periodic task name: TestTask")
  public void testTasksWithDuplicateName() {
    List<PeriodicTask> periodicTasks = new ArrayList<>(2);
    for (int i = 0; i < 2; i++) {
      periodicTasks.add(new BasePeriodicTask("TestTask", 1L, 0L) {
        @Override
        protected void runTask(Properties periodicTaskProperties) {
        }
      });
    }
    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);
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
      periodicTasks.add(new BasePeriodicTask("TestTask" + i, 1L, 0L) {
        @Override
        protected void setUpTask() {
          numTimesStartCalled.getAndIncrement();
        }

        @Override
        protected void runTask(Properties periodicTaskProperties) {
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
  public void testConcurrentExecutionOfSameTask()
      throws Exception {
    // Number of threads to run
    final int numThreads = 20;

    // Count number of threads that requested execution.
    final AtomicInteger attempts = new AtomicInteger();

    // Countdown latch to ensure that this test case will wait only for around half the tasks to complete.
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads / 2);

    // Create periodic task.
    PeriodicTask task = new BasePeriodicTask("TestTask", 1L, 0L) {
      private volatile boolean _isRunning = false;

      @Override
      protected void runTask(Properties periodicTaskProperties) {
        try {
          if (_isRunning) {
            // fail since task is already running in another thread.
            fail("More than one thread attempting to execute task at the same time.");
          }
          _isRunning = true;
          Thread.sleep(200);
          countDownLatch.countDown();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        } finally {
          _isRunning = false;
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
    Properties taskProperties = new Properties();
    taskProperties.put(PeriodicTask.PROPERTY_KEY_REQUEST_ID, getClass().getSimpleName());
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        attempts.incrementAndGet();
        taskScheduler.scheduleNow("TestTask", taskProperties);
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
    assertEquals(attempts.get(), numThreads);
  }

  @Test
  public void testCronScheduling() throws Exception {
    AtomicInteger numTimesRunCalled = new AtomicInteger();

    //let the frequency be 3600 seconds (1 hour) to prove that the cron job triggered the task.
    List<PeriodicTask> periodicTasks = List.of(new BasePeriodicTask("CronTask", 3600L, 3600L, "0/1 * * * * ?") {
      @Override
      protected void runTask(Properties periodicTaskProperties) {
        numTimesRunCalled.incrementAndGet();
      }
    });

    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);

    try {
      taskScheduler.start();
      Thread.sleep(1500L);
    } finally {
      taskScheduler.stop();
    }

    assertTrue(numTimesRunCalled.get() >= 1, "Task should have been triggered by Quartz CRON scheduler");
  }

  @Test
  public void testLegacyFallbackScheduling() throws Exception {
    AtomicInteger numTimesRunCalled = new AtomicInteger();
    //fallback to the default fixed delay method to prove it still works fine with code changes
    List<PeriodicTask> periodicTasks = List.of(new BasePeriodicTask("LegacyFallbackTask", 1L, 0L, null) {
      @Override
      protected void runTask(Properties periodicTaskProperties) {
        numTimesRunCalled.incrementAndGet();
      }
    });

    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);

    try {
      taskScheduler.start();
      Thread.sleep(1500L);
    } finally {
      taskScheduler.stop();
    }

    assertTrue(numTimesRunCalled.get() >= 1, "Task should have been triggered by legacy fixed-delay scheduler");
  }

  @Test
  public void testInvalidCronExpression() throws Exception {
    AtomicInteger numTimesRunCalled = new AtomicInteger();

    List<PeriodicTask> periodicTasks = List.of(new BasePeriodicTask("InvalidCronTask", 1L, 1L, "60 * * * *") {
      @Override
      protected void runTask(Properties periodicTaskProperties) {
        numTimesRunCalled.incrementAndGet();
      }
    });

    PeriodicTaskScheduler taskScheduler = new PeriodicTaskScheduler();
    taskScheduler.init(periodicTasks);

    assertThrows(IllegalArgumentException.class, taskScheduler::start);

    assertEquals(numTimesRunCalled.get(), 0, "Task should never run if the CRON expression is invalid");
  }

  /**
   * Test that the scheduler used to schedule Pinot Minion tasks (PinotTaskManager) is not
   * using the same quartz scheduler instance. It also tests that if we stop a Controller cron
   * task, Minion tasks are not stopped or interfered with.
   * @throws Exception
   */
  @Test
  public void testControllerAndMinionCronSchedulerIsolation() throws Exception {
    //quartz minion task scheduler
    Scheduler minionScheduler = StdSchedulerFactory.getDefaultScheduler();
    minionScheduler.start();

    //quartz controller task scheduler
    PeriodicTaskScheduler controllerPeriodicTaskScheduler = new PeriodicTaskScheduler();

    try {
      String minionJobName = "testMinionTableTask";
      String minionGroupName = "MinionTaskGroup";
      String controllerTaskName = "ControllerTestCronTask";

      JobDetail minionJob = JobBuilder.newJob(MockMinionJob.class)
          .withIdentity(minionJobName, minionGroupName)
          .build();
      CronTrigger minionTrigger = TriggerBuilder.newTrigger()
          .withIdentity("testMinionTrigger", minionGroupName)
          .withSchedule(CronScheduleBuilder.cronSchedule("0 0/1 * * * ?"))
          .build();
      minionScheduler.scheduleJob(minionJob, minionTrigger);

      Assert.assertTrue(minionScheduler.checkExists(minionJob.getKey()), "Minion job should be scheduled.");

      PeriodicTask controllerCronTask = new BasePeriodicTask(controllerTaskName, 0L, 0L, "0 0/5 * * * ?") {
        @Override
        protected void runTask(Properties periodicTaskProperties) {
          //mocking runtime execution logic.
        }
      };
      controllerPeriodicTaskScheduler.init(Collections.singletonList(controllerCronTask));
      controllerPeriodicTaskScheduler.start();

      Assert.assertFalse(minionScheduler.checkExists(JobKey.jobKey(controllerTaskName)),
          "Regression Failure: The minion scheduler should not be "
              + "able to see the controller's task namespace!");

      int totalMinionJobs = minionScheduler.getJobKeys(GroupMatcher.anyJobGroup()).size();
      Assert.assertEquals(totalMinionJobs, 1,
          "The broad matcher scan on the minion scheduler should "
              + "find exactly 1 minion job, ignoring controller tasks.");

      controllerPeriodicTaskScheduler.stop();

      Assert.assertTrue(minionScheduler.isStarted(),
          "Regression Failure: Stopping the controller scheduler accidentally tore down the minion scheduler!");
      Assert.assertFalse(minionScheduler.isShutdown(),
          "The minion scheduler must remain active.");
      Assert.assertTrue(minionScheduler.checkExists(minionJob.getKey()),
          "The scheduled minion tasks should still exist in the default scheduler runtime.");
    } finally {
      if (!minionScheduler.isShutdown()) {
        minionScheduler.shutdown(true);
      }
      controllerPeriodicTaskScheduler.stop();
    }
  }

  /**
   * Dummy job implementation needed to fulfill Quartz's verification layer.
   */
  public static class MockMinionJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {
    }
  }
}
