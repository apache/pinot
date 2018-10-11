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
package com.linkedin.pinot.core.periodictask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PeriodicTaskSchedulerTest {

  @Test
  public void testSchedulerWithOneTask() {
    AtomicInteger count = new AtomicInteger(0);
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler(0L);
    long runFrequencyInSeconds = 1L;
    long totalRunTimeInMilliseconds = 4_000L;

    List<PeriodicTask> periodicTasks = new ArrayList<>();
    PeriodicTask task = new BasePeriodicTask("Task", runFrequencyInSeconds, 0L) {
      @Override
      public void run() {
        // Execute task.
        count.incrementAndGet();
      }
    };
    periodicTasks.add(task);

    long start = System.currentTimeMillis();
    periodicTaskScheduler.start(periodicTasks);
    try {
      Thread.sleep(totalRunTimeInMilliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    periodicTaskScheduler.stop();

    Assert.assertTrue(count.get() > 0);
    Assert.assertTrue(count.get() < (totalRunTimeInMilliseconds / runFrequencyInSeconds));
    Assert.assertTrue(totalRunTimeInMilliseconds < (System.currentTimeMillis() - start));
  }

  @Test
  public void testSchedulerWithTwoStaggeredTasks() {
    AtomicInteger count = new AtomicInteger(0);
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler(0L);
    long runFrequencyInSeconds = 4L;
    long totalRunTimeInMilliseconds = 15_000L;

    List<PeriodicTask> periodicTasks = new ArrayList<>();
    PeriodicTask task1 = new BasePeriodicTask("Task1", runFrequencyInSeconds, 0L) {
      @Override
      public void run() {
        // Execute task.
        count.incrementAndGet();
      }
    };
    periodicTasks.add(task1);

    // Stagger 2 tasks.
    PeriodicTask task2 = new BasePeriodicTask("Task2", runFrequencyInSeconds, runFrequencyInSeconds / 2) {
      @Override
      public void run() {
        // Execute task.
        count.decrementAndGet();
      }
    };
    periodicTasks.add(task2);

    long start = System.currentTimeMillis();
    periodicTaskScheduler.start(periodicTasks);
    try {
      Thread.sleep(totalRunTimeInMilliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    periodicTaskScheduler.stop();

    Assert.assertTrue(count.get() == 0);
    Assert.assertTrue(count.get() < (totalRunTimeInMilliseconds / runFrequencyInSeconds));
    Assert.assertTrue(totalRunTimeInMilliseconds < (System.currentTimeMillis() - start));
  }

  @Test
  public void testSchedulerWithTwoTasksDifferentFrequencies() {
    long startTime = System.currentTimeMillis();
    AtomicLong count = new AtomicLong(startTime);
    AtomicLong count2 = new AtomicLong(startTime);
    final long[] maxRunTimeForTask1 = {0L};
    final long[] maxRunTimeForTask2 = {0L};
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler(0L);
    long runFrequencyInSeconds = 2L;
    long totalRunTimeInMilliseconds = 20_000L;

    List<PeriodicTask> periodicTasks = new ArrayList<>();
    PeriodicTask task1 = new BasePeriodicTask("Task1", runFrequencyInSeconds, 0L) {
      @Override
      public void run() {
        // Calculate the max waiting time between the same task.
        long lastTime = count.get();
        long now = System.currentTimeMillis();
        maxRunTimeForTask1[0] = Math.max(maxRunTimeForTask1[0], (now - lastTime));
        count.set(now);
      }
    };
    periodicTasks.add(task1);

    // The time for Task 2 to run is 5 seconds, which is larger than the frequency of Task 1.
    long TimeToRun = 5_000L;
    // Frequency of Task2 is 4x the one of Task1, and it takes 5 seconds to finish each task().
    PeriodicTask task2 = new BasePeriodicTask("Task2", runFrequencyInSeconds * 4, 0L) {
      @Override
      public void run() {
        // Calculate the max waiting time between the same task.
        long lastTime = count2.get();
        long now = System.currentTimeMillis();
        maxRunTimeForTask2[0] = Math.max(maxRunTimeForTask2[0], (now - lastTime));
        count2.set(now);
        try {
          Thread.sleep(TimeToRun);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    periodicTasks.add(task2);

    long start = System.currentTimeMillis();
    periodicTaskScheduler.start(periodicTasks);
    try {
      Thread.sleep(totalRunTimeInMilliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    periodicTaskScheduler.stop();

    Assert.assertTrue(count.get() > startTime);
    Assert.assertTrue(count2.get() > startTime);
    Assert.assertTrue(totalRunTimeInMilliseconds < (System.currentTimeMillis() - start));
    // Task1 waited until Task2 finished.
    Assert.assertTrue(maxRunTimeForTask1[0] > (task1.getIntervalInSeconds() + TimeToRun));
    Assert.assertTrue(maxRunTimeForTask2[0] > task2.getIntervalInSeconds());
  }

  @Test
  public void testNoTaskAssignedToQueue() {
    AtomicInteger count = new AtomicInteger(0);
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler(2L);
    long totalRunTimeInMilliseconds = 5_000L;

    // An empty list.
    List<PeriodicTask> periodicTasks = new ArrayList<>();
    long start = System.currentTimeMillis();
    periodicTaskScheduler.start(periodicTasks);
    try {
      Thread.sleep(totalRunTimeInMilliseconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    periodicTaskScheduler.stop();

    Assert.assertTrue(count.get() == 0);
    Assert.assertTrue(totalRunTimeInMilliseconds < (System.currentTimeMillis() - start));
  }
}
