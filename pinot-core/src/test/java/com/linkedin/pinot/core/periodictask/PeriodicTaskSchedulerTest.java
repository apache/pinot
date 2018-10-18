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
  public void testSchedulerWithOneTask() throws InterruptedException {
    AtomicInteger count = new AtomicInteger(0);
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler();
    long runFrequencyInSeconds = 1L;
    long initialDelayInSeconds = 1L;
    long totalRunTimeInMilliseconds = 3_500L;

    List<PeriodicTask> periodicTasks = new ArrayList<>();
    PeriodicTask task = new BasePeriodicTask("Task", runFrequencyInSeconds, initialDelayInSeconds) {
      @Override
      public void init() {
        count.set(0);
      }

      @Override
      public void run() {
        // Execute task.
        count.incrementAndGet();
      }
    };
    periodicTasks.add(task);

    long start = System.currentTimeMillis();
    periodicTaskScheduler.start(periodicTasks);
    Thread.sleep(totalRunTimeInMilliseconds);

    periodicTaskScheduler.stop();

    Assert.assertTrue(count.get() > 0);
    Assert.assertEquals(count.get(), (totalRunTimeInMilliseconds / (runFrequencyInSeconds * 1000)));
    Assert.assertTrue(totalRunTimeInMilliseconds <= (System.currentTimeMillis() - start));
  }

  @Test
  public void testSchedulerWithTwoStaggeredTasks() throws InterruptedException {
    AtomicInteger count = new AtomicInteger(0);
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler();
    long runFrequencyInSeconds = 2L;
    long totalRunTimeInMilliseconds = 1_500L;

    List<PeriodicTask> periodicTasks = new ArrayList<>();
    PeriodicTask task1 = new BasePeriodicTask("Task1", runFrequencyInSeconds, 0L) {
      @Override
      public void init() {
      }

      @Override
      public void run() {
        // Execute task.
        count.incrementAndGet();
      }
    };
    periodicTasks.add(task1);

    // Stagger 2 tasks by delaying the 2nd task half of the frequency.
    PeriodicTask task2 = new BasePeriodicTask("Task2", runFrequencyInSeconds, runFrequencyInSeconds / 2) {
      @Override
      public void init() {
      }

      @Override
      public void run() {
        // Execute task.
        count.decrementAndGet();
      }
    };
    periodicTasks.add(task2);

    long start = System.currentTimeMillis();
    periodicTaskScheduler.start(periodicTasks);
    Thread.sleep(totalRunTimeInMilliseconds);
    periodicTaskScheduler.stop();

    Assert.assertEquals(count.get(), 0);
    Assert.assertTrue(totalRunTimeInMilliseconds <= (System.currentTimeMillis() - start));
  }

  @Test
  public void testSchedulerWithTwoTasksDifferentFrequencies() throws InterruptedException {
    long startTime = System.currentTimeMillis();
    AtomicLong count = new AtomicLong(startTime);
    AtomicLong count2 = new AtomicLong(startTime);
    final long[] maxRunTimeForTask1 = {0L};
    final long[] maxRunTimeForTask2 = {0L};
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler();
    long runFrequencyInSeconds = 1L;
    long totalRunTimeInMilliseconds = 10_000L;

    List<PeriodicTask> periodicTasks = new ArrayList<>();
    PeriodicTask task1 = new BasePeriodicTask("Task1", runFrequencyInSeconds, 0L) {
      @Override
      public void init() {
      }

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

    // The time for Task 2 to run is 4 seconds, which is higher than the interval time of Task 1.
    long TimeToRunMs = 4_000L;
    PeriodicTask task2 = new BasePeriodicTask("Task2", runFrequencyInSeconds * 3, 0L) {
      @Override
      public void init() {
      }

      @Override
      public void run() {
        // Calculate the max waiting time between the same task.
        long lastTime = count2.get();
        long now = System.currentTimeMillis();
        maxRunTimeForTask2[0] = Math.max(maxRunTimeForTask2[0], (now - lastTime));
        count2.set(now);
        try {
          Thread.sleep(TimeToRunMs);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    periodicTasks.add(task2);

    periodicTaskScheduler.start(periodicTasks);
    Thread.sleep(totalRunTimeInMilliseconds);

    periodicTaskScheduler.stop();

    Assert.assertTrue(count.get() > startTime);
    Assert.assertTrue(count2.get() > startTime);
    // Task1 didn't waited until Task2 finished.
    Assert.assertTrue(maxRunTimeForTask1[0] - task1.getIntervalInSeconds() * 1000L < 100L);
    Assert.assertTrue(maxRunTimeForTask2[0] >= Math.max(task2.getIntervalInSeconds() * 1000L, TimeToRunMs));
  }

  @Test
  public void testNoTaskAssignedToQueue() throws InterruptedException {
    AtomicInteger count = new AtomicInteger(0);
    PeriodicTaskScheduler periodicTaskScheduler = new PeriodicTaskScheduler();
    long totalRunTimeInMilliseconds = 2_000L;

    // An empty list.
    List<PeriodicTask> periodicTasks = new ArrayList<>();
    long start = System.currentTimeMillis();
    periodicTaskScheduler.start(periodicTasks);
    Thread.sleep(totalRunTimeInMilliseconds);

    periodicTaskScheduler.stop();

    Assert.assertEquals(count.get(), 0);
    Assert.assertTrue(totalRunTimeInMilliseconds <= (System.currentTimeMillis() - start));
  }
}
