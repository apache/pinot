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
package org.apache.pinot.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.util.TaskUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TaskUtilsTest {
  @Test
  public void testGetNumTasksForQuery() {
    Assert.assertEquals(TaskUtils.getNumTasksForQuery(1, 2), 1);
    Assert.assertEquals(TaskUtils.getNumTasksForQuery(3, 2), 2);
    Assert.assertEquals(TaskUtils.getNumTasksForQuery(0, -1), 1);
    int numOps = TaskUtils.MAX_NUM_THREADS_PER_QUERY;
    // TaskUtils.MAX_NUM_THREADS_PER_QUERY at max
    Assert.assertEquals(TaskUtils.getNumTasksForQuery(numOps + 10, -1), TaskUtils.MAX_NUM_THREADS_PER_QUERY);
    // But 1 at min
    Assert.assertEquals(TaskUtils.getNumTasksForQuery(numOps - 1, -1), Math.max(1, numOps - 1));
  }

  @Test
  public void testGetNumTasks() {
    Assert.assertEquals(TaskUtils.getNumTasks(2, 3, 4), 1);
    Assert.assertEquals(TaskUtils.getNumTasks(7, 3, 4), 3);
    Assert.assertEquals(TaskUtils.getNumTasks(9, 3, 4), 3);
    Assert.assertEquals(TaskUtils.getNumTasks(10, 3, 4), 4);
    Assert.assertEquals(TaskUtils.getNumTasks(100, 3, 4), 4);
    int targetPerThread = 5;
    int numWorkUnits = TaskUtils.MAX_NUM_THREADS_PER_QUERY * targetPerThread;
    Assert.assertEquals(TaskUtils.getNumTasks(numWorkUnits + 10, targetPerThread, -1),
        TaskUtils.MAX_NUM_THREADS_PER_QUERY);
  }

  @Test
  public void testRunTasksWithDeadline() {
    ExecutorService exec = Executors.newCachedThreadPool();
    AtomicInteger sum = new AtomicInteger(0);
    TaskUtils.runTasksWithDeadline(5, index -> index, sum::addAndGet, e -> {
    }, exec, System.currentTimeMillis() + 500);
    // sum of 0, 1, .., 4 indices.
    Assert.assertEquals(sum.get(), 10);

    // Task throws exception before timeout.
    Exception[] err = new Exception[1];
    TaskUtils.runTasksWithDeadline(5, index -> {
      throw new RuntimeException("oops: " + index);
    }, sum::addAndGet, e -> err[0] = e, exec, System.currentTimeMillis() + 500);
    Assert.assertTrue(err[0].getMessage().contains("oops"));

    // Task timed out.
    TaskUtils.runTasksWithDeadline(5, index -> {
      try {
        Thread.sleep(10_000);
        return index;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }, sum::addAndGet, e -> err[0] = e, exec, System.currentTimeMillis() + 500);
    Assert.assertTrue(err[0] instanceof TimeoutException);
  }
}
