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
package org.apache.pinot.common.utils;

import java.util.concurrent.ThreadPoolExecutor;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ScalingThreadPoolExecutorTest {

  @Test
  public void testCreateThreadPerRunnable() {
    ThreadPoolExecutor executorService = (ThreadPoolExecutor) ScalingThreadPoolExecutor.newScalingThreadPool(0, 5, 500);
    assertEquals(executorService.getLargestPoolSize(), 0);
    for (int i = 0; i < 5; i++) {
      executorService.submit(getSleepingRunnable());
    }
    assertTrue(executorService.getLargestPoolSize() >= 2);
  }

  @Test
  public void testCreateThreadsUpToMax() {
    ThreadPoolExecutor executorService = (ThreadPoolExecutor) ScalingThreadPoolExecutor.newScalingThreadPool(0, 5, 500);
    for (int i = 0; i < 10; i++) {
      executorService.submit(getSleepingRunnable());
    }
    assertEquals(executorService.getLargestPoolSize(), 5);
  }

  @Test
  public void testScaleDownAfterDelay() {
    ThreadPoolExecutor executorService = (ThreadPoolExecutor) ScalingThreadPoolExecutor.newScalingThreadPool(0, 5, 500);
    for (int i = 0; i < 2; i++) {
      executorService.submit(getSleepingRunnable());
    }
    TestUtils.waitForCondition(aVoid -> executorService.getPoolSize() == 0, 2000,
        "Timed out waiting for thread pool to scale down");
  }

  private Runnable getSleepingRunnable() {
    return () -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
  }
}
