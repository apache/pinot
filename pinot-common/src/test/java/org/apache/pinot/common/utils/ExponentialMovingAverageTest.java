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

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ExponentialMovingAverageTest {
  ScheduledExecutorService _executorService = Executors.newSingleThreadScheduledExecutor();

  @Test
  public void testAvgInitialization() {
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 1.0, _executorService).getAverage(), 1.0);
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 0.123, _executorService).getAverage(), 0.123);
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 88.0, _executorService).getAverage(), 88.0);
    assertEquals(new ExponentialMovingAverage(0.5, -1, 0, 0.0, _executorService).getAverage(), 0.0);
  }

  @Test
  public void testWarmUpDuration()
      throws InterruptedException {
    // Test 1. Set warmupDurationMs to 5 seconds.
    ExponentialMovingAverage average = new ExponentialMovingAverage(0.5, -1, 5000, 0.0, _executorService);
    Random rand = new Random();
    for (int ii = 0; ii < 10; ii++) {
      average.compute(rand.nextDouble());
      assertEquals(average.getAverage(), 0.0, "Iteration=" + ii);
    }
    Thread.sleep(5000);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.5);

    // Test 2
    average = new ExponentialMovingAverage(0.5, -1, 0, 0.0, _executorService);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.5);
  }

  @Test
  public void testAverage() {
    // Test 1.
    ExponentialMovingAverage average = new ExponentialMovingAverage(1.0, -1, 0, 0.0, _executorService);
    assertEquals(average.getAverage(), 0.0);
    average.compute(0.5);
    assertEquals(average.getAverage(), 0.5);
    average.compute(0.112);
    assertEquals(average.getAverage(), 0.112);
    average.compute(10.0);
    assertEquals(average.getAverage(), 10.0);

    // Test 2.
    average = new ExponentialMovingAverage(0.5, -1, 0, 0.0, _executorService);
    assertEquals(average.getAverage(), 0.0);
    average.compute(0.1);
    assertEquals(average.getAverage(), 0.05);
    average.compute(0.5);
    assertEquals(average.getAverage(), 0.275);
    average.compute(1.25);
    assertEquals(average.getAverage(), 0.7625);

    // Test 3
    average = new ExponentialMovingAverage(0.3, -1, 0, 0.0, _executorService);
    assertEquals(average.getAverage(), 0.0);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.3);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.51);
    average.compute(1.0);
    assertEquals(average.getAverage(), 0.657);
  }

  @Test
  public void testAutoDecay()
      throws InterruptedException {
    // Test 1: Test decay
    ExponentialMovingAverage average = new ExponentialMovingAverage(0.3, 10, 0, 0.0, _executorService);
    average.compute(10.0);
    double currAvg = average.getAverage();

    for (int ii = 0; ii < 10; ii++) {
      Thread.sleep(100);
      assertTrue(average.getAverage() < currAvg);
    }

    // Test 2: Test no decay
    average = new ExponentialMovingAverage(1.0, -1, 0, 0, _executorService);
    average.compute(10.0);
    currAvg = average.getAverage();

    for (int jj = 0; jj < 10; jj++) {
      Thread.sleep(100);
      assertEquals(average.getAverage(), currAvg);
    }
  }
}
