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
package org.apache.pinot.broker.queryquota;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HitCounterTest {

  @Test
  public void testHitCounterWithOneSecondRange() {
    int timeInSec = 1;
    HitCounter hitCounter = new HitCounter(timeInSec);
    for (int i = 0; i < 10; i++) {
      hitCounter.hit(System.currentTimeMillis());
    }
    Assert.assertNotNull(hitCounter);
    Assert.assertEquals(hitCounter.getHitCount(System.currentTimeMillis()), 10);
  }

  @Test
  public void testHitCounterWithFiveSecondRange() {
    int timeInSec = 5;
    HitCounter hitCounter = new HitCounter(timeInSec);
    long currentTimestamp = System.currentTimeMillis();
    for (int i = 0; i < 3; i++) {
      hitCounter.hit(currentTimestamp);
      currentTimestamp += 1L;
    }
    // Sleep for 4 seconds. Same as: Thread.sleep(4000);
    currentTimestamp += 4000L;

    for (int i = 0; i < 7; i++) {
      hitCounter.hit(currentTimestamp);
      currentTimestamp += 1;
    }
    Assert.assertNotNull(hitCounter);
    Assert.assertEquals(hitCounter.getHitCount(currentTimestamp), 10);
  }

  @Test
  public void testHitCounterWithFiveSecondRangeSleepForMoreThanSixSeconds() {
    int timeInSec = 5;
    HitCounter hitCounter = new HitCounter(timeInSec);
    long currentTimestamp = System.currentTimeMillis();
    for (int i = 0; i < 3; i++) {
      hitCounter.hit(currentTimestamp);
      currentTimestamp += 1L;
    }
    // Sleep for more than 6 seconds. Same as: Thread.sleep(6000);
    currentTimestamp += 6000L;

    for (int i = 0; i < 7; i++) {
      hitCounter.hit(currentTimestamp);
      currentTimestamp += 1L;
    }
    Assert.assertNotNull(hitCounter);
    Assert.assertEquals(hitCounter.getHitCount(currentTimestamp), 7);
  }

  @Test
  public void testConcurrency() {
    Random random = new Random();
    // Run the test 3 times
    for (int k = 0; k < 3; k++) {
      long startTime = System.currentTimeMillis();
      int numThreads = 30;
      int numHitsPerThread = 200000;
      int expectedHitCount = numThreads * numHitsPerThread;
      // Randomly set the time range.
      int timeRangeInSecond = random.nextInt(100) + 10;
      HitCounter hitCounter = new HitCounter(timeRangeInSecond);
      List<Thread> threadList = new ArrayList<>();

      for (int i = 0; i < numThreads; i++) {
        Thread thread = new Thread(() -> {
          for (int j = 0; j < numHitsPerThread; j++) {
            hitCounter.hit(System.currentTimeMillis());
          }
        });
        thread.start();
        threadList.add(thread);
      }

      for (Thread thread : threadList) {
        Uninterruptibles.joinUninterruptibly(thread);
      }

      Assert.assertNotNull(hitCounter);
      Assert.assertEquals(hitCounter.getHitCount(), expectedHitCount);

      long duration = System.currentTimeMillis() - startTime;
      System.out.println(duration);
    }
  }
}
