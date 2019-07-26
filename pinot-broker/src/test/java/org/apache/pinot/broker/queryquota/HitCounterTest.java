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

  private static final int BUCKETS_100 = 100;

  @Test
  public void testHitCounterWithOneSecondRange() {
    int timeInSec = 1;
    HitCounter hitCounter = new HitCounter(timeInSec, BUCKETS_100);
    for (int i = 0; i < 10; i++) {
      hitCounter.hit(System.currentTimeMillis());
    }
    Assert.assertNotNull(hitCounter);
    Assert.assertEquals(hitCounter.getHitCount(System.currentTimeMillis()), 10);
  }

  @Test
  public void testHitCounterWithFiveSecondRange() {
    int timeInSec = 5;
    HitCounter hitCounter = new HitCounter(timeInSec, BUCKETS_100);
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
    HitCounter hitCounter = new HitCounter(timeInSec, BUCKETS_100);
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
      HitCounter hitCounter = new HitCounter(timeRangeInSecond, BUCKETS_100);
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

  @Test
  public void testPeakQPS() {
    // 6 buckets of 10000ms each covering a range of 60secs
    final HitCounter hitCounter = new HitCounter(60, 6);

    // use custom start time (in millis)
    long startTime = 1000000L;
    for (int i = 0; i < 600; i++) {
      // all 600 queries fired at an interval of 1ms each
      // all go to same bucket
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [0, 0, 0, 0, 100, 0]
    // hit counter array: [0, 0, 0, 0, 600, 0]

    startTime = 1010000L;
    for (int i = 0; i < 500; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [0, 0, 0, 0, 100, 101]
    // hit counter array: [0, 0, 0, 0, 600, 500]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 600);

    startTime = 1010500L;
    for (int i = 0; i < 80; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [0, 0, 0, 0, 100, 101]
    // hit counter array: [0, 0, 0, 0, 600, 580]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 600);

    startTime = 1060000L;
    for (int i = 0; i < 300; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [0, 0, 0, 0, 106, 101]
    // hit counter array: [0, 0, 0, 0, 300, 580]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 580);

    startTime = 1070000L;
    for (int i = 0; i < 315; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [0, 0, 0, 0, 106, 107]
    // hit counter array: [0, 0, 0, 0, 300, 315]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 315);

    startTime = 1070315L;
    for (int i = 0; i < 120; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [0, 0, 0, 0, 106, 107]
    // hit counter array: [0, 0, 0, 0, 300, 435]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 435);

    startTime = 1080000L;
    for (int i = 0; i < 450; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [108, 0, 0, 0, 106, 107]
    // hit counter array: [450, 0, 0, 0, 300, 435]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 450);

    startTime = 1120000L;
    for (int i = 0; i < 220; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [108, 0, 0, 0, 112, 107]
    // hit counter array: [450, 0, 0, 0, 220, 435]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 450);

    startTime = 1170000L;
    for (int i = 0; i < 219; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [108, 0, 0, 117, 112, 107]
    // hit counter array: [450, 0, 0, 219, 220, 435]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 220);

    startTime = 1180000L;
    for (int i = 0; i < 105; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [108, 0, 0, 117, 118, 107]
    // hit counter array: [450, 0, 0, 219, 105, 435]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 219);

    startTime = 1180105L;
    for (int i = 0; i < 140; i++) {
      hitCounter.hitAndUpdateLatestTime(startTime++);
    }

    // bucket start array: [108, 0, 0, 117, 118, 107]
    // hit counter array: [450, 0, 0, 219, 245, 435]

    Assert.assertEquals(hitCounter.getMaxHitCount(), 245);
  }
}
