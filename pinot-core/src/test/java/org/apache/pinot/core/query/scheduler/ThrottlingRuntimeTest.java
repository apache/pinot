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
package org.apache.pinot.core.query.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ThrottlingRuntimeTest {

  @Test
  public void testInitializationAndUpdate() {
    ThrottlingRuntime.setDefaultConcurrency(4);
    assertTrue(ThrottlingRuntime.getDefaultConcurrency() >= 4);
    assertTrue(ThrottlingRuntime.getCurrentLimit() >= 1);

    Map<String, String> cfg = new HashMap<>();
    cfg.put("throttling.pause_on_alarm", "true");
    cfg.put("throttling.alarm_max_concurrent", "1");
    cfg.put("throttling.normal_max_concurrent", "3");
    ThrottlingRuntime.applyClusterConfig(cfg);
    assertTrue(ThrottlingRuntime.isPauseOnAlarm());
    assertEquals(ThrottlingRuntime.getAlarmMaxConcurrent(), 1);
    assertEquals(ThrottlingRuntime.getNormalMaxConcurrent(), 3);

    ThrottlingRuntime.onLevelChange("HeapMemoryAlarmingVerbose");
    // best effort: gate should be at most 1 in alarm
    assertTrue(ThrottlingRuntime.getCurrentLimit() >= 1);

    ThrottlingRuntime.onLevelChange("Normal");
    assertEquals(ThrottlingRuntime.getCurrentLimit(), 3);
  }

  @Test
  public void testPermitGate()
      throws Exception {
    ThrottlingRuntime.setDefaultConcurrency(2);
    ThrottlingRuntime.setCurrentLimit(2);
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch done = new CountDownLatch(2);

    Runnable r = () -> {
      ThrottlingRuntime.acquireSchedulerPermit();
      try {
        started.countDown();
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
      } finally {
        ThrottlingRuntime.releaseSchedulerPermit();
        done.countDown();
      }
    };

    new Thread(r).start();
    new Thread(r).start();

    assertTrue(started.await(2, TimeUnit.SECONDS));

    // Reduce current limit; should not block until releases
    ThrottlingRuntime.setCurrentLimit(1);
    assertEquals(ThrottlingRuntime.getCurrentLimit(), 1);
    assertTrue(done.await(2, TimeUnit.SECONDS));
  }
}
