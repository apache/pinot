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
package org.apache.pinot.broker.broker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class BrokerDrainManagerTest {
  @Test
  public void testDrainWaitsForAcceptedQueriesBeforeShutdown()
      throws Exception {
    AtomicInteger drainStartedCount = new AtomicInteger();
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    BrokerDrainManager drainManager =
        BrokerDrainManager.localOnly("Broker_localhost_8099", drainStartedCount::incrementAndGet,
            shutdownLatch::countDown, 10_000L);

    BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery();
    assertNotNull(queryPermit);
    assertEquals(drainManager.getStatus().getInFlightQueries(), 1);

    BrokerDrainManager.DrainStatus timedOutStatus = drainManager.drain(10L, true);
    assertFalse(timedOutStatus.isDrained());
    assertFalse(timedOutStatus.isShutdownTriggered());
    assertEquals(drainStartedCount.get(), 1);
    assertNull(drainManager.tryAcquireQuery());

    queryPermit.close();
    BrokerDrainManager.DrainStatus drainedStatus = drainManager.drain(10_000L, true);
    assertTrue(drainedStatus.isDrained());
    assertTrue(shutdownLatch.await(10, TimeUnit.SECONDS));
    assertTrue(drainManager.getStatus().isShutdownTriggered());
    assertEquals(drainStartedCount.get(), 1);
  }

  @Test
  public void testDrainUsesDefaultTimeout()
      throws Exception {
    BrokerDrainManager drainManager = BrokerDrainManager.localOnly("Broker_localhost_8099", () -> {
    }, () -> {
    }, 1L);

    BrokerDrainManager.QueryPermit queryPermit = drainManager.tryAcquireQuery();
    assertNotNull(queryPermit);
    try {
      BrokerDrainManager.DrainStatus status = drainManager.drain(-1L, false);
      assertFalse(status.isDrained());
      assertTrue(status.isDraining());
      assertEquals(status.getInFlightQueries(), 1);
    } finally {
      queryPermit.close();
    }
  }
}
