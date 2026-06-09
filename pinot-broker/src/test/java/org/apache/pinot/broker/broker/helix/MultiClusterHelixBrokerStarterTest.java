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
package org.apache.pinot.broker.broker.helix;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.broker.broker.BrokerDrainManager;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;


/// Tests multi-cluster broker lifecycle coordination.
public class MultiClusterHelixBrokerStarterTest {
  private static final String INSTANCE_ID = "Broker_localhost_1234";

  @Test
  public void testDrainReadinessIsDeferredUntilMultiClusterStartupCompletes() {
    TestMultiClusterHelixBrokerStarter brokerStarter = new TestMultiClusterHelixBrokerStarter();
    brokerStarter._brokerDrainManager = new BrokerDrainManager(INSTANCE_ID, () -> null, () -> {
    }, () -> {
    }, 10_000L);

    assertFalse(brokerStarter.shouldMarkBrokerStartupReadyAfterBaseStart());
    assertThrows(BrokerDrainManager.BrokerStartupInProgressException.class,
        () -> brokerStarter._brokerDrainManager.drain(0L, false));

    brokerStarter.markBrokerStartupReady();
    assertThrows(IllegalStateException.class, () -> brokerStarter._brokerDrainManager.drain(0L, false));
  }

  @Test
  public void testStopCleansUpBaseAndRemoteComponentsOnlyOnce() {
    TestMultiClusterHelixBrokerStarter brokerStarter = new TestMultiClusterHelixBrokerStarter();

    brokerStarter.stop();
    brokerStarter.stop();

    assertEquals(brokerStarter._baseStopCount.get(), 1);
    assertEquals(brokerStarter._remoteStopCount.get(), 1);
  }

  @Test
  public void testRemoteStopCanBeRetriedAfterFailureWithoutRepeatingBaseStop() {
    FailOnceRemoteStopBrokerStarter brokerStarter = new FailOnceRemoteStopBrokerStarter();

    assertThrows(IllegalStateException.class, brokerStarter::stop);
    brokerStarter.stop();
    brokerStarter.stop();

    assertEquals(brokerStarter._baseStopCount.get(), 1);
    assertEquals(brokerStarter._remoteStopCount.get(), 2);
  }

  private static final class TestMultiClusterHelixBrokerStarter extends MultiClusterHelixBrokerStarter {
    private final AtomicInteger _baseStopCount = new AtomicInteger();
    private final AtomicInteger _remoteStopCount = new AtomicInteger();

    @Override
    void stopBrokerComponents() {
      _baseStopCount.incrementAndGet();
    }

    @Override
    protected void stopRemoteClusterComponents() {
      _remoteStopCount.incrementAndGet();
    }
  }

  private static final class FailOnceRemoteStopBrokerStarter extends MultiClusterHelixBrokerStarter {
    private final AtomicInteger _baseStopCount = new AtomicInteger();
    private final AtomicInteger _remoteStopCount = new AtomicInteger();

    @Override
    void stopBrokerComponents() {
      _baseStopCount.incrementAndGet();
    }

    @Override
    protected void stopRemoteClusterComponents() {
      if (_remoteStopCount.getAndIncrement() == 0) {
        throw new IllegalStateException("injected remote broker stop failure");
      }
    }
  }
}
