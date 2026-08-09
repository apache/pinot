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
package org.apache.pinot.server.starter.helix;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class BrokerRoutingReadyCheckerTest {
  private static final String SERVER_INSTANCE = "Server_localhost_8098";

  @Test
  public void testWaitsForOnlineBrokers() {
    AtomicReference<Set<String>> onlineBrokers = new AtomicReference<>(Set.of());
    BrokerRoutingReadyChecker checker =
        new BrokerRoutingReadyChecker(SERVER_INSTANCE, onlineBrokers::get, brokers -> true);

    checker.check();
    assertFalse(checker.isReady());

    onlineBrokers.set(Set.of("Broker_localhost_8099"));
    checker.check();
    assertTrue(checker.isReady());
  }

  @Test
  public void testRetriesUntilAllBrokersConfirm() {
    AtomicInteger attempts = new AtomicInteger();
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> Set.of("Broker_localhost_8099", "Broker_localhost_8100"),
        brokers -> attempts.incrementAndGet() > 1);

    checker.check();
    assertFalse(checker.isReady());

    checker.check();
    assertTrue(checker.isReady());
  }

  @Test
  public void testBrokerMembershipMustRemainStable() {
    AtomicInteger reads = new AtomicInteger();
    BrokerRoutingReadyChecker checker = new BrokerRoutingReadyChecker(SERVER_INSTANCE,
        () -> reads.incrementAndGet() == 1 ? Set.of("Broker_localhost_8099")
            : Set.of("Broker_localhost_8099", "Broker_localhost_8100"),
        brokers -> true);

    checker.check();
    assertFalse(checker.isReady());
  }
}
