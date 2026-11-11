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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.utils.ServiceStatus;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Deterministic coverage for the startup-warmup readiness gate's STARTING -> GOOD transition. Observing a
/// live broker mid-warmup is inherently racy (warmup against a healthy cluster completes in well under a
/// poll interval), so the gate callback is tested directly against a flag that stands in for `_isWarm` --
/// the same flag the runtime callback closes over.
public class BrokerWarmupGateTest {

  @Test
  public void gateReportsStartingUntilWarmThenGood() {
    AtomicBoolean warm = new AtomicBoolean(false);
    ServiceStatus.ServiceStatusCallback gate = BaseBrokerStarter.warmupGateCallback(warm::get);

    // While warming: STARTING (an existing status value, not a new enum constant) with the warming-up
    // description, so the readiness probe holds the broker out of rotation and the reason is visible.
    assertEquals(gate.getServiceStatus(), ServiceStatus.Status.STARTING);
    assertEquals(gate.getStatusDescription(), BaseBrokerStarter.WARMUP_GATE_STARTING_DESCRIPTION);

    // The same callback flips the instant the flag does, exactly as _isWarm does at runtime: GOOD, no
    // description. No new callback is installed -- the gate transitions in place.
    warm.set(true);
    assertEquals(gate.getServiceStatus(), ServiceStatus.Status.GOOD);
    assertEquals(gate.getStatusDescription(), ServiceStatus.STATUS_DESCRIPTION_NONE);
  }
}
