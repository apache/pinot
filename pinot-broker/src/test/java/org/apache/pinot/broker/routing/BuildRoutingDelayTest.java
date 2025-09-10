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
package org.apache.pinot.broker.routing;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BuildRoutingDelayTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testBuildRoutingSkipsWhenRequestIsOlderThanLastStart()
      throws Exception {
    // Construct with nulls as the build should return early before using these fields
    BrokerRoutingManager manager = new BrokerRoutingManager(null, null, new PinotConfiguration());

    String tableNameWithType = "testTable_OFFLINE";

    // Set a future last build start time to force skipping the current build call
    long futureStart = System.currentTimeMillis() + 10_000L;

    Field startTimesField = BrokerRoutingManager.class.getDeclaredField("_routingTableBuildStartTimeMs");
    startTimesField.setAccessible(true);
    Map<String, Long> startTimes = (Map<String, Long>) startTimesField.get(manager);
    if (startTimes == null) {
      startTimes = new ConcurrentHashMap<>();
      startTimesField.set(manager, startTimes);
    }
    startTimes.put(tableNameWithType, futureStart);

    // Should return without throwing and without attempting to build routing
    manager.buildRouting(tableNameWithType);

    // Ensure routing was not created and the last start time was not overwritten
    Assert.assertFalse(manager.routingExists(tableNameWithType));
    Assert.assertEquals(startTimes.get(tableNameWithType).longValue(), futureStart);
  }
}
