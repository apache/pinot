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
package org.apache.pinot.query.context;

import java.util.Map;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class PhysicalPlannerContextTest {

  @Test
  public void testGetRandomInstanceIdWithNoInstances() {
    // Test case: No instances present in context (should throw IllegalStateException)
    PhysicalPlannerContext context = createPhysicalPlannerContext();

    // Clear the instances map to simulate no instances
    context.getInstanceIdToQueryServerInstance().clear();

    try {
      context.getRandomInstanceId();
      Assert.fail("Expected IllegalStateException when no instances are present");
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "No instances present in context");
    }
  }

  @Test
  public void testGetRandomInstanceIdWithSingleInstance() {
    // Test case: Only one instance present (should return that instance)
    PhysicalPlannerContext context = createPhysicalPlannerContext();

    // The constructor automatically adds the broker instance, so we should have exactly one
    String randomInstanceId = context.getRandomInstanceId();
    Assert.assertEquals(randomInstanceId, "broker_instance_1");
  }

  @Test
  public void testGetRandomInstanceIdWithMultipleInstances() {
    // Test case: Multiple instances present (should return one that's not the current instance)
    PhysicalPlannerContext context = createPhysicalPlannerContext();

    // Add additional server instances
    QueryServerInstance serverInstance2 = new QueryServerInstance("server_instance_2", "host2", 8081, 8081);
    QueryServerInstance serverInstance3 = new QueryServerInstance("server_instance_3", "host3", 8082, 8082);

    context.getInstanceIdToQueryServerInstance().put("server_instance_2", serverInstance2);
    context.getInstanceIdToQueryServerInstance().put("server_instance_3", serverInstance3);

    // Call getRandomInstanceId multiple times to verify it returns different server instances
    // but never the broker instance
    for (int i = 0; i < 10; i++) {
      String randomInstanceId = context.getRandomInstanceId();
      Assert.assertNotEquals(randomInstanceId, "broker_instance_1",
          "Random instance should not be the current broker instance");
      Assert.assertTrue(randomInstanceId.equals("server_instance_2") || randomInstanceId.equals("server_instance_3"),
          "Random instance should be one of the server instances");
    }
  }

  @Test
  public void testGetRandomInstanceIdWithTwoInstances() {
    // Test case: Two instances (broker + one server) - should return the server
    PhysicalPlannerContext context = createPhysicalPlannerContext();

    // Add one server instance
    QueryServerInstance serverInstance = new QueryServerInstance("server_instance_1", "host1", 8081, 8081);
    context.getInstanceIdToQueryServerInstance().put("server_instance_1", serverInstance);

    String randomInstanceId = context.getRandomInstanceId();
    Assert.assertEquals(randomInstanceId, "server_instance_1",
        "With two instances, should return the non-broker instance");
  }

  private PhysicalPlannerContext createPhysicalPlannerContext() {
    RoutingManager mockRoutingManager = mock(RoutingManager.class);
    return new PhysicalPlannerContext(mockRoutingManager, "localhost", 8080, 12345L, "broker_instance_1", Map.of());
  }
}
