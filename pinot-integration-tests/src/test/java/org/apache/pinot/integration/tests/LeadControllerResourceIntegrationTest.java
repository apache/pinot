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
package org.apache.pinot.integration.tests;

import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class LeadControllerResourceIntegrationTest extends ControllerTest {

  @BeforeClass
  public void setUp() {
    startZk();
    startController();

    ControllerLeaderLocator.create(_helixManager);
  }

  @Test
  public void testControllerLeaderLocator() {
    String testTableName1 = "testTable1";
    String testTableName2 = "testTable2";
    ControllerLeaderLocator controllerLeaderLocator = ControllerLeaderLocator.getInstance();

    Pair<String, Integer> pair = controllerLeaderLocator.getControllerLeader(testTableName1);
    // Before resource config is enabled, use helix leader
    Assert.assertNotNull(pair);
    Assert.assertEquals(pair.getFirst(), ControllerTest.LOCAL_HOST);
    Assert.assertEquals((int) pair.getSecond(), ControllerTest.DEFAULT_CONTROLLER_PORT);

    // Enable lead controller resource
    enableResourceConfigForLeadControllerResource(true);

    // Mock the behavior that 40 seconds have passed.
    controllerLeaderLocator.setLastCacheInvalidateMillis(System.currentTimeMillis() - 40_000L);
    controllerLeaderLocator.invalidateCachedControllerLeader(testTableName1);

    // After resource config is enabled, use the lead controller in the resource
    pair = controllerLeaderLocator.getControllerLeader(testTableName1);
    Assert.assertNotNull(pair);
    Assert.assertEquals(pair.getFirst(), ControllerTest.LOCAL_HOST);
    Assert.assertEquals((int) pair.getSecond(), ControllerTest.DEFAULT_CONTROLLER_PORT);

    // Fetch the pair for test table 2, which should be the same as the 1st pair.
    Pair<String, Integer> secondPair = controllerLeaderLocator.getControllerLeader(testTableName2);
    Assert.assertNotNull(secondPair);
    Assert.assertEquals(pair.getFirst(), secondPair.getFirst());
    Assert.assertEquals(pair.getSecond(), secondPair.getSecond());
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
