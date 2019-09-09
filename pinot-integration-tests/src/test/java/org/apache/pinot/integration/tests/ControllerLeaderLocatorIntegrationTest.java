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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerLeaderLocatorIntegrationTest extends ControllerTest {
  private static long TIMEOUT_IN_MS = 1000_000L;

  @BeforeClass
  public void setUp() {
    startZk();
    startController();

    ControllerLeaderLocator.create(_helixManager);
  }

  @Test
  public void testControllerLeaderLocator() {
    String testTableName1 = "testTable";
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
    controllerLeaderLocator.invalidateCachedControllerLeader();

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

    ControllerConf secondControllerConfig = getDefaultControllerConfiguration();
    secondControllerConfig.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT + 1));
    ControllerStarter secondControllerStarter = new ControllerStarter(secondControllerConfig);
    secondControllerStarter.start();

    TestUtils
        .waitForCondition(aVoid -> secondControllerStarter.getHelixResourceManager().getHelixZkManager().isConnected(),
            TIMEOUT_IN_MS, "Failed to start the second controller");

    // Generate a table name that the second controller is its new lead controller, which should be different from the first table name.
    AtomicReference<String> testTableName3 = new AtomicReference<>();
    TestUtils.waitForCondition(aVoid -> {
      String tableName;
      try {
        tableName = generateTableNameUsingSecondController(secondControllerConfig, testTableName1);
      } catch (Exception e) {
        return false;
      }
      testTableName3.set(tableName);
      return tableName != null;
    }, TIMEOUT_IN_MS, "Failed to find the second table");

    // Mock the behavior that 40 seconds have passed.
    controllerLeaderLocator.setLastCacheInvalidateMillis(System.currentTimeMillis() - 40_000L);
    controllerLeaderLocator.invalidateCachedControllerLeader();

    // The second controller should be the lead controller for test table 3, which isn't the helix leader.
    Pair<String, Integer> thirdPair = controllerLeaderLocator.getControllerLeader(testTableName3.get());
    Assert.assertNotNull(thirdPair);
    Assert.assertEquals(thirdPair.getFirst(), ControllerTest.LOCAL_HOST);
    Assert.assertEquals((int) thirdPair.getSecond(), (ControllerTest.DEFAULT_CONTROLLER_PORT + 1));
    Assert.assertNotEquals(pair.getSecond(), thirdPair.getSecond());

    secondControllerStarter.stop();
  }

  /**
   * Generates a table name which uses the second controller as its lead controller.
   */
  private String generateTableNameUsingSecondController(ControllerConf secondControllerConfig, String testTableName) {
    String secondParticipantId = LeadControllerUtils
        .generateParticipantInstanceId(secondControllerConfig.getControllerHost(),
            Integer.parseInt(secondControllerConfig.getControllerPort()));
    Set<Integer> partitionIdsForSecondController = new HashSet<>();

    // Find out all the partitionIds that the second controller is assigned to.
    int partitionId = LeadControllerUtils.getPartitionIdForTable(testTableName);
    String partitionNameForTable1 = LeadControllerUtils.generatePartitionName(partitionId);
    ExternalView leadControllerResourceExternalView =
        _helixAdmin.getResourceExternalView(_helixManager.getClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    Set<String> partitionNames = leadControllerResourceExternalView.getPartitionSet();
    for (String partitionName : partitionNames) {
      Map<String, String> partitionStateMap = leadControllerResourceExternalView.getStateMap(partitionName);
      // Get master host from partition map. Return null if no master found.
      for (Map.Entry<String, String> entry : partitionStateMap.entrySet()) {
        if (!partitionNameForTable1.equals(partitionName) && MasterSlaveSMD.States.MASTER.name()
            .equals(entry.getValue())) {
          if (secondParticipantId.equals(entry.getKey())) {
            partitionIdsForSecondController.add(LeadControllerUtils.extractPartitionId(partitionName));
          }
        }
      }
    }

    // Try 100 times to find out a table which is using the second controller as lead controller.
    for (int i = 0; i < 100; i++) {
      String newTestTableName = testTableName + i;
      int newPartitionId = LeadControllerUtils.getPartitionIdForTable(newTestTableName);
      if (partitionIdsForSecondController.contains(newPartitionId)) {
        return newTestTableName;
      }
    }
    throw new RuntimeException(
        "Fail to find a table name within 100 times which uses the second controller as lead controller");
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}