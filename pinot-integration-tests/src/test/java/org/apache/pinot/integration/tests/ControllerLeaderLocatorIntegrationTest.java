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

import java.util.HashMap;
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
  private HashMap<Integer, String> _partitionToTableMap;

  @BeforeClass
  public void setUp() {
    startZk();
    startController();

    ControllerLeaderLocator.create(_helixManager);

    findTableNamesForAllPartitions();
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

    Set<String> resultSet = new HashSet<>();
    TestUtils.waitForCondition(aVoid -> {
      resultSet.clear();
      for (Map.Entry<Integer, String> entry : _partitionToTableMap.entrySet()) {
        // Mock the behavior that 40 seconds have passed.
        controllerLeaderLocator.setLastCacheInvalidateMillis(System.currentTimeMillis() - 40_000L);
        controllerLeaderLocator.invalidateCachedControllerLeader();

        String tableName = entry.getValue();
        Pair<String, Integer> pair1 = controllerLeaderLocator.getControllerLeader(tableName);
        if (pair1 == null) {
          return false;
        }
        resultSet.add(pair1.getFirst() + pair1.getSecond());
      }
      // There are two combinations in the result set.
      return resultSet.size() == 2;
    }, TIMEOUT_IN_MS, "Failed to get two pairs of controllers.");

    // Disable lead controller resource
    enableResourceConfigForLeadControllerResource(false);

    TestUtils.waitForCondition(aVoid -> {
      resultSet.clear();
      for (Map.Entry<Integer, String> entry : _partitionToTableMap.entrySet()) {
        // Mock the behavior that 40 seconds have passed.
        controllerLeaderLocator.setLastCacheInvalidateMillis(System.currentTimeMillis() - 40_000L);
        controllerLeaderLocator.invalidateCachedControllerLeader();

        String tableName = entry.getValue();
        Pair<String, Integer> pair1 = controllerLeaderLocator.getControllerLeader(tableName);
        if (pair1 == null) {
          return false;
        }
        resultSet.add(pair1.getFirst() + pair1.getSecond());
      }
      // Only 1 controller should be fetched, which is the helix leader.
      return resultSet.size() == 1;
    }, TIMEOUT_IN_MS, "Failed to get only one pair of controller");

    // Mock the behavior that 40 seconds have passed.
    controllerLeaderLocator.setLastCacheInvalidateMillis(System.currentTimeMillis() - 40_000L);
    controllerLeaderLocator.invalidateCachedControllerLeader();

    // Since lead controller resource disabled, helix leader should be used.
    Pair<String, Integer> thirdPair = controllerLeaderLocator.getControllerLeader(testTableName1);

    Assert.assertEquals(pair.getFirst(), thirdPair.getFirst());
    Assert.assertEquals(pair.getSecond(), thirdPair.getSecond());

    // Stop the second controller.
    secondControllerStarter.stop();
  }

  /**
   * Find the table names for all the partitions.
   */
  private void findTableNamesForAllPartitions() {
    _partitionToTableMap = new HashMap<>();

    int count = 0;
    while (_partitionToTableMap.size() < Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE) {
      String tableName = "testTable" + count;
      int partitionId = LeadControllerUtils.getPartitionIdForTable(tableName);
      _partitionToTableMap.putIfAbsent(partitionId, tableName);
      count++;
    }
    System.out.println("Successfully generate partition to table map after " + count + " iterations");
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}