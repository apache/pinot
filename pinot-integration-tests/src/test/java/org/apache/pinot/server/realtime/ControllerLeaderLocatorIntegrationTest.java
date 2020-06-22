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
package org.apache.pinot.server.realtime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerLeaderLocatorIntegrationTest extends ControllerTest {
  private static long TIMEOUT_IN_MS = 10_000L;
  private HashMap<Integer, String> _partitionToTableMap;

  @BeforeClass
  public void setUp() {
    startZk();
    startController();

    FakeControllerLeaderLocator.create(_helixManager);

    findTableNamesForAllPartitions();
  }

  @Test
  public void testControllerLeaderLocator() {
    Set<String> resultSet = new HashSet<>();
    FakeControllerLeaderLocator controllerLeaderLocator = FakeControllerLeaderLocator.getInstance();

    // Before enabling lead controller resource, helix leader should be used.
    validateResultSet(controllerLeaderLocator, resultSet, 1, "Failed to get only one pair of controller");

    // Enable lead controller resource
    enableResourceConfigForLeadControllerResource(true);

    // After resource config is enabled, use the lead controller in the resource.
    validateResultSet(controllerLeaderLocator, resultSet, 1, "Failed to get only one pair of controller");

    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_PORT, DEFAULT_CONTROLLER_PORT + 1);
    ControllerStarter secondControllerStarter = new ControllerStarter(new ControllerConf(properties));
    secondControllerStarter.start();

    TestUtils
        .waitForCondition(aVoid -> secondControllerStarter.getHelixResourceManager().getHelixZkManager().isConnected(),
            TIMEOUT_IN_MS, "Failed to start the second controller");

    // After starting a second controller, there should be two leaders for all the partitions.
    validateResultSet(controllerLeaderLocator, resultSet, 2, "Failed to get two pairs of controllers.");

    // Disable lead controller resource
    enableResourceConfigForLeadControllerResource(false);

    // After disabling lead controller resource, only helix leader should be used.
    validateResultSet(controllerLeaderLocator, resultSet, 1, "Failed to get only one pair of controller");

    // Mock time so it is beyond minimum time to invalidate leader cache
    controllerLeaderLocator.setCurrentTimeMs(
        controllerLeaderLocator.getCurrentTimeMs() + 2 * controllerLeaderLocator.getMinInvalidateIntervalMs());
    controllerLeaderLocator.invalidateCachedControllerLeader();

    // Randomly pick a table name to get a controller leader, and this leader should be the same as the helix leader.
    Pair<String, Integer> pair = controllerLeaderLocator.getControllerLeader(
        _partitionToTableMap.get(new Random().nextInt(Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE)));

    Assert.assertEquals(pair.getFirst(), ControllerTest.LOCAL_HOST);
    Assert.assertEquals((int) pair.getSecond(), ControllerTest.DEFAULT_CONTROLLER_PORT);

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
  }

  private void validateResultSet(FakeControllerLeaderLocator controllerLeaderLocator, Set<String> resultSet,
      int expectedNumberOfUniqueResults, String errorMessage) {
    TestUtils.waitForCondition(aVoid -> {
      resultSet.clear();
      for (Map.Entry<Integer, String> entry : _partitionToTableMap.entrySet()) {
        // Mock time so it is beyond minimum time to invalidate leader cache
        controllerLeaderLocator.setCurrentTimeMs(
            controllerLeaderLocator.getCurrentTimeMs() + 2 * controllerLeaderLocator.getMinInvalidateIntervalMs());
        controllerLeaderLocator.invalidateCachedControllerLeader();

        String tableName = entry.getValue();
        Pair<String, Integer> pair1 = controllerLeaderLocator.getControllerLeader(tableName);
        if (pair1 == null) {
          return false;
        }
        resultSet.add(pair1.getFirst() + pair1.getSecond());
      }
      return resultSet.size() == expectedNumberOfUniqueResults;
    }, TIMEOUT_IN_MS, errorMessage);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  static class FakeControllerLeaderLocator extends ControllerLeaderLocator {
    private static FakeControllerLeaderLocator _instance = null;
    private long _currentTimeMs;

    FakeControllerLeaderLocator(HelixManager helixManager) {
      super(helixManager);
    }

    public static void create(HelixManager helixManager) {
      _instance = new FakeControllerLeaderLocator(helixManager);
    }

    public static FakeControllerLeaderLocator getInstance() {
      return _instance;
    }

    protected long getCurrentTimeMs() {
      return _currentTimeMs;
    }

    void setCurrentTimeMs(long currentTimeMs) {
      _currentTimeMs = currentTimeMs;
    }
  }
}