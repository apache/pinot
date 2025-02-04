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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Map;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.FailureDetector;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.BrokerResourceStateModel;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Integration test that extends OfflineClusterIntegrationTest but start multiple brokers and servers.
 */
public class MultiNodesOfflineClusterIntegrationTest extends OfflineClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodesOfflineClusterIntegrationTest.class);
  private static final int NUM_BROKERS = 2;
  private static final int NUM_SERVERS = 3;

  @Override
  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  @Override
  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected int getNumReplicas() {
    return NUM_SERVERS;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);
    brokerConf.setProperty(FailureDetector.CONFIG_OF_TYPE, FailureDetector.Type.CONNECTION.name());
  }

  @Test
  public void testUpdateBrokerResource()
      throws Exception {
    // Add a new broker to the cluster
    BaseBrokerStarter brokerStarter = startOneBroker(NUM_BROKERS);

    // Check if broker is added to all the tables in broker resource
    String clusterName = getHelixClusterName();
    String brokerId = brokerStarter.getInstanceId();
    IdealState brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertEquals(brokerAssignment.get(brokerId), BrokerResourceStateModel.ONLINE);
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (!brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to find broker in broker resource ExternalView");

    // Stop the broker
    brokerStarter.stop();
    _brokerPorts.remove(_brokerPorts.size() - 1);

    // Dropping the broker should fail because it is still in the broker resource
    try {
      sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));
      fail("Dropping instance should fail because it is still in the broker resource");
    } catch (Exception e) {
      // Expected
    }

    // Untag the broker and update the broker resource so that it is removed from the broker resource
    sendPutRequest(_controllerRequestURLBuilder.forInstanceUpdateTags(brokerId, Collections.emptyList(), true));

    // Check if broker is removed from all the tables in broker resource
    brokerResourceIdealState =
        _helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map<String, String> brokerAssignment : brokerResourceIdealState.getRecord().getMapFields().values()) {
      assertFalse(brokerAssignment.containsKey(brokerId));
    }
    TestUtils.waitForCondition(aVoid -> {
      ExternalView brokerResourceExternalView =
          _helixAdmin.getResourceExternalView(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (Map<String, String> brokerAssignment : brokerResourceExternalView.getRecord().getMapFields().values()) {
        if (brokerAssignment.containsKey(brokerId)) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to remove broker from broker resource ExternalView");

    // Dropping the broker should success now
    sendDeleteRequest(_controllerRequestURLBuilder.forInstance(brokerId));

    // Check if broker is dropped from the cluster
    assertFalse(_helixAdmin.getInstancesInCluster(clusterName).contains(brokerId));
  }

  @Test
  public void testServerHardFailure()
      throws Exception {
    long expectedCountStarResult = getCountStarResult();
    testCountStarQuery(3, false);
    assertEquals(getCurrentCountStarResult(), expectedCountStarResult);

    LOGGER.warn("Shutting down server {}", _serverStarters.get(NUM_SERVERS - 1).getInstanceId());
    // Take a server and shut down its query server to mimic a hard failure
    BaseServerStarter serverStarter = _serverStarters.get(NUM_SERVERS - 1);
    try {
      serverStarter.getServerInstance().shutDown();

      // First query should hit all servers and get connection refused or reset exception
      // TODO: This is a flaky test. There is a race condition between shutDown and the query being executed.
      testCountStarQuery(NUM_SERVERS, true);

      // Second query should not hit the failed server, and should return the correct result
      testCountStarQuery(NUM_SERVERS - 1, false);
    } finally {
      // Restart the failed server, and it should be included in the routing again
      serverStarter = restartServer(serverStarter);
      _serverStarters.set(NUM_SERVERS - 1, serverStarter);
      TestUtils.waitForCondition((aVoid) -> {
        try {
          JsonNode queryResult = postQuery("SELECT COUNT(*) FROM mytable");
          // Result should always be correct
          assertEquals(queryResult.get("resultTable").get("rows").get(0).get(0).longValue(), getCountStarResult());
          return queryResult.get("numServersQueried").intValue() == NUM_SERVERS;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, 10_000L, "Failed to include the restarted server into the routing. Other tests may be affected");
    }
  }

  private void testCountStarQuery(int expectedNumServersQueried, boolean exceptionExpected)
      throws Exception {
    JsonNode queryResult = postQuery("SELECT COUNT(*) FROM mytable");
    assertEquals(queryResult.get("numServersQueried").intValue(), expectedNumServersQueried);
    if (exceptionExpected) {
      // 2 exceptions expected: 1. connection refused from the failed server; 2. servers not responded
      JsonNode exceptions = queryResult.get("exceptions");
      assertEquals(exceptions.size(), 2);
      JsonNode firstException = exceptions.get(0);
      // NOTE:
      // Only verify the error code but not the exception message because there can be different messages:
      // - Connection refused
      // - Connection reset
      // - Channel is inactive
      assertEquals(firstException.get("errorCode").intValue(), QueryException.BROKER_REQUEST_SEND_ERROR_CODE);
      JsonNode secondException = exceptions.get(1);
      assertEquals(secondException.get("errorCode").intValue(), QueryException.SERVER_NOT_RESPONDING_ERROR_CODE);
    } else {
      assertEquals(queryResult.get("resultTable").get("rows").get(0).get(0).longValue(), getCountStarResult());
      assertTrue(queryResult.get("exceptions").isEmpty());
    }
  }

  @Test
  public void testServerReturnFinalResult()
      throws Exception {
    // Data is segment partitioned on DaysSinceEpoch.
    JsonNode result = postQuery("SELECT DISTINCT_COUNT(DaysSinceEpoch) FROM mytable");
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).intValue(), 364);
    result = postQuery("SELECT SEGMENT_PARTITIONED_DISTINCT_COUNT(DaysSinceEpoch) FROM mytable");
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).intValue(), 364);
    result = postQuery("SET serverReturnFinalResult = true; SELECT DISTINCT_COUNT(DaysSinceEpoch) FROM mytable");
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).intValue(), 364);

    // Data is not partitioned on DayOfWeek. Each segment contains all 7 unique values.
    result = postQuery("SELECT DISTINCT_COUNT(DayOfWeek) FROM mytable");
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).intValue(), 7);
    result = postQuery("SELECT SEGMENT_PARTITIONED_DISTINCT_COUNT(DayOfWeek) FROM mytable");
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).intValue(), 84);
    result = postQuery("SET serverReturnFinalResult = true; SELECT DISTINCT_COUNT(DayOfWeek) FROM mytable");
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).intValue(), 21);

    // Data is segment partitioned on DaysSinceEpoch.
    result =
        postQuery("SELECT DaysSinceEpoch, DISTINCT_COUNT(CRSArrTime) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    JsonNode row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 16138);
    assertEquals(row.get(1).intValue(), 398);
    result = postQuery("SELECT DaysSinceEpoch, SEGMENT_PARTITIONED_DISTINCT_COUNT(CRSArrTime) FROM mytable GROUP BY 1 "
        + "ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 16138);
    assertEquals(row.get(1).intValue(), 398);
    result = postQuery("SET serverReturnFinalResult = true; "
        + "SELECT DaysSinceEpoch, DISTINCT_COUNT(CRSArrTime) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 16138);
    assertEquals(row.get(1).intValue(), 398);
    result = postQuery("SET serverReturnFinalResultKeyUnpartitioned = true; "
        + "SELECT DaysSinceEpoch, DISTINCT_COUNT(CRSArrTime) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 16138);
    assertEquals(row.get(1).intValue(), 398);

    // Data is segment partitioned on DaysSinceEpoch.
    result =
        postQuery("SELECT CRSArrTime, DISTINCT_COUNT(DaysSinceEpoch) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 2100);
    assertEquals(row.get(1).intValue(), 253);
    result = postQuery("SELECT CRSArrTime, SEGMENT_PARTITIONED_DISTINCT_COUNT(DaysSinceEpoch) FROM mytable GROUP BY 1 "
        + "ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 2100);
    assertEquals(row.get(1).intValue(), 253);
    result = postQuery("SET serverReturnFinalResultKeyUnpartitioned = true; "
        + "SELECT CRSArrTime, DISTINCT_COUNT(DaysSinceEpoch) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 2100);
    assertEquals(row.get(1).intValue(), 253);
    // Data is not partitioned on CRSArrTime. Using serverReturnFinalResult will give wrong result.
    result = postQuery("SET serverReturnFinalResult = true; "
        + "SELECT CRSArrTime, DISTINCT_COUNT(DaysSinceEpoch) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertTrue(row.get(1).intValue() < 253);

    // Should fail when merging final results that cannot be merged.
    try {
      postQuery("SET serverReturnFinalResult = true; SELECT AVG(DaysSinceEpoch) FROM mytable");
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Cannot merge final results for function: AVG"));
    }
    try {
      postQuery("SET serverReturnFinalResultKeyUnpartitioned = true; "
          + "SELECT CRSArrTime, AVG(DaysSinceEpoch) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Cannot merge final results for function: AVG"));
    }

    // Should not fail when group keys are partitioned because there is no need to merge final results.
    result = postQuery("SELECT DaysSinceEpoch, AVG(CRSArrTime) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 16257);
    assertEquals(row.get(1).doubleValue(), 725560.0 / 444);
    result = postQuery("SET serverReturnFinalResult = true; "
        + "SELECT DaysSinceEpoch, AVG(CRSArrTime) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 16257);
    assertEquals(row.get(1).doubleValue(), 725560.0 / 444);
    result = postQuery("SET serverReturnFinalResultKeyUnpartitioned = true; "
        + "SELECT DaysSinceEpoch, AVG(CRSArrTime) FROM mytable GROUP BY 1 ORDER BY 2 DESC LIMIT 1");
    row = result.get("resultTable").get("rows").get(0);
    assertEquals(row.get(0).intValue(), 16257);
    assertEquals(row.get(1).doubleValue(), 725560.0 / 444);
  }

  // Disabled because with multiple replicas, there is no guarantee that all replicas are reloaded
  @Test(enabled = false)
  public void testStarTreeTriggering() {
    // Ignored
  }

  // Disabled because with multiple replicas, there is no guarantee that all replicas are reloaded
  @Test(enabled = false)
  @Override
  public void testDefaultColumns(boolean useMultiStageQueryEngine) {
    // Ignored
  }

  // Disabled because with multiple replicas, there is no guarantee that all replicas are reloaded
  @Test(enabled = false)
  @Override
  public void testForwardIndexTriggering() {
    // Ignored
  }

  // Disabled because with multiple replicas, there is no guarantee that all replicas are reloaded
  @Test(enabled = false)
  public void testBloomFilterTriggering() {
    // Ignored
  }

  // Disabled because with multiple replicas, there is no guarantee that all replicas are reloaded
  @Test(enabled = false)
  @Override
  public void testRangeIndexTriggering(boolean useMultiStageQueryEngine)
      throws Exception {
    // Ignored
  }

  // Disabled because with multiple replicas, there is no guarantee that all replicas are reloaded
  @Test(enabled = false)
  @Override
  public void testInvertedIndexTriggering() {
    // Ignored
  }

  @Test(enabled = false)
  @Override
  public void testHardcodedServerPartitionedSqlQueries() {
    // Ignored
  }
}
