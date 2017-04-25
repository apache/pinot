/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import java.io.IOException;
import org.apache.helix.AccessOption;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;

import static org.testng.Assert.*;

/**
 * Integration test for instance decommission
 */
public class InstanceDecommissionIntegrationTest extends OfflineClusterIntegrationTest {

  @Test
  public void testInstanceDecommission() throws Exception {
    ControllerRequestURLBuilder urlBuilder = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);

    // Fetch the list of the instances
    JSONObject response = new JSONObject(sendGetRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceList()));
    JSONArray instanceList = response.getJSONArray("instances");
    assertEquals(instanceList.length(), 2, "Expected to have one broker and one server");

    // Try to delete a server that does not exist
    String deleteInstanceRequest = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceDelete("aa");
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (404)");
    } catch (IOException e) {
      // Expected exception beacuse of 404 status code
    }

    // Get the server name
    String serverName = "";
    String brokerName = "";
    for (int i = 0; i < instanceList.length(); i++) {
      if (instanceList.get(i).toString().startsWith("Server_")) {
        serverName = instanceList.get(i).toString();
      } else if (instanceList.get(i).toString().startsWith("Broker_")) {
        brokerName = instanceList.get(i).toString();
      }
    }

    // Try to delete a live server
    deleteInstanceRequest = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceDelete(serverName);
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception because of 409 status code
    }

    // Kill Server
    stopServer();

    // Try to delete a server whose information is still on the idealstate
    try {
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception because of 409 status code
    }

    // Delete the table
    sendDeleteRequest(
        ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forDeleteTableWithType("mytable", "offline")
    );

    // Now, decommissioning for the server should work
    response = new JSONObject(sendDeleteRequest(deleteInstanceRequest));
    System.out.println(response);
    Assert.assertEquals(response.get("status").toString(), "success");

    // Try to delete a broker whose information is still live
    try {
      deleteInstanceRequest = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceDelete(brokerName);
      sendDeleteRequest(deleteInstanceRequest);
      Assert.fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception because of 409 status code
    }

    // TODO: Add test to delete broker instance. Currently, stopBroker() does not work correctly.

    // Check if '/INSTANCES/<server_name>' has been erased correctly
    ensureZkHelixManagerIsInitialized();
    String instancePath = "/" + _zkHelixManager.getClusterName() + "/INSTANCES/" + serverName;
    boolean exist = _zkHelixManager.getHelixDataAccessor().getBaseDataAccessor()
        .exists(instancePath, AccessOption.PERSISTENT);
    Assert.assertEquals(exist, false);

    // Check if '/CONFIGS/PARTICIPANT/<server_name>' has been erased correctly
    String configPath = "/" + _zkHelixManager.getClusterName() + "/CONFIGS/PARTICIPANT/" + serverName;
    exist = _zkHelixManager.getHelixDataAccessor().getBaseDataAccessor().exists(instancePath, AccessOption.PERSISTENT);
    Assert.assertEquals(exist, false);
  }

  @Override
  @Test(enabled = false)
  public void testDistinctCountGroupByQuery() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testDistinctCountNoGroupByQuery() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testDistinctCountHLLNoGroupByQuery() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testDistinctCountHLLGroupByQuery() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testQuantileGroupByQuery() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testQuantileNoGroupByQuery() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testHardcodedQuerySet() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testGeneratedQueriesWithMultiValues() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testInstancesStarted() {
  }

  @AfterClass
  @Override
  public void tearDown() throws Exception {
    stopBroker();
    stopController();
    // No need to stop server because it has already been killed from the test.
    try {
      stopZk();
    } catch (Exception e) {
      // Swallow ZK Exceptions.
    }
    deleteTempDirectory();
  }
}
