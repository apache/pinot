/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.controller.helix.ControllerTest;
import org.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Test for table to tenant mapping. Real working test is inside offline cluster integration test because it requires
 * segment upload.
 */
public class PinotTenantRestletResourceTest extends ControllerTest {

  @BeforeClass
  public void setUp() {
    startZk();
    startController();
  }

  @Test
  public void testTableListForTenant() throws Exception {
    JSONObject tableList = null;

    try {
      // Check that invalid request throws exception
      new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTablesFromTenant("randomTenant")));
      fail("Expected invalid tenant to fail");
    } catch (Exception e) {

    }

    // Create untagged broker and server instances
    JSONObject brokerInstance = new JSONObject("{\"host\":\"1.2.3.4\", \"type\":\"broker\", \"port\":\"1234\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    JSONObject serverInstance = new JSONObject("{\"host\":\"1.2.3.4\", \"type\":\"server\", \"port\":\"2345\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    // Create tagged broker and server instances
    brokerInstance.put("tag", "someTag");
    brokerInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    serverInstance.put("tag", "server_REALTIME");
    serverInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    try {
      new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTablesFromTenant("server_OFFLINE")));
      fail("Should not work because haven't added an offline server");
    } catch (Exception e) {

    }

    // Check that no tables on tenant works
    tableList = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTablesFromTenant("server_REALTIME")));
    assertEquals(tableList.getJSONArray("tables").length(), 0, "Expected no tables");

    // Try to make sure both kinds of tags work
    tableList = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forTablesFromTenant("server")));
    assertEquals(tableList.getJSONArray("tables").length(), 0, "Expected no tables");
  }
}
