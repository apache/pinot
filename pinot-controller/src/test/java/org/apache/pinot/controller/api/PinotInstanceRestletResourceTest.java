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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Tests for the instances Restlet.
 */
public class PinotInstanceRestletResourceTest extends ControllerTest {
  @BeforeClass
  public void setUp() {
    startZk();
    startController();
  }

  @Test
  public void testInstanceListingAndCreation()
      throws Exception {
    // Check that there is only one instance, which is the controller instance.
    JsonNode instanceList = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    assertEquals(instanceList.get("instances").size(), 1, "Expected only one instance at beginning of test");

    // Create untagged broker and server instances
    ObjectNode brokerInstance =
        (ObjectNode) JsonUtils.stringToJsonNode("{\"host\":\"1.2.3.4\", \"type\":\"broker\", \"port\":\"1234\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    ObjectNode serverInstance =
        (ObjectNode) JsonUtils.stringToJsonNode("{\"host\":\"1.2.3.4\", \"type\":\"server\", \"port\":\"2345\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    // Check that there are three instances
    TestUtils.waitForCondition(aVoid -> {
      try {
        // Check that there are two instances
        return
            JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceList())).get("instances")
                .size() == 3;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 500L, 10_000L, "Expected three instances after creation of tagged instances");

    // Create tagged broker and server instances
    brokerInstance.put("tag", "someTag");
    brokerInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    serverInstance.put("tag", "someTag");
    serverInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    // It may take some time for cache data accessor to update its data.
    TestUtils.waitForCondition(aVoid -> {
      try {
        // Check that there are five instances
        return
            JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceList())).get("instances")
                .size() == 5;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 500L, 10_000L, "Expected five instances after creation of tagged instances");

    // Create duplicate broker and server instances (both calls should fail)
    try {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());
      fail("Duplicate broker instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    try {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());
      fail("Duplicate server instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Check that there are five instances
    JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    assertEquals(
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceList())).get("instances")
            .size(), 5, "Expected five instances after creation of duplicate instances");

    // Check that the instances are properly created
    JsonNode instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Broker_1.2.3.4_1234")));
    assertEquals(instance.get("instanceName").asText(), "Broker_1.2.3.4_1234");
    assertEquals(instance.get("hostName").asText(), "1.2.3.4");
    assertEquals(instance.get("port").asText(), "1234");
    assertTrue(instance.get("enabled").asBoolean());
    assertEquals(instance.get("tags").size(), 1);
    assertEquals(instance.get("tags").get(0).asText(), CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);

    instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_1.2.3.4_2345")));
    assertEquals(instance.get("instanceName").asText(), "Server_1.2.3.4_2345");
    assertEquals(instance.get("hostName").asText(), "1.2.3.4");
    assertEquals(instance.get("port").asText(), "2345");
    assertTrue(instance.get("enabled").asBoolean());
    assertEquals(instance.get("tags").size(), 1);
    assertEquals(instance.get("tags").get(0).asText(), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);

    instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Broker_2.3.4.5_1234")));
    assertEquals(instance.get("instanceName").asText(), "Broker_2.3.4.5_1234");
    assertEquals(instance.get("hostName").asText(), "2.3.4.5");
    assertEquals(instance.get("port").asText(), "1234");
    assertTrue(instance.get("enabled").asBoolean());
    assertEquals(instance.get("tags").size(), 1);
    assertEquals(instance.get("tags").get(0).asText(), "someTag");

    instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_2.3.4.5_2345")));
    assertEquals(instance.get("instanceName").asText(), "Server_2.3.4.5_2345");
    assertEquals(instance.get("hostName").asText(), "2.3.4.5");
    assertEquals(instance.get("port").asText(), "2345");
    assertTrue(instance.get("enabled").asBoolean());
    assertEquals(instance.get("tags").size(), 1);
    assertEquals(instance.get("tags").get(0).asText(), "someTag");

    // Check that an error is given for an instance that does not exist
    try {
      sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_potato_8126"));
      fail("Request to get instance information for an instance that does not exist did not fail");
    } catch (IOException e) {
      // Expected
    }
  }
}
