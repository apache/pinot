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

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.ControllerTest;
import java.io.IOException;
import org.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


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
  public void testInstanceListingAndCreation() throws Exception {
    // Check that there are no instances
    JSONObject instanceList = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 0,
        "Expected empty instance list at beginning of test");

    // Create untagged broker and server instances
    JSONObject brokerInstance = new JSONObject("{\"host\":\"1.2.3.4\", \"type\":\"broker\", \"port\":\"1234\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    JSONObject serverInstance = new JSONObject("{\"host\":\"1.2.3.4\", \"type\":\"server\", \"port\":\"2345\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    // Check that there are two instances
    instanceList = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 2,
        "Expected two instances after creation of untagged instances");

    // Create tagged broker and server instances
    brokerInstance.put("tag", "someTag");
    brokerInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    serverInstance.put("tag", "someTag");
    serverInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    // Check that there are four instances
    instanceList = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 4,
        "Expected two instances after creation of tagged instances");

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

    // Check that there are four instances
    instanceList = new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 4,
        "Expected two instances after creation of duplicate instances");

    // Check that the instances are properly created
    JSONObject instance =
        new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Broker_1.2.3.4_1234")));
    assertEquals(instance.get("instanceName"), "Broker_1.2.3.4_1234");
    assertEquals(instance.get("hostName"), "1.2.3.4");
    assertEquals(instance.get("port"), "1234");
    assertEquals(instance.get("enabled"), true);
    assertEquals(instance.getJSONArray("tags").length(), 1);
    assertEquals(instance.getJSONArray("tags").get(0), CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);

    instance =
        new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_1.2.3.4_2345")));
    assertEquals(instance.get("instanceName"), "Server_1.2.3.4_2345");
    assertEquals(instance.get("hostName"), "1.2.3.4");
    assertEquals(instance.get("port"), "2345");
    assertEquals(instance.get("enabled"), true);
    assertEquals(instance.getJSONArray("tags").length(), 1);
    assertEquals(instance.getJSONArray("tags").get(0), CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);

    instance =
        new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Broker_2.3.4.5_1234")));
    assertEquals(instance.get("instanceName"), "Broker_2.3.4.5_1234");
    assertEquals(instance.get("hostName"), "2.3.4.5");
    assertEquals(instance.get("port"), "1234");
    assertEquals(instance.get("enabled"), true);
    assertEquals(instance.getJSONArray("tags").length(), 1);
    assertEquals(instance.getJSONArray("tags").get(0), "someTag");

    instance =
        new JSONObject(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_2.3.4.5_2345")));
    assertEquals(instance.get("instanceName"), "Server_2.3.4.5_2345");
    assertEquals(instance.get("hostName"), "2.3.4.5");
    assertEquals(instance.get("port"), "2345");
    assertEquals(instance.get("enabled"), true);
    assertEquals(instance.getJSONArray("tags").length(), 1);
    assertEquals(instance.getJSONArray("tags").get(0), "someTag");

    // Check that an error is given for an instance that does not exist
    try {
      sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_potato_8126"));
      fail("Request to get instance information for an instance that does not exist did not fail");
    } catch (IOException e) {
      // Expected
    }
  }
}
