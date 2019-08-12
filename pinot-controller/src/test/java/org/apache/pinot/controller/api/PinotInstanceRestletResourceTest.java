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
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Helix.InstanceType;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.config.Instance;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.annotations.AfterClass;
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
    // Check that there is only one CONTROLLER instance in the cluster
    String listInstancesUrl = _controllerRequestURLBuilder.forInstanceList();
    JsonNode instanceList = JsonUtils.stringToJsonNode(sendGetRequest(listInstancesUrl));
    assertEquals(instanceList.get("instances").size(), 1);
    assertTrue(instanceList.get("instances").get(0).asText().startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE));

    // Create untagged broker and server instances
    String createInstanceUrl = _controllerRequestURLBuilder.forInstanceCreate();
    Instance brokerInstance = new Instance("1.2.3.4", 1234, InstanceType.BROKER, null, null);
    sendPostRequest(createInstanceUrl, brokerInstance.toJsonString());

    Instance serverInstance = new Instance("1.2.3.4", 2345, InstanceType.SERVER, null, null);
    sendPostRequest(createInstanceUrl, serverInstance.toJsonString());

    // Check that there are 3 instances
    assertEquals(JsonUtils.stringToJsonNode(sendGetRequest(listInstancesUrl)).get("instances").size(), 3);

    // Create broker and server instances with tags and pools
    brokerInstance = new Instance("2.3.4.5", 1234, InstanceType.BROKER, Collections.singletonList("tag_BROKER"), null);
    sendPostRequest(createInstanceUrl, brokerInstance.toJsonString());

    Map<String, Integer> serverPools = new TreeMap<>();
    serverPools.put("tag_OFFLINE", 0);
    serverPools.put("tag_REALTIME", 1);
    serverInstance =
        new Instance("2.3.4.5", 2345, InstanceType.SERVER, Arrays.asList("tag_OFFLINE", "tag_REALTIME"), serverPools);
    sendPostRequest(createInstanceUrl, serverInstance.toJsonString());

    // Check that there are 5 instances
    assertEquals(JsonUtils.stringToJsonNode(sendGetRequest(listInstancesUrl)).get("instances").size(), 5);

    // Create duplicate broker and server instances should fail
    try {
      sendPostRequest(createInstanceUrl, brokerInstance.toJsonString());
      fail("Duplicate broker instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    try {
      sendPostRequest(createInstanceUrl, serverInstance.toJsonString());
      fail("Duplicate server instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Check that there are still 5 instances
    assertEquals(JsonUtils.stringToJsonNode(sendGetRequest(listInstancesUrl)).get("instances").size(), 5);

    // Check that the instances are properly created
    JsonNode instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Broker_1.2.3.4_1234")));
    assertEquals(instance.get("instanceName").asText(), "Broker_1.2.3.4_1234");
    assertEquals(instance.get("hostName").asText(), "Broker_1.2.3.4");
    assertEquals(instance.get("port").asText(), "1234");
    assertTrue(instance.get("enabled").asBoolean());
    assertEquals(instance.get("tags").size(), 0);
    assertTrue(instance.get("pools").isNull());

    instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_1.2.3.4_2345")));
    assertEquals(instance.get("instanceName").asText(), "Server_1.2.3.4_2345");
    assertEquals(instance.get("hostName").asText(), "Server_1.2.3.4");
    assertEquals(instance.get("port").asText(), "2345");
    assertTrue(instance.get("enabled").asBoolean());
    assertEquals(instance.get("tags").size(), 0);
    assertTrue(instance.get("pools").isNull());

    instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Broker_2.3.4.5_1234")));
    assertEquals(instance.get("instanceName").asText(), "Broker_2.3.4.5_1234");
    assertEquals(instance.get("hostName").asText(), "Broker_2.3.4.5");
    assertEquals(instance.get("port").asText(), "1234");
    assertTrue(instance.get("enabled").asBoolean());
    JsonNode tags = instance.get("tags");
    assertEquals(tags.size(), 1);
    assertEquals(tags.get(0).asText(), "tag_BROKER");
    assertTrue(instance.get("pools").isNull());

    instance = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_2.3.4.5_2345")));
    assertEquals(instance.get("instanceName").asText(), "Server_2.3.4.5_2345");
    assertEquals(instance.get("hostName").asText(), "Server_2.3.4.5");
    assertEquals(instance.get("port").asText(), "2345");
    assertTrue(instance.get("enabled").asBoolean());
    tags = instance.get("tags");
    assertEquals(tags.size(), 2);
    assertEquals(tags.get(0).asText(), "tag_OFFLINE");
    assertEquals(tags.get(1).asText(), "tag_REALTIME");
    JsonNode pools = instance.get("pools");
    assertEquals(pools.size(), 2);
    assertEquals(pools.get("tag_OFFLINE").asText(), "0");
    assertEquals(pools.get("tag_REALTIME").asText(), "1");

    // Check that an error is given for an instance that does not exist
    try {
      sendGetRequest(_controllerRequestURLBuilder.forInstanceInformation("Server_potato_8126"));
      fail("Request to get instance information for an instance that does not exist did not fail");
    } catch (IOException e) {
      // Expected
    }
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
