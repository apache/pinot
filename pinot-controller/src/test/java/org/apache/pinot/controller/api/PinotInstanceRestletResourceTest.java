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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the instances Restlet.
 */
public class PinotInstanceRestletResourceTest extends ControllerTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testInstanceListingAndCreation()
      throws Exception {
    ControllerRequestURLBuilder requestURLBuilder = DEFAULT_INSTANCE.getControllerRequestURLBuilder();
    String listInstancesUrl = requestURLBuilder.forInstanceList();
    int expectedNumInstances = 1 + DEFAULT_NUM_BROKER_INSTANCES + DEFAULT_NUM_SERVER_INSTANCES;
    checkNumInstances(listInstancesUrl, expectedNumInstances);

    // Create untagged broker and server instances
    String createInstanceUrl = requestURLBuilder.forInstanceCreate();
    Instance brokerInstance1 = new Instance("1.2.3.4", 1234, InstanceType.BROKER, null, null, 0, 0, 0, 0, false);
    sendPostRequest(createInstanceUrl, brokerInstance1.toJsonString());
    Instance serverInstance1 =
        new Instance("1.2.3.4", 2345, InstanceType.SERVER, null, null, 8090, 8091, 8092, 8093, false);
    sendPostRequest(createInstanceUrl, serverInstance1.toJsonString());

    // Check that we have added two more instances
    checkNumInstances(listInstancesUrl, expectedNumInstances + 2);

    // Create broker and server instances with tags and pools
    Instance brokerInstance2 =
        new Instance("2.3.4.5", 1234, InstanceType.BROKER, Collections.singletonList("tag_BROKER"), null, 0, 0, 0, 0,
            false);
    sendPostRequest(createInstanceUrl, brokerInstance2.toJsonString());
    Map<String, Integer> serverPools = new TreeMap<>();
    serverPools.put("tag_OFFLINE", 0);
    serverPools.put("tag_REALTIME", 1);
    Instance serverInstance2 =
        new Instance("2.3.4.5", 2345, InstanceType.SERVER, Arrays.asList("tag_OFFLINE", "tag_REALTIME"), serverPools,
            18090, 18091, 18092, 18093, false);
    sendPostRequest(createInstanceUrl, serverInstance2.toJsonString());

    // Check that we have added four instances so far
    checkNumInstances(listInstancesUrl, expectedNumInstances + 4);

    // Create duplicate broker and server instances should fail
    assertThrows(IOException.class, () -> sendPostRequest(createInstanceUrl, brokerInstance1.toJsonString()));
    assertThrows(IOException.class, () -> sendPostRequest(createInstanceUrl, serverInstance1.toJsonString()));
    assertThrows(IOException.class, () -> sendPostRequest(createInstanceUrl, brokerInstance2.toJsonString()));
    assertThrows(IOException.class, () -> sendPostRequest(createInstanceUrl, serverInstance2.toJsonString()));

    // Check that number of instances did not change.
    checkNumInstances(listInstancesUrl, expectedNumInstances + 4);

    // Check that the instances are properly created
    checkInstanceInfo("Broker_1.2.3.4_1234", "1.2.3.4", 1234, new String[0], null, -1, -1, -1, -1, false);
    checkInstanceInfo("Server_1.2.3.4_2345", "1.2.3.4", 2345, new String[0], null, 8090, 8091, 8092, 8093, false);
    checkInstanceInfo("Broker_2.3.4.5_1234", "2.3.4.5", 1234, new String[]{"tag_BROKER"}, null, -1, -1, -1, -1, false);
    checkInstanceInfo("Server_2.3.4.5_2345", "2.3.4.5", 2345, new String[]{"tag_OFFLINE", "tag_REALTIME"}, serverPools,
        18090, 18091, 18092, 18093, false);

    // Test PUT instance API
    String newBrokerTag = "new-broker-tag";
    Instance newBrokerInstance =
        new Instance("1.2.3.4", 1234, InstanceType.BROKER, Collections.singletonList(newBrokerTag), null, 0, 0, 0, 0,
            false);
    String brokerInstanceId = "Broker_1.2.3.4_1234";
    String brokerInstanceUrl = requestURLBuilder.forInstance(brokerInstanceId);
    sendPutRequest(brokerInstanceUrl, newBrokerInstance.toJsonString());
    String newServerTag = "new-server-tag";
    Instance newServerInstance =
        new Instance("1.2.3.4", 2345, InstanceType.SERVER, Collections.singletonList(newServerTag), null, 28090, 28091,
            28092, 28093, true);
    String serverInstanceId = "Server_1.2.3.4_2345";
    String serverInstanceUrl = requestURLBuilder.forInstance(serverInstanceId);
    sendPutRequest(serverInstanceUrl, newServerInstance.toJsonString());

    checkInstanceInfo(brokerInstanceId, "1.2.3.4", 1234, new String[]{newBrokerTag}, null, -1, -1, -1, -1, false);
    checkInstanceInfo(serverInstanceId, "1.2.3.4", 2345, new String[]{newServerTag}, null, 28090, 28091, 28092, 28093,
        true);

    // Test Instance updateTags API
    String brokerInstanceUpdateTagsUrl =
        requestURLBuilder.forInstanceUpdateTags(brokerInstanceId, Lists.newArrayList("tag_BROKER", "newTag_BROKER"));
    sendPutRequest(brokerInstanceUpdateTagsUrl);
    String serverInstanceUpdateTagsUrl = requestURLBuilder.forInstanceUpdateTags(serverInstanceId,
        Lists.newArrayList("tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"));
    sendPutRequest(serverInstanceUpdateTagsUrl);
    checkInstanceInfo(brokerInstanceId, "1.2.3.4", 1234, new String[]{"tag_BROKER", "newTag_BROKER"}, null, -1, -1, -1,
        -1, false);
    checkInstanceInfo(serverInstanceId, "1.2.3.4", 2345,
        new String[]{"tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"}, null, 28090, 28091, 28092, 28093, true);

    // Test DELETE instance API
    sendDeleteRequest(requestURLBuilder.forInstance("Broker_1.2.3.4_1234"));
    sendDeleteRequest(requestURLBuilder.forInstance("Server_1.2.3.4_2345"));
    sendDeleteRequest(requestURLBuilder.forInstance("Broker_2.3.4.5_1234"));
    sendDeleteRequest(requestURLBuilder.forInstance("Server_2.3.4.5_2345"));
    checkNumInstances(listInstancesUrl, expectedNumInstances);
  }

  private void checkNumInstances(String listInstancesUrl, int expectedNumInstances)
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(listInstancesUrl));
    JsonNode instances = response.get("instances");
    assertEquals(instances.size(), expectedNumInstances);
    int numControllers = 0;
    for (int i = 0; i < expectedNumInstances; i++) {
      if (instances.get(i).asText().startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE)) {
        numControllers++;
      }
    }
    assertEquals(numControllers, 1);
  }

  private void checkInstanceInfo(String instanceName, String hostName, int port, String[] tags,
      @Nullable Map<String, Integer> pools, int grpcPort, int adminPort, int queryServicePort, int queryMailboxPort,
      boolean queriesDisabled)
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(
        ControllerTest.sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forInstance(instanceName)));
    assertEquals(response.get("instanceName").asText(), instanceName);
    assertEquals(response.get("hostName").asText(), hostName);
    assertTrue(response.get("enabled").asBoolean());
    assertEquals(response.get("port").asInt(), port);
    JsonNode actualTags = response.get("tags");
    assertEquals(actualTags.size(), tags.length);
    for (int i = 0; i < tags.length; i++) {
      assertEquals(actualTags.get(i).asText(), tags[i]);
    }
    JsonNode actualPools = response.get("pools");
    if (pools != null) {
      assertEquals(actualPools.size(), pools.size());
      for (Map.Entry<String, Integer> entry : pools.entrySet()) {
        assertEquals(actualPools.get(entry.getKey()).asInt(), (int) entry.getValue());
      }
    } else {
      assertTrue(actualPools.isNull());
    }
    assertEquals(response.get("grpcPort").asInt(), grpcPort);
    assertEquals(response.get("adminPort").asInt(), adminPort);
    assertEquals(response.get("queryServicePort").asInt(), queryServicePort);
    assertEquals(response.get("queryMailboxPort").asInt(), queryMailboxPort);
    if (queriesDisabled) {
      assertTrue(response.get("queriesDisabled").asBoolean());
    } else {
      assertNull(response.get("queriesDisabled"));
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    DEFAULT_INSTANCE.cleanup();
  }
}
