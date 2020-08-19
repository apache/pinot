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
import com.google.common.base.Function;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;


/**
 * Tests for the instances Restlet.
 */
public class PinotInstanceRestletResourceTest extends ControllerTest {

  private static final long GET_CALL_TIMEOUT_MS = 10000;

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

    TestUtils.waitForCondition(aVoid -> {
      try {
        String getResponse = sendGetRequest(listInstancesUrl);
        JsonNode jsonNode = JsonUtils.stringToJsonNode(getResponse);
        return (jsonNode != null) && (jsonNode.get("instances") != null) && (jsonNode.get("instances").size() == 1)
            && (jsonNode.get("instances").get(0).asText().startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, GET_CALL_TIMEOUT_MS, "Expected one controller instance");

    // Create untagged broker and server instances
    String createInstanceUrl = _controllerRequestURLBuilder.forInstanceCreate();
    Instance brokerInstance = new Instance("1.2.3.4", 1234, InstanceType.BROKER, null, null);
    sendPostRequest(createInstanceUrl, brokerInstance.toJsonString());

    Instance serverInstance = new Instance("1.2.3.4", 2345, InstanceType.SERVER, null, null);
    sendPostRequest(createInstanceUrl, serverInstance.toJsonString());

    // Check that there are 3 instances -- controller, broker and server
    checkNumInstances(listInstancesUrl, 3);

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
    // 1 controller, 2 brokers and 2 servers
    checkNumInstances(listInstancesUrl, 5);

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
    checkNumInstances(listInstancesUrl, 5);

    // Check that the instances are properly created
    checkInstanceInfo("Broker_1.2.3.4_1234", "Broker_1.2.3.4", 1234, new String[0], null, null);
    checkInstanceInfo("Server_1.2.3.4_2345", "Server_1.2.3.4", 2345, new String[0], null, null);
    checkInstanceInfo("Broker_2.3.4.5_1234", "Broker_2.3.4.5", 1234, new String[]{"tag_BROKER"}, null, null);
    checkInstanceInfo("Server_2.3.4.5_2345", "Server_2.3.4.5", 2345, new String[]{"tag_OFFLINE", "tag_REALTIME"},
        new String[]{"tag_OFFLINE", "tag_REALTIME"}, new int[]{0, 1});

    // Test PUT Instance API
    String newBrokerTag = "new-broker-tag";
    Instance newBrokerInstance = new Instance("1.2.3.4", 1234, InstanceType.BROKER, Collections.singletonList(newBrokerTag), null);
    String brokerInstanceId = "Broker_1.2.3.4_1234";
    String brokerInstanceUrl = _controllerRequestURLBuilder.forInstance(brokerInstanceId);
    sendPutRequest(brokerInstanceUrl, newBrokerInstance.toJsonString());

    String newServerTag = "new-server-tag";
    Instance newServerInstance = new Instance("1.2.3.4", 2345, InstanceType.SERVER, Collections.singletonList(newServerTag), null);
    String serverInstanceId = "Server_1.2.3.4_2345";
    String serverInstanceUrl = _controllerRequestURLBuilder.forInstance(serverInstanceId);
    sendPutRequest(serverInstanceUrl, newServerInstance.toJsonString());

    checkInstanceInfo(brokerInstanceId, "Broker_1.2.3.4", 1234, new String[]{newBrokerTag}, null, null);
    checkInstanceInfo(serverInstanceId, "Server_1.2.3.4", 2345, new String[]{newServerTag}, null, null);

    // Test Instance updateTags API
    String brokerInstanceUpdateTagsUrl =
        _controllerRequestURLBuilder.forInstanceUpdateTags(brokerInstanceId, "tag_BROKER,newTag_BROKER");
    sendPutRequest(brokerInstanceUpdateTagsUrl);
    String serverInstanceUpdateTagsUrl = _controllerRequestURLBuilder
        .forInstanceUpdateTags(serverInstanceId, "tag_REALTIME,newTag_OFFLINE,newTag_REALTIME");
    sendPutRequest(serverInstanceUpdateTagsUrl);
    checkInstanceInfo(brokerInstanceId, "Broker_1.2.3.4", 1234, new String[]{"tag_BROKER", "newTag_BROKER"}, null,
        null);
    checkInstanceInfo(serverInstanceId, "Server_1.2.3.4", 2345,
        new String[]{"tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"}, null, null);
  }

  private void checkInstanceInfo(String instanceName, String hostName, int port, String[] tags, String[] pools,
      int[] poolValues) {
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          String getResponse = sendGetRequest(_controllerRequestURLBuilder.forInstance(instanceName));
          JsonNode instance = JsonUtils.stringToJsonNode(getResponse);
          boolean result =
              (instance.get("instanceName") != null) && (instance.get("instanceName").asText().equals(instanceName))
                  && (instance.get("hostName") != null) && (instance.get("hostName").asText().equals(hostName)) && (
                  instance.get("port") != null) && (instance.get("port").asText().equals(String.valueOf(port)))
                  && (instance.get("enabled").asBoolean()) && (instance.get("tags") != null) && (
                  instance.get("tags").size() == tags.length);

          for (int i = 0; i < tags.length; i++) {
            result = result && instance.get("tags").get(i).asText().equals(tags[i]);
          }

          if (!result) {
            return false;
          }

          if (pools != null) {
            result = result && (instance.get("pools") != null) && (instance.get("pools").size() == pools.length);
            for (int i = 0; i < pools.length; i++) {
              result = result && instance.get("pools").get(pools[i]).asText().equals((String.valueOf(poolValues[i])));
            }
          } else {
            result = result && instance.get("pools").isNull();
          }

          return result;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }, GET_CALL_TIMEOUT_MS, "Failed to retrieve correct information for instance: " + instanceName);
  }

  private void checkNumInstances(String listInstancesUrl, int numInstances) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        String getResponse = sendGetRequest(listInstancesUrl);
        JsonNode jsonNode = JsonUtils.stringToJsonNode(getResponse);
        return jsonNode != null && jsonNode.get("instances") != null
            && jsonNode.get("instances").size() == numInstances;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, GET_CALL_TIMEOUT_MS, "Expected " + numInstances + " instances after creation of tagged instances");
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
