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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;


/**
 * Tests for the instances Restlet.
 */
public class PinotInstanceRestletResourceTest {

  private static final long GET_CALL_TIMEOUT_MS = 10000;

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testInstanceListingAndCreation()
      throws Exception {
    // Check that there is only one CONTROLLER instance in the cluster
    String listInstancesUrl = ControllerTestUtils.getControllerRequestURLBuilder().forInstanceList();

    // Determine number of instances and controllers. count[0]: number of instances, count[1]: number of controllers;
    int[] counts = {0, 0};
    TestUtils.waitForCondition(aVoid -> {
      try {
        String getResponse = ControllerTestUtils.sendGetRequest(listInstancesUrl);
        JsonNode jsonNode = JsonUtils.stringToJsonNode(getResponse);

        if (jsonNode != null && jsonNode.get("instances") != null && jsonNode.get("instances").size() > 0) {
          JsonNode instances = jsonNode.get("instances");
          counts[0] = instances.size();
          for (int i = 0; i < counts[0]; i++) {
            if (instances.get(i).asText().startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE)) {
              counts[1]++;
            }
          }
        }

        return counts[1] == 1;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, GET_CALL_TIMEOUT_MS, "Expected one controller instance");

    // Create untagged broker and server instances
    String createInstanceUrl = ControllerTestUtils.getControllerRequestURLBuilder().forInstanceCreate();
    Instance brokerInstance = new Instance("1.2.3.4", 1234, InstanceType.BROKER, null, null, 0, 0, false);
    ControllerTestUtils.sendPostRequest(createInstanceUrl, brokerInstance.toJsonString());

    Instance serverInstance = new Instance("1.2.3.4", 2345, InstanceType.SERVER, null, null, 8090, 8091, false);
    ControllerTestUtils.sendPostRequest(createInstanceUrl, serverInstance.toJsonString());

    // Check that we have added two more instances
    checkNumInstances(listInstancesUrl, counts[0] + 2);

    // Create broker and server instances with tags and pools
    brokerInstance =
        new Instance("2.3.4.5", 1234, InstanceType.BROKER, Collections.singletonList("tag_BROKER"), null, 0, 0, false);
    ControllerTestUtils.sendPostRequest(createInstanceUrl, brokerInstance.toJsonString());

    Map<String, Integer> serverPools = new TreeMap<>();
    serverPools.put("tag_OFFLINE", 0);
    serverPools.put("tag_REALTIME", 1);
    serverInstance =
        new Instance("2.3.4.5", 2345, InstanceType.SERVER, Arrays.asList("tag_OFFLINE", "tag_REALTIME"), serverPools,
            18090, 18091, false);
    ControllerTestUtils.sendPostRequest(createInstanceUrl, serverInstance.toJsonString());

    // Check that we have added four instances so far
    checkNumInstances(listInstancesUrl, counts[0] + 4);

    // Create duplicate broker and server instances should fail
    try {
      ControllerTestUtils.sendPostRequest(createInstanceUrl, brokerInstance.toJsonString());
      fail("Duplicate broker instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    try {
      ControllerTestUtils.sendPostRequest(createInstanceUrl, serverInstance.toJsonString());
      fail("Duplicate server instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Check that number of instances did not change.
    checkNumInstances(listInstancesUrl, counts[0] + 4);

    // Check that the instances are properly created
    checkInstanceInfo("Broker_1.2.3.4_1234", "1.2.3.4", 1234, new String[0], null, null, false);
    checkInstanceInfo("Server_1.2.3.4_2345", "1.2.3.4", 2345, new String[0], null, null, 8090, 8091, false);
    checkInstanceInfo("Broker_2.3.4.5_1234", "2.3.4.5", 1234, new String[]{"tag_BROKER"}, null, null, false);
    checkInstanceInfo("Server_2.3.4.5_2345", "2.3.4.5", 2345, new String[]{"tag_OFFLINE", "tag_REALTIME"},
        new String[]{"tag_OFFLINE", "tag_REALTIME"}, new int[]{0, 1}, 18090, 18091, false);

    // Test PUT Instance API
    String newBrokerTag = "new-broker-tag";
    Instance newBrokerInstance =
        new Instance("1.2.3.4", 1234, InstanceType.BROKER, Collections.singletonList(newBrokerTag), null, 0, 0, false);
    String brokerInstanceId = "Broker_1.2.3.4_1234";
    String brokerInstanceUrl = ControllerTestUtils.getControllerRequestURLBuilder().forInstance(brokerInstanceId);
    ControllerTestUtils.sendPutRequest(brokerInstanceUrl, newBrokerInstance.toJsonString());

    String newServerTag = "new-server-tag";
    Instance newServerInstance =
        new Instance("1.2.3.4", 2345, InstanceType.SERVER, Collections.singletonList(newServerTag), null, 28090, 28091,
            true);
    String serverInstanceId = "Server_1.2.3.4_2345";
    String serverInstanceUrl = ControllerTestUtils.getControllerRequestURLBuilder().forInstance(serverInstanceId);
    ControllerTestUtils.sendPutRequest(serverInstanceUrl, newServerInstance.toJsonString());

    checkInstanceInfo(brokerInstanceId, "1.2.3.4", 1234, new String[]{newBrokerTag}, null, null, false);
    checkInstanceInfo(serverInstanceId, "1.2.3.4", 2345, new String[]{newServerTag}, null, null, 28090, 28091, true);

    // Test Instance updateTags API
    String brokerInstanceUpdateTagsUrl = ControllerTestUtils.getControllerRequestURLBuilder()
        .forInstanceUpdateTags(brokerInstanceId, Lists.newArrayList("tag_BROKER", "newTag_BROKER"));
    ControllerTestUtils.sendPutRequest(brokerInstanceUpdateTagsUrl);
    String serverInstanceUpdateTagsUrl = ControllerTestUtils.getControllerRequestURLBuilder()
        .forInstanceUpdateTags(serverInstanceId,
            Lists.newArrayList("tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"));
    ControllerTestUtils.sendPutRequest(serverInstanceUpdateTagsUrl);
    checkInstanceInfo(brokerInstanceId, "1.2.3.4", 1234, new String[]{"tag_BROKER", "newTag_BROKER"}, null, null,
        false);
    checkInstanceInfo(serverInstanceId, "1.2.3.4", 2345,
        new String[]{"tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"}, null, null, 28090, 28091, true);
  }

  private void checkInstanceInfo(String instanceName, String hostName, int port, String[] tags, String[] pools,
      int[] poolValues, boolean queriesDisabled) {
    checkInstanceInfo(instanceName, hostName, port, tags, pools, poolValues, Instance.NOT_SET_GRPC_PORT_VALUE,
        Instance.NOT_SET_ADMIN_PORT_VALUE, queriesDisabled);
  }

  private void checkInstanceInfo(String instanceName, String hostName, int port, String[] tags, String[] pools,
      int[] poolValues, int grpcPort, int adminPort, boolean queriesDisabled) {
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          String getResponse = ControllerTestUtils
              .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forInstance(instanceName));
          JsonNode instance = JsonUtils.stringToJsonNode(getResponse);
          boolean result =
              (instance.get("instanceName") != null) && (instance.get("instanceName").asText().equals(instanceName))
                  && (instance.get("hostName") != null) && (instance.get("hostName").asText().equals(hostName)) && (
                  instance.get("port") != null) && (instance.get("port").asText().equals(String.valueOf(port)))
                  && (instance.get("enabled").asBoolean()) && (instance.get("tags") != null) && (
                  instance.get("tags").size() == tags.length) && (
                  instance.get("grpcPort").asText().equals(String.valueOf(grpcPort)) && (
                      instance.get("queriesDisabled") == null && !queriesDisabled
                          || instance.get("queriesDisabled") != null
                          && instance.get("queriesDisabled").asBoolean() == queriesDisabled));

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
        String getResponse = ControllerTestUtils.sendGetRequest(listInstancesUrl);
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
    ControllerTestUtils.cleanup();
  }
}
