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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.controller.api.resources.InstanceTagUpdateRequest;
import org.apache.pinot.controller.api.resources.OperationValidationResponse;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for the instances Restlet.
 */
public class PinotInstanceRestletResourceTest extends ControllerTest {

  private ControllerRequestURLBuilder _urlBuilder = null;

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
    _urlBuilder = DEFAULT_INSTANCE.getControllerRequestURLBuilder();
  }

  @Test
  public void testInstanceListingAndCreation()
      throws Exception {
    String listInstancesUrl = _urlBuilder.forInstanceList();
    int expectedNumInstances = 1 + DEFAULT_NUM_BROKER_INSTANCES + DEFAULT_NUM_SERVER_INSTANCES;
    checkNumInstances(listInstancesUrl, expectedNumInstances);

    // Create untagged broker and server instances
    String createInstanceUrl = _urlBuilder.forInstanceCreate();
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
    String brokerInstanceUrl = _urlBuilder.forInstance(brokerInstanceId);
    sendPutRequest(brokerInstanceUrl, newBrokerInstance.toJsonString());
    String newServerTag = "new-server-tag";
    Instance newServerInstance =
        new Instance("1.2.3.4", 2345, InstanceType.SERVER, Collections.singletonList(newServerTag), null, 28090, 28091,
            28092, 28093, true);
    String serverInstanceId = "Server_1.2.3.4_2345";
    String serverInstanceUrl = _urlBuilder.forInstance(serverInstanceId);
    sendPutRequest(serverInstanceUrl, newServerInstance.toJsonString());

    checkInstanceInfo(brokerInstanceId, "1.2.3.4", 1234, new String[]{newBrokerTag}, null, -1, -1, -1, -1, false);
    checkInstanceInfo(serverInstanceId, "1.2.3.4", 2345, new String[]{newServerTag}, null, 28090, 28091, 28092, 28093,
        true);

    // Test Instance updateTags API
    String brokerInstanceUpdateTagsUrl =
        _urlBuilder.forInstanceUpdateTags(brokerInstanceId, Lists.newArrayList("tag_BROKER", "newTag_BROKER"));
    sendPutRequest(brokerInstanceUpdateTagsUrl);
    String serverInstanceUpdateTagsUrl = _urlBuilder.forInstanceUpdateTags(serverInstanceId,
        Lists.newArrayList("tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"));
    sendPutRequest(serverInstanceUpdateTagsUrl);
    checkInstanceInfo(brokerInstanceId, "1.2.3.4", 1234, new String[]{"tag_BROKER", "newTag_BROKER"}, null, -1, -1, -1,
        -1, false);
    checkInstanceInfo(serverInstanceId, "1.2.3.4", 2345,
        new String[]{"tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"}, null, 28090, 28091, 28092, 28093, true);

    // Test DELETE instance API
    sendDeleteRequest(_urlBuilder.forInstance("Broker_1.2.3.4_1234"));
    sendDeleteRequest(_urlBuilder.forInstance("Server_1.2.3.4_2345"));
    sendDeleteRequest(_urlBuilder.forInstance("Broker_2.3.4.5_1234"));
    sendDeleteRequest(_urlBuilder.forInstance("Server_2.3.4.5_2345"));
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
        ControllerTest.sendGetRequest(_urlBuilder.forInstance(instanceName)));
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

  @Test
  public void instanceRetagHappyPathTest()
      throws IOException {
    Map<String, List<String>> currentInstanceTagsMap = getCurrentInstanceTagsMap();
    List<InstanceTagUpdateRequest> request = new ArrayList<>();
    currentInstanceTagsMap.forEach((instance, tags) -> {
      if (instance.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)
          || instance.startsWith(Helix.PREFIX_OF_BROKER_INSTANCE)) {
        InstanceTagUpdateRequest payload = new InstanceTagUpdateRequest();
        payload.setInstanceName(instance);
        payload.setNewTags(tags);
        request.add(payload);
      }
    });
    List<OperationValidationResponse> response = Arrays.asList(new ObjectMapper().readValue(
        sendPostRequest(_urlBuilder.forUpdateTagsValidation(), JsonUtils.objectToString(request)),
        OperationValidationResponse[].class));
    assertNotNull(response);
    response.forEach(item -> assertTrue(item.isSafe()));
  }

  @Test
  public void instanceRetagServerDeficiencyTest()
      throws Exception {
    String tableName = "testTable";
    DEFAULT_INSTANCE.addDummySchema(tableName);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
        .setNumReplicas(2).build();
    // create table with replication as 2 so that DefaultTenant has a minimum server requirement as 2.
    DEFAULT_INSTANCE.getControllerRequestClient().addTableConfig(tableConfig);
    Map<String, List<String>> currentInstanceTagsMap = getCurrentInstanceTagsMap();
    List<InstanceTagUpdateRequest> request = new ArrayList<>();
    currentInstanceTagsMap.forEach((instance, tags) -> {
      if (instance.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)
          || instance.startsWith(Helix.PREFIX_OF_BROKER_INSTANCE)) {
        InstanceTagUpdateRequest payload = new InstanceTagUpdateRequest();
        payload.setInstanceName(instance);
        payload.setNewTags(Lists.newArrayList());
        request.add(payload);
      }
    });
    List<OperationValidationResponse> response = Arrays.asList(new ObjectMapper().readValue(
        sendPostRequest(_urlBuilder.forUpdateTagsValidation(), JsonUtils.objectToString(request)),
        OperationValidationResponse[].class));
    assertNotNull(response);

    int deficientServers = 2;
    int deficientBrokers = 1;
    for (OperationValidationResponse item : response) {
      String instanceName = item.getInstanceName();
      boolean validity = item.isSafe();
      if (!validity) {
        List<OperationValidationResponse.ErrorWrapper> issues = item.getIssues();
        assertEquals(issues.size(), 1);
        assertEquals(issues.get(0).getCode(), OperationValidationResponse.ErrorCode.MINIMUM_INSTANCE_UNSATISFIED);
        if (instanceName.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)) {
          deficientServers--;
        } else if (instanceName.startsWith(Helix.PREFIX_OF_BROKER_INSTANCE)) {
          deficientBrokers--;
        }
      }
    }
    assertEquals(deficientServers, 0);
    assertEquals(deficientBrokers, 0);
    DEFAULT_INSTANCE.dropOfflineTable(tableName);
  }

  private Map<String, List<String>> getCurrentInstanceTagsMap()
      throws IOException {
    String listInstancesUrl = _urlBuilder.forInstanceList();
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(listInstancesUrl));
    JsonNode instances = response.get("instances");
    Map<String, List<String>> map = new HashMap<>(instances.size());
    for (int i = 0; i < instances.size(); i++) {
      String instance = instances.get(i).asText();
      map.put(instance, getInstanceTags(instance));
    }
    return map;
  }

  private List<String> getInstanceTags(String instance)
      throws IOException {
    String getInstancesUrl = _urlBuilder.forInstance(instance);
    List<String> tags = new ArrayList<>();
    for (JsonNode tag : JsonUtils.stringToJsonNode(sendGetRequest(getInstancesUrl)).get("tags")) {
      tags.add(tag.asText());
    }
    return tags;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    DEFAULT_INSTANCE.cleanup();
  }
}
