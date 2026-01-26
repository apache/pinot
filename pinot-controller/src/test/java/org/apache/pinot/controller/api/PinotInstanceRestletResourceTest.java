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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.api.resources.InstanceTagUpdateRequest;
import org.apache.pinot.controller.api.resources.OperationValidationResponse;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
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
    PinotAdminClient adminClient = DEFAULT_INSTANCE.getOrCreateAdminClient();
    int expectedNumInstances =
        1 + DEFAULT_NUM_BROKER_INSTANCES + DEFAULT_NUM_SERVER_INSTANCES + DEFAULT_NUM_MINION_INSTANCES;
    checkNumInstances(adminClient, expectedNumInstances);

    // Create untagged broker and server instances
    Instance brokerInstance1 = new Instance("1.2.3.4", 1234, InstanceType.BROKER, null, null, 0, 0, 0, 0, false);
    adminClient.getInstanceClient().createInstance(brokerInstance1.toJsonString());
    Instance serverInstance1 =
        new Instance("1.2.3.4", 2345, InstanceType.SERVER, null, null, 8090, 8091, 8092, 8093, false);
    adminClient.getInstanceClient().createInstance(serverInstance1.toJsonString());

    // Check that we have added two more instances
    checkNumInstances(adminClient, expectedNumInstances + 2);

    // Create broker and server instances with tags and pools
    Instance brokerInstance2 =
        new Instance("2.3.4.5", 1234, InstanceType.BROKER, Collections.singletonList("tag_BROKER"), null, 0, 0, 0, 0,
            false);
    adminClient.getInstanceClient().createInstance(brokerInstance2.toJsonString());
    Map<String, Integer> serverPools = new TreeMap<>();
    serverPools.put("tag_OFFLINE", 0);
    serverPools.put("tag_REALTIME", 1);
    Instance serverInstance2 =
        new Instance("2.3.4.5", 2345, InstanceType.SERVER, Arrays.asList("tag_OFFLINE", "tag_REALTIME"), serverPools,
            18090, 18091, 18092, 18093, false);
    adminClient.getInstanceClient().createInstance(serverInstance2.toJsonString());

    // Check that we have added four instances so far
    checkNumInstances(adminClient, expectedNumInstances + 4);

    // Create duplicate broker and server instances should fail
    assertThrows(RuntimeException.class,
        () -> adminClient.getInstanceClient().createInstance(brokerInstance1.toJsonString()));
    assertThrows(RuntimeException.class,
        () -> adminClient.getInstanceClient().createInstance(serverInstance1.toJsonString()));
    assertThrows(RuntimeException.class,
        () -> adminClient.getInstanceClient().createInstance(brokerInstance2.toJsonString()));
    assertThrows(RuntimeException.class,
        () -> adminClient.getInstanceClient().createInstance(serverInstance2.toJsonString()));

    // Check that number of instances did not change.
    checkNumInstances(adminClient, expectedNumInstances + 4);

    // Check that the instances are properly created
    checkInstanceInfo(adminClient, "Broker_1.2.3.4_1234", "1.2.3.4", 1234, new String[0], null, -1, -1, -1, -1,
        false);
    checkInstanceInfo(adminClient, "Server_1.2.3.4_2345", "1.2.3.4", 2345, new String[0], null, 8090, 8091, 8092, 8093,
        false);
    checkInstanceInfo(adminClient, "Broker_2.3.4.5_1234", "2.3.4.5", 1234, new String[]{"tag_BROKER"}, null, -1, -1,
        -1, -1, false);
    checkInstanceInfo(adminClient, "Server_2.3.4.5_2345", "2.3.4.5", 2345,
        new String[]{"tag_OFFLINE", "tag_REALTIME"}, serverPools, 18090, 18091, 18092, 18093, false);

    // Test PUT instance API
    String newBrokerTag = "new-broker-tag";
    Instance newBrokerInstance =
        new Instance("1.2.3.4", 1234, InstanceType.BROKER, Collections.singletonList(newBrokerTag), null, 0, 0, 0, 0,
            false);
    String brokerInstanceId = "Broker_1.2.3.4_1234";
    adminClient.getInstanceClient().updateInstance(brokerInstanceId, newBrokerInstance.toJsonString());
    String newServerTag = "new-server-tag";
    Instance newServerInstance =
        new Instance("1.2.3.4", 2345, InstanceType.SERVER, Collections.singletonList(newServerTag), null, 28090, 28091,
            28092, 28093, true);
    String serverInstanceId = "Server_1.2.3.4_2345";
    adminClient.getInstanceClient().updateInstance(serverInstanceId, newServerInstance.toJsonString());

    checkInstanceInfo(adminClient, brokerInstanceId, "1.2.3.4", 1234, new String[]{newBrokerTag}, null, -1, -1, -1,
        -1, false);
    checkInstanceInfo(adminClient, serverInstanceId, "1.2.3.4", 2345, new String[]{newServerTag}, null, 28090, 28091,
        28092, 28093, true);

    // Test Instance updateTags API
    adminClient.getInstanceClient()
        .updateInstanceTags(brokerInstanceId, Lists.newArrayList("tag_BROKER", "newTag_BROKER"), false);
    adminClient.getInstanceClient()
        .updateInstanceTags(serverInstanceId, Lists.newArrayList("tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"),
            false);
    checkInstanceInfo(adminClient, brokerInstanceId, "1.2.3.4", 1234, new String[]{"tag_BROKER", "newTag_BROKER"},
        null, -1, -1, -1, -1, false);
    checkInstanceInfo(adminClient, serverInstanceId, "1.2.3.4", 2345,
        new String[]{"tag_REALTIME", "newTag_OFFLINE", "newTag_REALTIME"}, null, 28090, 28091, 28092, 28093, true);

    // Test DELETE instance API
    adminClient.getInstanceClient().dropInstance("Broker_1.2.3.4_1234");
    adminClient.getInstanceClient().dropInstance("Server_1.2.3.4_2345");
    adminClient.getInstanceClient().dropInstance("Broker_2.3.4.5_1234");
    adminClient.getInstanceClient().dropInstance("Server_2.3.4.5_2345");
    checkNumInstances(adminClient, expectedNumInstances);
  }

  private void checkNumInstances(PinotAdminClient adminClient, int expectedNumInstances)
      throws Exception {
    List<String> instances = adminClient.getInstanceClient().listInstances();
    assertEquals(instances.size(), expectedNumInstances);
    long numControllers =
        instances.stream().filter(name -> name.startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE)).count();
    assertEquals(numControllers, 1);
  }

  private void checkInstanceInfo(PinotAdminClient adminClient, String instanceName, String hostName, int port,
      String[] tags,
      @Nullable Map<String, Integer> pools, int grpcPort, int adminPort, int queryServicePort, int queryMailboxPort,
      boolean queriesDisabled)
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(adminClient.getInstanceClient().getInstance(instanceName));
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
      throws Exception {
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
        DEFAULT_INSTANCE.getOrCreateAdminClient().getInstanceClient()
            .validateInstanceTagUpdates(JsonUtils.objectToString(request)),
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
    DEFAULT_INSTANCE.addTableConfig(tableConfig);
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
        DEFAULT_INSTANCE.getOrCreateAdminClient().getInstanceClient()
            .validateInstanceTagUpdates(JsonUtils.objectToString(request)),
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
      throws Exception {
    PinotAdminClient adminClient = DEFAULT_INSTANCE.getOrCreateAdminClient();
    List<String> instances = adminClient.getInstanceClient().listInstances();
    Map<String, List<String>> map = new HashMap<>(instances.size());
    for (String instance : instances) {
      map.put(instance, getInstanceTags(adminClient, instance));
    }
    return map;
  }

  private List<String> getInstanceTags(PinotAdminClient adminClient, String instance)
      throws Exception {
    List<String> tags = new ArrayList<>();
    for (JsonNode tag : JsonUtils.stringToJsonNode(adminClient.getInstanceClient().getInstance(instance)).get("tags")) {
      tags.add(tag.asText());
    }
    return tags;
  }

  @Test
  public void testDrainMinionInstance()
      throws Exception {
    // Create a minion instance with minion_untagged tag
    String createInstanceUrl = _urlBuilder.forInstanceCreate();
    Instance minionInstance =
        new Instance("minion1.test.com", 9514, InstanceType.MINION,
            Collections.singletonList(Helix.UNTAGGED_MINION_INSTANCE),
            null, 0, 0, 0, 0, false);
    sendPostRequest(createInstanceUrl, minionInstance.toJsonString());
    String minionInstanceId = "Minion_minion1.test.com_9514";

    // Verify the minion was created with minion_untagged tag
    checkInstanceInfo(minionInstanceId, "minion1.test.com", 9514, new String[]{Helix.UNTAGGED_MINION_INSTANCE},
        null, -1, -1, -1, -1, false);

    // Drain the minion instance
    String drainUrl = _urlBuilder.forInstanceState(minionInstanceId);
    sendPutRequest(drainUrl + "?state=DRAIN", "");

    // Verify the minion now has minion_drained tag instead of minion_untagged
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_urlBuilder.forInstance(minionInstanceId)));
    assertEquals(response.get("instanceName").asText(), minionInstanceId);
    JsonNode tags = response.get("tags");
    assertEquals(tags.size(), 1);
    assertEquals(tags.get(0).asText(), Helix.DRAINED_MINION_INSTANCE);

    // Cleanup - delete the minion instance
    sendDeleteRequest(_urlBuilder.forInstance(minionInstanceId));
  }

  @Test
  public void testDrainMinionInstanceWithCustomTags()
      throws Exception {
    // Create a minion instance with custom tags - should succeed and replace ALL tags
    String createInstanceUrl = _urlBuilder.forInstanceCreate();
    List<String> tags = Arrays.asList(Helix.UNTAGGED_MINION_INSTANCE, "custom_tag1", "custom_tag2");
    Instance minionInstance =
        new Instance("minion2.test.com", 9515, InstanceType.MINION, tags, null, 0, 0, 0, 0, false);
    sendPostRequest(createInstanceUrl, minionInstance.toJsonString());
    String minionInstanceId = "Minion_minion2.test.com_9515";

    // Drain the minion instance - should succeed
    String drainUrl = _urlBuilder.forInstanceState(minionInstanceId);
    sendPutRequest(drainUrl + "?state=DRAIN", "");

    // Verify ALL tags were replaced with just minion_drained
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_urlBuilder.forInstance(minionInstanceId)));
    JsonNode responseTags = response.get("tags");
    assertEquals(responseTags.size(), 1);
    assertEquals(responseTags.get(0).asText(), Helix.DRAINED_MINION_INSTANCE);

    // Cleanup
    sendDeleteRequest(_urlBuilder.forInstance(minionInstanceId));
  }

  @Test
  public void testDrainMinionInstanceWithOnlyCustomTag()
      throws Exception {
    // Create a minion instance with only custom tag - should succeed and replace it
    String createInstanceUrl = _urlBuilder.forInstanceCreate();
    List<String> tags = Arrays.asList("custom_tag");
    Instance minionInstance =
        new Instance("minion3.test.com", 9516, InstanceType.MINION, tags, null, 0, 0, 0, 0, false);
    sendPostRequest(createInstanceUrl, minionInstance.toJsonString());
    String minionInstanceId = "Minion_minion3.test.com_9516";

    // Drain the minion instance - should succeed
    String drainUrl = _urlBuilder.forInstanceState(minionInstanceId);
    sendPutRequest(drainUrl + "?state=DRAIN", "");

    // Verify tag was replaced with minion_drained
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_urlBuilder.forInstance(minionInstanceId)));
    JsonNode responseTags = response.get("tags");
    assertEquals(responseTags.size(), 1);
    assertEquals(responseTags.get(0).asText(), Helix.DRAINED_MINION_INSTANCE);

    // Cleanup
    sendDeleteRequest(_urlBuilder.forInstance(minionInstanceId));
  }

  @Test
  public void testDrainMinionInstanceAlreadyDrainedFails()
      throws Exception {
    // Create a minion instance that's already drained - should fail to drain again
    String createInstanceUrl = _urlBuilder.forInstanceCreate();
    List<String> tags = Arrays.asList(Helix.DRAINED_MINION_INSTANCE);
    Instance minionInstance =
        new Instance("minion4.test.com", 9517, InstanceType.MINION, tags, null, 0, 0, 0, 0, false);
    sendPostRequest(createInstanceUrl, minionInstance.toJsonString());
    String minionInstanceId = "Minion_minion4.test.com_9517";

    // Attempt to drain the already drained minion instance - should fail
    String drainUrl = _urlBuilder.forInstanceState(minionInstanceId);
    assertThrows(IOException.class, () -> sendPutRequest(drainUrl + "?state=DRAIN", ""));

    // Cleanup
    sendDeleteRequest(_urlBuilder.forInstance(minionInstanceId));
  }

  @Test
  public void testDrainNonMinionInstanceFails()
      throws Exception {
    // Try to drain a broker instance (should fail)
    String brokerInstanceId = "Broker_localhost_1234";
    Instance brokerInstance =
        new Instance("localhost", 1234, InstanceType.BROKER, Collections.singletonList("broker_tag"), null, 0, 0, 0, 0,
            false);
    sendPostRequest(_urlBuilder.forInstanceCreate(), brokerInstance.toJsonString());

    // Attempt to drain the broker - should fail with 400 Bad Request
    String drainUrl = _urlBuilder.forInstanceState(brokerInstanceId);
    assertThrows(IOException.class, () -> sendPutRequest(drainUrl + "?state=DRAIN", ""));

    // Cleanup
    sendDeleteRequest(_urlBuilder.forInstance(brokerInstanceId));
  }

  @Test
  public void testDrainNonExistentMinionFails()
      throws Exception {
    // Try to drain a non-existent minion instance
    String nonExistentMinionId = "Minion_nonexistent_9999";
    String drainUrl = _urlBuilder.forInstanceState(nonExistentMinionId);

    // Should fail with 404 Not Found
    assertThrows(IOException.class, () -> sendPutRequest(drainUrl + "?state=DRAIN", ""));
  }

  @Test
  public void testDrainMinionWithEmptyTags()
      throws Exception {
    // Create a minion instance with no tags
    String createInstanceUrl = _urlBuilder.forInstanceCreate();
    Instance minionInstance =
        new Instance("minion5.test.com", 9518, InstanceType.MINION, null, null, 0, 0, 0, 0, false);
    sendPostRequest(createInstanceUrl, minionInstance.toJsonString());
    String minionInstanceId = "Minion_minion5.test.com_9518";

    // Drain the minion instance
    String drainUrl = _urlBuilder.forInstanceState(minionInstanceId);
    sendPutRequest(drainUrl + "?state=DRAIN", "");

    // Verify minion_drained tag was added
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_urlBuilder.forInstance(minionInstanceId)));
    JsonNode responseTags = response.get("tags");
    assertEquals(responseTags.size(), 1);
    assertEquals(responseTags.get(0).asText(), Helix.DRAINED_MINION_INSTANCE);

    // Cleanup
    sendDeleteRequest(_urlBuilder.forInstance(minionInstanceId));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    DEFAULT_INSTANCE.cleanup();
  }
}
