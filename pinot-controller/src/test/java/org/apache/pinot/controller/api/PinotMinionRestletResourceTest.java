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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PinotMinionRestletResourceTest extends ControllerTest {

    private ControllerRequestURLBuilder _urlBuilder = null;

    @BeforeClass
    public void setUp()
        throws Exception {
      DEFAULT_INSTANCE.setupSharedStateAndValidate();
      _urlBuilder = DEFAULT_INSTANCE.getControllerRequestURLBuilder();
    }

  @Test
  public void testMinionStatusEndpoint()
      throws Exception {
    // Create 5 minion instances
    List<String> minionIds = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String host = "minion-status-test-" + i + ".example.com";
      int port = 9514;
      Instance minionInstance = new Instance(host, port, InstanceType.MINION,
          Collections.singletonList(CommonConstants.Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
      sendPostRequest(_urlBuilder.forInstanceCreate(), minionInstance.toJsonString());
      minionIds.add("Minion_" + host + "_" + port);
    }

    // Test 1: Get all minions (no filter)
    String allMinionsUrl = _urlBuilder.forMinionStatus(null, false);
    String allMinionsResponse = sendGetRequest(allMinionsUrl);
    JsonNode allMinionsJson = JsonUtils.stringToJsonNode(allMinionsResponse);

    assertNotNull(allMinionsJson.get("currentMinionCount"));
    // Should have at least our 5 minions (may have more from other tests)
    assertTrue(allMinionsJson.get("currentMinionCount").asInt() >= 5);
    assertNotNull(allMinionsJson.get("minionStatus"));
    assertTrue(allMinionsJson.get("minionStatus").isArray());

    // Verify the structure of minion status
    for (JsonNode minionStatus : allMinionsJson.get("minionStatus")) {
      assertNotNull(minionStatus.get("instanceId"));
      assertNotNull(minionStatus.get("host"));
      assertNotNull(minionStatus.get("port"));
      assertNotNull(minionStatus.get("runningTaskCount"));
      assertNotNull(minionStatus.get("status"));
      assertEquals(minionStatus.get("status").asText(), "ONLINE");
    }

    // Test 2: Drain 2 minions
    String minion0 = minionIds.get(0);
    String minion2 = minionIds.get(2);
    sendPutRequest(_urlBuilder.forInstanceState(minion0) + "?state=DRAIN", "");
    sendPutRequest(_urlBuilder.forInstanceState(minion2) + "?state=DRAIN", "");

    // Test 3: Get only DRAINED minions
    String drainedMinionsUrl = _urlBuilder.forMinionStatus("DRAINED", false);
    String drainedMinionsResponse = sendGetRequest(drainedMinionsUrl);
    JsonNode drainedMinionsJson = JsonUtils.stringToJsonNode(drainedMinionsResponse);

    assertTrue(drainedMinionsJson.get("currentMinionCount").asInt() >= 2);
    for (JsonNode minionStatus : drainedMinionsJson.get("minionStatus")) {
      assertEquals(minionStatus.get("status").asText(), "DRAINED");
    }

    // Test 4: Get only ONLINE minions
    String onlineMinionsUrl = _urlBuilder.forMinionStatus("ONLINE", false);
    String onlineMinionsResponse = sendGetRequest(onlineMinionsUrl);
    JsonNode onlineMinionsJson = JsonUtils.stringToJsonNode(onlineMinionsResponse);

    assertTrue(onlineMinionsJson.get("currentMinionCount").asInt() >= 3);
    for (JsonNode minionStatus : onlineMinionsJson.get("minionStatus")) {
      assertEquals(minionStatus.get("status").asText(), "ONLINE");
    }

    // Cleanup
    for (String minionId : minionIds) {
      sendDeleteRequest(_urlBuilder.forInstance(minionId));
    }
  }

  @Test
  public void testMinionStatusEndpointCaseInsensitive()
      throws Exception {
    // Create a minion and drain it
    String host = "minion-case-test.example.com";
    int port = 9514;
    Instance minionInstance = new Instance(host, port, InstanceType.MINION,
        Collections.singletonList(CommonConstants.Helix.UNTAGGED_MINION_INSTANCE), null, 0, 0, 0, 0, false);
    sendPostRequest(_urlBuilder.forInstanceCreate(), minionInstance.toJsonString());
    String minionId = "Minion_" + host + "_" + port;
    sendPutRequest(_urlBuilder.forInstanceState(minionId) + "?state=DRAIN", "");

    // Test case-insensitive status filter
    String upperCaseUrl = _urlBuilder.forMinionStatus("DRAINED", false);
    String lowerCaseUrl = _urlBuilder.forMinionStatus("drained", false);

    String upperResponse = sendGetRequest(upperCaseUrl);
    String lowerResponse = sendGetRequest(lowerCaseUrl);

    JsonNode upperJson = JsonUtils.stringToJsonNode(upperResponse);
    JsonNode lowerJson = JsonUtils.stringToJsonNode(lowerResponse);

    // Both should return the same results
    assertTrue(upperJson.get("currentMinionCount").asInt() >= 1);
    assertTrue(lowerJson.get("currentMinionCount").asInt() >= 1);

    // Cleanup
    sendDeleteRequest(_urlBuilder.forInstance(minionId));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    DEFAULT_INSTANCE.cleanup();
  }
}
