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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class PinotTableRestletResourceTest {

  @Test
  public void testTweakRealtimeTableConfig() throws Exception {
    try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("table/table_config_with_instance_assignment.json")) {
      ObjectNode tableConfig = (ObjectNode) JsonUtils.inputStreamToJsonNode(inputStream);

      String brokerTenant = "testBroker";
      String serverTenant = "testServer";
      CopyTablePayload copyTablePayload = new CopyTablePayload("http://localhost:9000", null,
          "http://localhost:9000", null, brokerTenant, serverTenant,
          Map.of("server1_REALTIME", "testServer_REALTIME"), null, null);
      PinotTableRestletResource.tweakRealtimeTableConfig(tableConfig, copyTablePayload);

      assertEquals(tableConfig.get("tenants").get("broker").asText(), brokerTenant);
      assertEquals(tableConfig.get("tenants").get("server").asText(), serverTenant);
      assertEquals(tableConfig.path("instanceAssignmentConfigMap").path("CONSUMING").path("tagPoolConfig").path("tag")
          .asText(), serverTenant + "_REALTIME");
    }
  }

  @Test
  public void testCopyTablePayloadJobType() throws Exception {
    // Test 1: Backwards compatibility - null jobType defaults to CONTROLLER
    CopyTablePayload payload1 = new CopyTablePayload("http://src", null, "http://dest", null,
        "broker", "server", null, null, null);
    assertEquals(payload1.getJobType(), CopyTablePayload.JobType.CONTROLLER);

    // Test 2: Explicit CONTROLLER (uppercase)
    String json2 = "{\"sourceClusterUri\":\"http://src\",\"destinationClusterUri\":\"http://dest\","
        + "\"brokerTenant\":\"broker\",\"serverTenant\":\"server\",\"jobType\":\"CONTROLLER\"}";
    CopyTablePayload payload2 = JsonUtils.stringToObject(json2, CopyTablePayload.class);
    assertEquals(payload2.getJobType(), CopyTablePayload.JobType.CONTROLLER);

    // Test 3: MINION enum value deserializes correctly
    String json3 = "{\"sourceClusterUri\":\"http://src\",\"destinationClusterUri\":\"http://dest\","
        + "\"brokerTenant\":\"broker\",\"serverTenant\":\"server\",\"jobType\":\"MINION\"}";
    CopyTablePayload payload3 = JsonUtils.stringToObject(json3, CopyTablePayload.class);
    assertEquals(payload3.getJobType(), CopyTablePayload.JobType.MINION);

    // Test 4: Invalid job type throws exception during deserialization
    String json4 = "{\"sourceClusterUri\":\"http://src\",\"destinationClusterUri\":\"http://dest\","
        + "\"brokerTenant\":\"broker\",\"serverTenant\":\"server\",\"jobType\":\"invalid\"}";
    expectThrows(Exception.class, () -> JsonUtils.stringToObject(json4, CopyTablePayload.class));
  }
}
