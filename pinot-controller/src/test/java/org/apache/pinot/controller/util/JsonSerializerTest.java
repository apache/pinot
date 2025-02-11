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
package org.apache.pinot.controller.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.restlet.resources.ServerSegmentsReloadCheckResponse;
import org.apache.pinot.common.restlet.resources.TableSegmentsReloadCheckResponse;
import org.apache.pinot.controller.api.resources.PinotLeadControllerRestletResource;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Test class to check for json responses during serialization and deserialization
 */
public class JsonSerializerTest {
  @Test
  public void testTableReloadResponseSerialization()
      throws IOException {
    boolean needReload = true;
    String instanceId = "instance123";
    ServerSegmentsReloadCheckResponse response = new ServerSegmentsReloadCheckResponse(needReload, instanceId);
    Map<String, ServerSegmentsReloadCheckResponse> serversResponse = new HashMap<>();
    serversResponse.put(instanceId, response);
    TableSegmentsReloadCheckResponse tableResponse = new TableSegmentsReloadCheckResponse(needReload, serversResponse);
    String responseString = JsonUtils.objectToPrettyString(response);
    String tableResponseString = JsonUtils.objectToPrettyString(tableResponse);

    assertNotNull(responseString);
    assertNotNull(tableResponseString);
    JsonNode tableResponseJsonNode = JsonUtils.stringToJsonNode(tableResponseString);
    assertTrue(tableResponseJsonNode.get("needReload").asBoolean());

    JsonNode serversList = tableResponseJsonNode.get("serverToSegmentsCheckReloadList");
    JsonNode serverResp = serversList.get("instance123");
    assertEquals(serverResp.get("instanceId").asText(), "instance123");
    assertTrue(serverResp.get("needReload").asBoolean());
    //@formatter:off
    assertEquals("{\n"
        + "  \"needReload\" : true,\n"
        + "  \"serverToSegmentsCheckReloadList\" : {\n"
        + "    \"instance123\" : {\n"
        + "      \"needReload\" : true,\n"
        + "      \"instanceId\" : \"instance123\"\n"
        + "    }\n"
        + "  }\n"
        + "}", tableResponseString);
    assertEquals("{\n"
        + "  \"needReload\" : true,\n"
        + "  \"instanceId\" : \"instance123\"\n"
        + "}", responseString);
    //@formatter:on
  }

  @Test
  public void testTableReloadResponseDeserialization()
      throws Exception {
    //@formatter:off
    String jsonResponse = "{\n"
        + "  \"needReload\": false,\n"
        + "  \"serverToSegmentsCheckReloadList\": {\n"
        + "    \"Server_10.0.0.215_7050\": {\n"
        + "      \"needReload\": false,\n"
        + "      \"instanceId\": \"Server_10.0.0.215_7050\"\n"
        + "    }\n"
        + "  }\n"
        + "}\n";
    //@formatter:on
    JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonResponse);
    TableSegmentsReloadCheckResponse tableReloadResponse =
        JsonUtils.stringToObject(jsonResponse, new TypeReference<TableSegmentsReloadCheckResponse>() {
        });
    assertNotNull(jsonNode);
    assertFalse(tableReloadResponse.isNeedReload());
    assertNotNull(tableReloadResponse.getServerToSegmentsCheckReloadList());
    Map<String, ServerSegmentsReloadCheckResponse> serverSegmentReloadResp =
        tableReloadResponse.getServerToSegmentsCheckReloadList();
    assertEquals(serverSegmentReloadResp.get("Server_10.0.0.215_7050").isNeedReload(), false);
  }

  @Test
  public void testLeadControllerResponseSerialization()
      throws IOException {
    boolean leadControllerResourceEnabled = true;
    Map<String, PinotLeadControllerRestletResource.LeadControllerEntry> leadControllerEntryMap = new HashMap<>();
    List<String> tableNames = new ArrayList<>();
    tableNames.add("fineFoodReviews_OFFLINE");
    tableNames.add("dimBaseballTeams_OFFLINE");
    PinotLeadControllerRestletResource.LeadControllerEntry leadControllerEntry =
        new PinotLeadControllerRestletResource.LeadControllerEntry("Controller_192.168.1.148_9000", tableNames);
    leadControllerEntryMap.put("leadControllerResource_0", leadControllerEntry);
    PinotLeadControllerRestletResource.LeadControllerResponse leadControllerResponse =
        new PinotLeadControllerRestletResource.LeadControllerResponse(leadControllerResourceEnabled,
            leadControllerEntryMap);
    String leaderControllerRespStr = JsonUtils.objectToPrettyString(leadControllerResponse);
    String leadControllerEntryRespStr = JsonUtils.objectToPrettyString(leadControllerEntry);
    //@formatter:off
    assertEquals("{\n"
        + "  \"leadControllerResourceEnabled\" : true,\n"
        + "  \"leadControllerEntryMap\" : {\n"
        + "    \"leadControllerResource_0\" : {\n"
        + "      \"leadControllerId\" : \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\" : [ \"fineFoodReviews_OFFLINE\", \"dimBaseballTeams_OFFLINE\" ]\n"
        + "    }\n"
        + "  }\n"
        + "}", leaderControllerRespStr);
    assertEquals("{\n"
            + "  \"leadControllerId\" : \"Controller_192.168.1.148_9000\",\n"
            + "  \"tableNames\" : [ \"fineFoodReviews_OFFLINE\", \"dimBaseballTeams_OFFLINE\" ]\n"
            + "}",
        leadControllerEntryRespStr);
    //@formatter:on
    assertNotNull(leaderControllerRespStr);
    assertNotNull(leadControllerEntryRespStr);
    JsonNode leadControllerRespNodeStr = JsonUtils.stringToJsonNode(leaderControllerRespStr);
    JsonNode leadControllerEntryRespNodeStr = JsonUtils.stringToJsonNode(leadControllerEntryRespStr);

    assertTrue(leadControllerRespNodeStr.get("leadControllerResourceEnabled").asBoolean());
    JsonNode controllerEntryMap = leadControllerRespNodeStr.get("leadControllerEntryMap");
    JsonNode controllerEntry = controllerEntryMap.get("leadControllerResource_0");
    assertEquals(controllerEntry.get("leadControllerId").asText(), "Controller_192.168.1.148_9000");
    assertEquals(controllerEntry.get("tableNames").size(), 2);
    PinotLeadControllerRestletResource.LeadControllerEntry tableReloadResponse =
        JsonUtils.stringToObject(leadControllerEntryRespStr, new TypeReference<>() {
        });
    assertEquals(tableReloadResponse.getLeadControllerId(), "Controller_192.168.1.148_9000");
    assertEquals(tableReloadResponse.getTableNames().size(), 2);
    assertTrue(tableReloadResponse.getTableNames().contains("fineFoodReviews_OFFLINE"));
    assertTrue(tableReloadResponse.getTableNames().contains("dimBaseballTeams_OFFLINE"));
  }

  @Test
  public void testLeadControllerResponseDeserialization()
      throws Exception {
    //@formatter:off
    String jsonResponse = "{\n"
        + "  \"leadControllerResourceEnabled\": true,\n"
        + "  \"leadControllerEntryMap\": {\n"
        + "    \"leadControllerResource_0\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_1\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_2\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_3\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_4\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_5\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_6\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_7\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": [\n"
        + "        \"baseballStats_OFFLINE\"\n"
        + "      ]\n"
        + "    },\n"
        + "    \"leadControllerResource_8\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_9\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_10\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_11\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_12\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_13\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_14\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_15\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_16\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_17\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": [\n"
        + "        \"clickstreamFunnel_OFFLINE\"\n"
        + "      ]\n"
        + "    },\n"
        + "    \"leadControllerResource_18\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_19\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": [\n"
        + "        \"airlineStats_OFFLINE\"\n"
        + "      ]\n"
        + "    },\n"
        + "    \"leadControllerResource_20\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_21\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_22\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    },\n"
        + "    \"leadControllerResource_23\": {\n"
        + "      \"leadControllerId\": \"Controller_192.168.1.148_9000\",\n"
        + "      \"tableNames\": []\n"
        + "    }\n"
        + "  }\n"
        + "}";
    //@formatter:on
    JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonResponse);
    PinotLeadControllerRestletResource.LeadControllerResponse tableReloadResponse =
        JsonUtils.stringToObject(jsonResponse, new TypeReference<>() {
        });
    assertNotNull(jsonNode);
    assertTrue(tableReloadResponse.isLeadControllerResourceEnabled());
    assertNotNull(tableReloadResponse.getLeadControllerEntryMap());
    Map<String, PinotLeadControllerRestletResource.LeadControllerEntry> serverSegmentReloadResp =
        tableReloadResponse.getLeadControllerEntryMap();
    assertTrue(
        serverSegmentReloadResp.get("leadControllerResource_17").getTableNames().contains("clickstreamFunnel_OFFLINE"));
  }
}
