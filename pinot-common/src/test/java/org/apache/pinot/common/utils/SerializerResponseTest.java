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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.restlet.resources.ServerSegmentsReloadCheckResponse;
import org.apache.pinot.common.restlet.resources.TableSegmentsReloadCheckResponse;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests some of the serializer and deserialization responses from SegmentsReloadCheckResponse class
 * needReload will have to be carefully evaluated
 */
public class SerializerResponseTest {

  @Test
  public void testSerialization()
      throws IOException {
    // Given
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

    JsonNode serversList =
        tableResponseJsonNode.get("serverToSegmentsCheckReloadList");
    JsonNode serverResp = serversList.get("instance123");
    assertEquals(serverResp.get("instanceId").asText(), "instance123");
    assertTrue(serverResp.get("needReload").asBoolean());

    assertEquals("{\n" + "  \"needReload\" : true,\n" + "  \"serverToSegmentsCheckReloadList\" : {\n"
            + "    \"instance123\" : {\n" + "      \"needReload\" : true,\n" + "      \"instanceId\" : \"instance123\"\n"
            + "    }\n" + "  }\n" + "}",
        tableResponseString);
    assertEquals("{\n" + "  \"needReload\" : true,\n" + "  \"instanceId\" : \"instance123\"\n" + "}", responseString);

  }

  @Test
  public void testDeserialization()
      throws Exception {
    String jsonResponse = "{\n" + "  \"needReload\": false,\n" + "  \"serverToSegmentsCheckReloadList\": {\n"
        + "    \"Server_10.0.0.215_7050\": {\n" + "      \"needReload\": false,\n"
        + "      \"instanceId\": \"Server_10.0.0.215_7050\"\n" + "    }\n" + "  }\n" + "}";
    JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonResponse);
    TableSegmentsReloadCheckResponse tableReloadResponse = JsonUtils.stringToObject(
        jsonResponse,
        new TypeReference<TableSegmentsReloadCheckResponse>() {}
    );
    // Then
    assertNotNull(jsonNode);
    assertFalse(tableReloadResponse.isNeedReload());
    assertNotNull(tableReloadResponse.getServerToSegmentsCheckReloadList());
    Map<String, ServerSegmentsReloadCheckResponse> serverSegmentReloadResp = tableReloadResponse.getServerToSegmentsCheckReloadList();
    assertEquals(serverSegmentReloadResp.get("Server_10.0.0.215_7050").getNeedReload(), false);
  }
}
