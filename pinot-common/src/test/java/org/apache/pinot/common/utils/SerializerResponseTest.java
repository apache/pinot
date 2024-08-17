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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentsReloadCheckResponse;
import org.apache.pinot.common.restlet.resources.TableSegmentsReloadCheckResponse;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests some of the serializer and deserialization responses from SegmentsReloadCheckResponse class
 * needReload will have to be carefully evaluated
 */
public class SerializerResponseTest {

  @Test
  public void testSerialization() {
    // Given
    boolean needReload = true;
    String instanceId = "instance123";
    SegmentsReloadCheckResponse response = new SegmentsReloadCheckResponse(needReload, instanceId);
    TableSegmentsReloadCheckResponse tableResponse = new TableSegmentsReloadCheckResponse(needReload, new HashMap<>());
    String responseString = ResourceUtils.convertToJsonString(response);
    String tableResponseString = ResourceUtils.convertToJsonString(tableResponse);

    assertNotNull(responseString);
    assertNotNull(tableResponseString);
    assertEquals("{\n" + "  \"needReload\" : true,\n" + "  \"serverToSegmentsReloadList\" : { }\n" + "}",
        tableResponseString);
    assertEquals("{\n" + "  \"needReload\" : true,\n" + "  \"instanceId\" : \"instance123\"\n" + "}", responseString);
  }

  @Test
  public void testDeserialization()
      throws Exception {
    // Given
    boolean needReload = true;
    String instanceId = "instance123";
    SegmentsReloadCheckResponse response = new SegmentsReloadCheckResponse(needReload, instanceId);
    TableSegmentsReloadCheckResponse tableResponse = new TableSegmentsReloadCheckResponse(needReload, new HashMap<>());
    String responseString = ResourceUtils.convertToJsonString(response);
    JsonNode jsonNode = JsonUtils.stringToJsonNode(responseString);
    String jsonResponse = JsonUtils.objectToPrettyString(tableResponse);
    JsonNode jsonNodeTableResponse = JsonUtils.stringToJsonNode(jsonResponse);
    // Then
    assertNotNull(jsonNode);
    assertNotNull(jsonNodeTableResponse);
    assertTrue(jsonNodeTableResponse.get("needReload").asBoolean());
    assertTrue(jsonNode.get("needReload").asBoolean());
    assertEquals("instance123", jsonNode.get("instanceId").asText());
  }
}
