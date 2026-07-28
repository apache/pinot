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
package org.apache.pinot.query.runtime.blocks;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ErrorMseBlockTest {

  @Test
  public void toStringIncludesProvenance()
      throws Exception {
    ErrorMseBlock block =
        new ErrorMseBlock(3, 7, "Server_localhost_8000", Map.of(QueryErrorCode.INTERNAL, "boom"));

    JsonNode json = JsonUtils.stringToJsonNode(block.toString());

    assertEquals(json.get("type").asText(), "error");
    // stage/worker/server are surfaced so downstream logs can locate where the failure originated.
    assertEquals(json.get("stageId").asInt(), 3);
    assertEquals(json.get("workerId").asInt(), 7);
    assertEquals(json.get("serverId").asText(), "Server_localhost_8000");
    assertTrue(json.get("errorMessages").toString().contains("boom"));
  }

  @Test
  public void toStringHandlesUnknownProvenance()
      throws Exception {
    // The deprecated constructor leaves the origin undetermined: stageId/workerId = -1, serverId = null.
    @SuppressWarnings("deprecation")
    ErrorMseBlock block = new ErrorMseBlock(Map.of(QueryErrorCode.UNKNOWN, "no context"));

    JsonNode json = JsonUtils.stringToJsonNode(block.toString());

    assertEquals(json.get("stageId").asInt(), -1);
    assertEquals(json.get("workerId").asInt(), -1);
    assertTrue(json.get("serverId").isNull(), "serverId should serialize as null when the origin is unknown");
  }
}
