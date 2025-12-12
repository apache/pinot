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

public class PinotTableRestletResourceTest {

  @Test
  public void testTweakRealtimeTableConfig() throws Exception {
    try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("table/table_config_with_instance_assignment.json")) {
      ObjectNode tableConfig = (ObjectNode) JsonUtils.inputStreamToJsonNode(inputStream);

      String brokerTenant = "testBroker";
      String serverTenant = "testServer";
      PinotTableRestletResource.tweakRealtimeTableConfig(tableConfig, brokerTenant, serverTenant,
          Map.of("server1_REALTIME", "testServer_REALTIME"));

      assertEquals(tableConfig.get("tenants").get("broker").asText(), brokerTenant);
      assertEquals(tableConfig.get("tenants").get("server").asText(), serverTenant);
      assertEquals(tableConfig.path("instanceAssignmentConfigMap").path("CONSUMING").path("tagPoolConfig").path("tag")
          .asText(), serverTenant + "_REALTIME");
    }
  }
}
