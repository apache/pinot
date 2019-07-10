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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Test for table to tenant mapping. Real working test is inside offline cluster integration test because it requires
 * segment upload.
 */
public class PinotTenantRestletResourceTest extends ControllerTest {
  private final TableConfig.Builder _offlineBuilder = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE);
  private static final int NUM_BROKER_INSTANCES = 2;
  private static final int NUM_SERVER_INSTANCES = 6;

  @BeforeClass
  public void setUp() {
    startZk();
    startController();
  }

  @Test
  public void testTableListForTenant()
      throws Exception {
    // Create untagged broker and server instances
    ObjectNode brokerInstance =
        (ObjectNode) JsonUtils.stringToJsonNode("{\"host\":\"1.2.3.4\", \"type\":\"broker\", \"port\":\"1234\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    ObjectNode serverInstance =
        (ObjectNode) JsonUtils.stringToJsonNode("{\"host\":\"1.2.3.4\", \"type\":\"server\", \"port\":\"2345\"}");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    // Create tagged broker and server instances
    brokerInstance.put("tag", "someTag");
    brokerInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), brokerInstance.toString());

    serverInstance.put("tag", "server_REALTIME");
    serverInstance.put("host", "2.3.4.5");
    sendPostRequest(_controllerRequestURLBuilder.forInstanceCreate(), serverInstance.toString());

    // Check that no tables on tenant works
    JsonNode tableList =
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTablesFromTenant("server_REALTIME")));
    assertEquals(tableList.get("tables").size(), 0, "Expected no tables");

    // Try to make sure both kinds of tags work
    tableList = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTablesFromTenant("server")));
    assertEquals(tableList.get("tables").size(), 0, "Expected no tables");

    // Add a table to the server
    String createTableUrl = _controllerRequestURLBuilder.forTableCreate();

    ControllerRequestBuilderUtil
        .addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR,
            NUM_BROKER_INSTANCES, true);
    ControllerRequestBuilderUtil
        .addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR,
            NUM_SERVER_INSTANCES, true);

    _offlineBuilder.setTableName("testOfflineTable").setTimeColumnName("timeColumn").setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5").setServerTenant("DefaultTenant");

    TableConfig offlineTableConfig = _offlineBuilder.build();
    offlineTableConfig.setTableName("mytable_OFFLINE");
    String offlineTableJSONConfigString = offlineTableConfig.toJsonConfigString();
    sendPostRequest(createTableUrl, offlineTableJSONConfigString);

    // Try to make sure both kinds of tags work
    tableList =
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forTablesFromTenant("DefaultTenant")));
    assertEquals(tableList.get("tables").size(), 1, "Expected 1 table");
    assertEquals(tableList.get("tables").get(0).asText(), "mytable_OFFLINE");
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
