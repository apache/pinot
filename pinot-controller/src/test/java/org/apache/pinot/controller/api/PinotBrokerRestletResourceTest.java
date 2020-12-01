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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerTestUtils.*;

public class PinotBrokerRestletResourceTest {
  private static final String TABLE_NAME_1 = "testTable1";
  private static final String TABLE_NAME_2 = "testTable2";

  @BeforeClass
  public void setUp()
      throws Exception {
    validate();
  }

  public void testGetBrokersHelper(String state, int onlineServers, int offlineServers)
      throws Exception {
    List<String> expectedBrokers = new ArrayList<>();
    if (state == null) {
      for (int i = 0; i < onlineServers + offlineServers; i++) {
        expectedBrokers.add("Broker_localhost_" + i);
      }
    } else {
      switch (state) {
        case "OFFLINE":
          for (int i = onlineServers; i < onlineServers + offlineServers; i++) {
            expectedBrokers.add("Broker_localhost_" + i);
          }
          break;
        default:
          for (int i = 0; i < onlineServers; i++) {
            expectedBrokers.add("Broker_localhost_" + i);
          }
          break;
      }
    }
    Map<String, Map<String, List<String>>> allMap =
        JsonUtils.stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokersGet(state)), Map.class);

    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(allMap.get("tenants").get("DefaultTenant").contains(expectedBroker));
      Assert.assertTrue(allMap.get("tables").get(TABLE_NAME_1).contains(expectedBroker));
      Assert.assertTrue(allMap.get("tables").get(TABLE_NAME_2).contains(expectedBroker));
    }

    Map<String, List<String>> tenantsMap =
        JsonUtils.stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokerTenantsGet(state)), Map.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tenantsMap.get("DefaultTenant").contains(expectedBroker));
    }

    List<String> tenantBrokers = JsonUtils
        .stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokerTenantGet("DefaultTenant", state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tenantBrokers.contains(expectedBroker));
    }

    try {
      sendGetRequest(getControllerRequestURLBuilder().forBrokerTenantGet("nonExistTenant", state));
      Assert.fail("Shouldn't reach here");
    } catch (Exception e) {
    }

    Map<String, List<String>> tablesMap =
        JsonUtils.stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokerTablesGet(state)), Map.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tablesMap.get(TABLE_NAME_1).contains(expectedBroker));
      Assert.assertTrue(tablesMap.get(TABLE_NAME_2).contains(expectedBroker));
    }

    List<String> tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokerTableGet(TABLE_NAME_1, "OFFLINE", state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokerTableGet(TABLE_NAME_1, null, state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokerTableGet(TABLE_NAME_2, "OFFLINE", state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(getControllerRequestURLBuilder().forBrokerTableGet(TABLE_NAME_2, null, state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    try {
      sendGetRequest(getControllerRequestURLBuilder().forBrokerTableGet("nonExistTable", null, state));
      Assert.fail("Shouldn't reach here");
    } catch (Exception e) {
    }
  }

  /**
   * TODO: Enabling this test will cause random failures in other test cases, because this test calls stopFakeInstance.
   */
  @Test(enabled = false)
  public void testGetBrokers()
      throws Exception {
    // Adding table
    getHelixResourceManager()
        .addTable(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_1).setNumReplicas(1).build());
    getHelixResourceManager()
        .addTable(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_2).setNumReplicas(1).build());

    // Wait for the table addition
    while (!getHelixResourceManager().hasOfflineTable(TABLE_NAME_1) && !getHelixResourceManager()
        .hasOfflineTable(TABLE_NAME_2)) {
      Thread.sleep(1000);
    }

    testGetBrokersHelper(null, NUM_BROKER_INSTANCES, 0);
    testGetBrokersHelper("ONLINE", NUM_BROKER_INSTANCES, 0);
    testGetBrokersHelper("OFFLINE", NUM_BROKER_INSTANCES, 0);
    for (int i = NUM_BROKER_INSTANCES; i >= 0; i--) {
      stopFakeInstance(BROKER_INSTANCE_ID_PREFIX + i);
      testGetBrokersHelper(null, i, NUM_BROKER_INSTANCES - i);
      testGetBrokersHelper("ONLINE", i, NUM_BROKER_INSTANCES - i);
      testGetBrokersHelper("OFFLINE", i, NUM_BROKER_INSTANCES - i);
    }
  }

  @AfterClass
  public void tearDown() {
    cleanup();
  }
}
