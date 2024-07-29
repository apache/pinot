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
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "stateless")
public class PinotBrokerRestletResourceStatelessTest extends ControllerTest {
  private static final String TABLE_NAME_1 = "testTable1";
  private static final String TABLE_NAME_2 = "testTable2";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size(),
        1);
  }

  private void testGetBrokersHelper(String state, int onlineServers, int offlineServers)
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
        JsonUtils.stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokersGet(state)), Map.class);

    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(allMap.get("tenants").get("DefaultTenant").contains(expectedBroker));
      Assert.assertTrue(allMap.get("tables").get("testTable1").contains(expectedBroker));
      Assert.assertTrue(allMap.get("tables").get("testTable2").contains(expectedBroker));
    }

    Map<String, List<String>> tenantsMap =
        JsonUtils.stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTenantsGet(state)), Map.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tenantsMap.get("DefaultTenant").contains(expectedBroker));
    }

    List<String> tenantBrokers = JsonUtils
        .stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTenantGet("DefaultTenant", state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tenantBrokers.contains(expectedBroker));
    }

    try {
      sendGetRequest(_controllerRequestURLBuilder.forBrokerTenantGet("nonExistTenant", state));
      Assert.fail("Shouldn't reach here");
    } catch (Exception e) {
    }

    Map<String, List<String>> tablesMap =
        JsonUtils.stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTablesGet(state)), Map.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tablesMap.get("testTable1").contains(expectedBroker));
      Assert.assertTrue(tablesMap.get("testTable2").contains(expectedBroker));
    }

    List<String> tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTableGet("testTable1", "OFFLINE", state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTableGet("testTable1", null, state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTableGet("testTable2", "OFFLINE", state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    tableBrokers = JsonUtils
        .stringToObject(sendGetRequest(_controllerRequestURLBuilder.forBrokerTableGet("testTable2", null, state)),
            List.class);
    for (String expectedBroker : expectedBrokers) {
      Assert.assertTrue(tableBrokers.contains(expectedBroker));
    }
    try {
      sendGetRequest(_controllerRequestURLBuilder.forBrokerTableGet("nonExistTable", null, state));
      Assert.fail("Shouldn't reach here");
    } catch (Exception e) {
    }
  }

  @Test
  public void testGetBrokers()
      throws Exception {
    addFakeBrokerInstancesToAutoJoinHelixCluster(10, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size(),
        10);

    // Create schema
    addDummySchema(TABLE_NAME_1);
    addDummySchema(TABLE_NAME_2);

    // Adding table
    _helixResourceManager
        .addTable(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_1).setNumReplicas(1).build());
    _helixResourceManager
        .addTable(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME_2).setNumReplicas(1).build());

    // Wait for the table addition
    while (!_helixResourceManager.hasOfflineTable(TABLE_NAME_1) && !_helixResourceManager
        .hasOfflineTable(TABLE_NAME_2)) {
      Thread.sleep(100);
    }

    testGetBrokersHelper(null, 10, 0);
    testGetBrokersHelper("ONLINE", 10, 0);
    testGetBrokersHelper("OFFLINE", 10, 0);
    for (int i = 9; i >= 0; i--) {
      stopFakeInstance(BROKER_INSTANCE_ID_PREFIX + i);
      testGetBrokersHelper(null, i, 10 - i);
      testGetBrokersHelper("ONLINE", i, 10 - i);
      testGetBrokersHelper("OFFLINE", i, 10 - i);
    }
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
