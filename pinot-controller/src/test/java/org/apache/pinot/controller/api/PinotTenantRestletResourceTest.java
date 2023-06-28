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
import java.io.IOException;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotTenantRestletResourceTest extends ControllerTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TABLE_NAME = "restletTable_OFFLINE";
  private static final String RAW_TABLE_NAME = "toggleTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);


  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testTableListForTenant()
      throws Exception {
    // Check that no tables on tenant works
    String listTablesUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTablesFromTenant(TagNameUtils.DEFAULT_TENANT_NAME);
    JsonNode tableList = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    assertEquals(tableList.get("tables").size(), 0);

    // Add a table
    ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forTableCreate(),
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build().toJsonString());

    // There should be 1 table on the tenant
    tableList = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    JsonNode tables = tableList.get("tables");
    assertEquals(tables.size(), 1);

    // Check to make sure that test table exists.
    boolean found = false;
    for (int i = 0; !found && i < tables.size(); i++) {
      found = tables.get(i).asText().equals(TABLE_NAME);
    }

    assertTrue(found);

    // reset the ZK node to simulate corruption
    ZkHelixPropertyStore<ZNRecord> propertyStore = TEST_INSTANCE.getPropertyStore();
    String zkPath = "/CONFIGS/TABLE/" + TABLE_NAME;
    ZNRecord znRecord = propertyStore.get(zkPath, null, 0);
    propertyStore.set(zkPath, new ZNRecord(znRecord.getId()), 1);

    // Now there should be no tables
    tableList = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    tables = tableList.get("tables");
    assertEquals(tables.size(), 0);
  }

  @Test
  public void testListInstance()
      throws Exception {
    String listInstancesUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTenantGet(TagNameUtils.DEFAULT_TENANT_NAME);
    JsonNode instanceList = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listInstancesUrl));
    assertEquals(instanceList.get("ServerInstances").size(), DEFAULT_NUM_SERVER_INSTANCES);
    assertEquals(instanceList.get("BrokerInstances").size(), DEFAULT_NUM_BROKER_INSTANCES);
  }

  @Test
  public void testToggleTenantState()
    throws Exception {
    // Create an offline table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(DEFAULT_MIN_NUM_REPLICAS)
            .build();
    sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forTableCreate(), tableConfig.toJsonString());
    assertEquals(TEST_INSTANCE.getHelixAdmin()
        .getResourceIdealState(TEST_INSTANCE.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getInstanceSet(OFFLINE_TABLE_NAME).size(), DEFAULT_NUM_BROKER_INSTANCES);

    // Add segments
    for (int i = 0; i < DEFAULT_NUM_SERVER_INSTANCES; i++) {
      TEST_INSTANCE.getHelixResourceManager()
          .addNewSegment(OFFLINE_TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME),
              "downloadUrl");
      assertEquals(TEST_INSTANCE.getHelixAdmin()
          .getResourceIdealState(TEST_INSTANCE.getHelixClusterName(), OFFLINE_TABLE_NAME).getNumPartitions(), i + 1);
    }

    // Disable server instances
    String disableServerInstanceUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME,
            "server", "disable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(disableServerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, 0);

    // Enable server instances
    String enableServerInstanceUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME,
            "server", "enable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(enableServerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, DEFAULT_NUM_SERVER_INSTANCES);

    // Disable broker instances
    String disableBrokerInstanceUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME,
            "broker", "disable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(disableBrokerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, 0);

    // Enable broker instances
    String enableBrokerInstanceUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME,
            "broker", "enable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(enableBrokerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        DEFAULT_NUM_BROKER_INSTANCES);

    // Delete table
    sendDeleteRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forTableDelete(RAW_TABLE_NAME));

    // Check exception in case of enum mismatch of State
    try {
      String mismatchStateBrokerInstanceUrl =
          TEST_INSTANCE.getControllerRequestURLBuilder().forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME,
              "broker", "random");
      sendPostRequest(mismatchStateBrokerInstanceUrl);
      fail("Passing invalid state to tenant toggle state does not fail.");
    } catch (IOException e) {
      // Expected 500 Bad Request
      assertTrue(e.getMessage().contains("Error: State mentioned random is wrong. "
          + "Valid States: Enable, Disable, Drop"));
    }
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
