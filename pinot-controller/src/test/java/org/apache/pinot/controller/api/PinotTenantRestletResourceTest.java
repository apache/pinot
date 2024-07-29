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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotTenantRestletResourceTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTale";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  private ControllerRequestURLBuilder _urlBuilder;

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
    _urlBuilder = DEFAULT_INSTANCE.getControllerRequestURLBuilder();
  }

  @Test
  public void testTableListForTenant()
      throws Exception {
    // Check that there is no existing tables
    String listTablesUrl = _urlBuilder.forTablesFromTenant(TagNameUtils.DEFAULT_TENANT_NAME);
    JsonNode listTablesResponse = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    assertTrue(listTablesResponse.get("tables").isEmpty());

    // Add 2 brokers with non-default broker tag
    String createInstanceUrl = _urlBuilder.forInstanceCreate();
    String brokerTenant = "test";
    String brokerTag = TagNameUtils.getBrokerTagForTenant(brokerTenant);
    Instance brokerInstance1 =
        new Instance("1.2.3.4", 1234, InstanceType.BROKER, Collections.singletonList(brokerTag), null, 0, 0, 0, 0,
            false);
    Instance brokerInstance2 =
        new Instance("2.3.4.5", 2345, InstanceType.BROKER, Collections.singletonList(brokerTag), null, 0, 0, 0, 0,
            false);
    sendPostRequest(createInstanceUrl, brokerInstance1.toJsonString());
    sendPostRequest(createInstanceUrl, brokerInstance2.toJsonString());

    // Add a table to the default tenant
    String createSchemaUrl = _urlBuilder.forSchemaCreate();
    ControllerTest.sendPostRequest(createSchemaUrl, createDummySchema(RAW_TABLE_NAME).toSingleLineJsonString());
    String createTableUrl = _urlBuilder.forTableCreate();
    ControllerTest.sendPostRequest(createTableUrl,
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build().toJsonString());

    // Add a second table to the non-default tenant
    String rawTableName2 = "testTable2";
    String offlineTableName2 = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName2);
    ControllerTest.sendPostRequest(createSchemaUrl, createDummySchema(rawTableName2).toSingleLineJsonString());
    ControllerTest.sendPostRequest(createTableUrl,
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName2).setBrokerTenant(brokerTenant).build()
            .toJsonString());

    // There should be 2 tables returned when querying default tenant for servers w/o specifying ?type=server
    listTablesResponse = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    JsonNode tables = listTablesResponse.get("tables");
    assertEquals(tables.size(), 2);
    Set<String> tableSet = new HashSet<>();
    tableSet.add(tables.get(0).asText());
    tableSet.add(tables.get(1).asText());
    assertEquals(tableSet, Sets.newHashSet(OFFLINE_TABLE_NAME, offlineTableName2));

    // There should be 2 tables returned when specifying ?type=server as that is the default
    listTablesUrl = _urlBuilder.forTablesFromTenant(TagNameUtils.DEFAULT_TENANT_NAME, "server");
    listTablesResponse = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    tables = listTablesResponse.get("tables");
    assertEquals(tables.size(), 2);
    tableSet = new HashSet<>();
    tableSet.add(tables.get(0).asText());
    tableSet.add(tables.get(1).asText());
    assertEquals(tableSet, Sets.newHashSet(OFFLINE_TABLE_NAME, offlineTableName2));

    // There should be only 1 table returned when specifying ?type=broker for the default tenant
    listTablesUrl = _urlBuilder.forTablesFromTenant(TagNameUtils.DEFAULT_TENANT_NAME, "broker");
    listTablesResponse = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    tables = listTablesResponse.get("tables");
    assertEquals(tables.size(), 1);
    assertEquals(tables.get(0).asText(), OFFLINE_TABLE_NAME);

    // There should be only 1 table returned when specifying ?type=broker for the non-default tenant
    listTablesUrl = _urlBuilder.forTablesFromTenant(brokerTenant, "broker");
    listTablesResponse = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    tables = listTablesResponse.get("tables");
    assertEquals(tables.size(), 1);
    assertEquals(tables.get(0).asText(), offlineTableName2);

    // Remove the tables and brokers
    DEFAULT_INSTANCE.waitForEVToAppear(OFFLINE_TABLE_NAME);
    DEFAULT_INSTANCE.waitForEVToAppear(offlineTableName2);
    DEFAULT_INSTANCE.dropOfflineTable(RAW_TABLE_NAME);
    DEFAULT_INSTANCE.deleteSchema(RAW_TABLE_NAME);
    DEFAULT_INSTANCE.dropOfflineTable(rawTableName2);
    DEFAULT_INSTANCE.deleteSchema(rawTableName2);
    DEFAULT_INSTANCE.waitForEVToDisappear(OFFLINE_TABLE_NAME);
    DEFAULT_INSTANCE.waitForEVToDisappear(offlineTableName2);
    sendDeleteRequest(_urlBuilder.forInstance("Broker_1.2.3.4_1234"));
    sendDeleteRequest(_urlBuilder.forInstance("Broker_2.3.4.5_2345"));
  }

  @Test
  public void testListInstance()
      throws Exception {
    String listInstancesUrl = _urlBuilder.forTenantGet(TagNameUtils.DEFAULT_TENANT_NAME);
    JsonNode instanceList = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listInstancesUrl));
    assertEquals(instanceList.get("ServerInstances").size(), DEFAULT_NUM_SERVER_INSTANCES);
    assertEquals(instanceList.get("BrokerInstances").size(), DEFAULT_NUM_BROKER_INSTANCES);
  }

  @Test
  public void testToggleTenantState()
      throws Exception {
    // Create an offline table
    String createSchemaUrl = _urlBuilder.forSchemaCreate();
    ControllerTest.sendPostRequest(createSchemaUrl, createDummySchema(RAW_TABLE_NAME).toSingleLineJsonString());
    String createTableUrl = _urlBuilder.forTableCreate();
    sendPostRequest(createTableUrl,
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(DEFAULT_MIN_NUM_REPLICAS)
            .build().toJsonString());

    // Broker resource should be updated
    HelixAdmin helixAdmin = DEFAULT_INSTANCE.getHelixAdmin();
    String clusterName = DEFAULT_INSTANCE.getHelixClusterName();
    assertEquals(helixAdmin.getResourceIdealState(clusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getInstanceSet(OFFLINE_TABLE_NAME).size(), DEFAULT_NUM_BROKER_INSTANCES);

    // Add segments
    PinotHelixResourceManager resourceManager = DEFAULT_INSTANCE.getHelixResourceManager();
    for (int i = 0; i < DEFAULT_NUM_SERVER_INSTANCES; i++) {
      resourceManager.addNewSegment(OFFLINE_TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME),
          "downloadUrl");
      assertEquals(helixAdmin.getResourceIdealState(clusterName, OFFLINE_TABLE_NAME).getNumPartitions(), i + 1);
    }

    // Disable server instances
    String disableServerInstanceUrl =
        _urlBuilder.forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME, "server", "disable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(disableServerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, 0);

    // Enable server instances
    String enableServerInstanceUrl =
        _urlBuilder.forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME, "server", "enable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(enableServerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, DEFAULT_NUM_SERVER_INSTANCES);

    // Disable broker instances
    String disableBrokerInstanceUrl =
        _urlBuilder.forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME, "broker", "disable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(disableBrokerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, 0);

    // Enable broker instances
    String enableBrokerInstanceUrl =
        _urlBuilder.forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME, "broker", "enable");
    JsonUtils.stringToJsonNode(ControllerTest.sendPostRequest(enableBrokerInstanceUrl));
    checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        DEFAULT_NUM_BROKER_INSTANCES);

    // Check exception in case of enum mismatch of State
    try {
      String mismatchStateBrokerInstanceUrl =
          _urlBuilder.forTenantInstancesToggle(TagNameUtils.DEFAULT_TENANT_NAME, "broker", "random");
      sendPostRequest(mismatchStateBrokerInstanceUrl);
      fail("Passing invalid state to tenant toggle state does not fail.");
    } catch (IOException e) {
      // Expected 500 Bad Request
      assertTrue(e.getMessage().contains("Error: State mentioned random is wrong. Valid States: Enable, Disable"));
    }

    // Delete table and schema
    DEFAULT_INSTANCE.dropOfflineTable(RAW_TABLE_NAME);
    DEFAULT_INSTANCE.deleteSchema(RAW_TABLE_NAME);
    DEFAULT_INSTANCE.waitForEVToDisappear(OFFLINE_TABLE_NAME);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    DEFAULT_INSTANCE.cleanup();
  }
}
