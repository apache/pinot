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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


public class PinotTenantRestletResourceTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTale";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testTableListForTenant()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    // Check that there is no existing tables
    JsonNode listTablesResponse =
        JsonUtils.stringToJsonNode(adminClient.getTenantClient().getTenantTables(TagNameUtils.DEFAULT_TENANT_NAME,
            null, false));
    assertTrue(listTablesResponse.get("tables").isEmpty());

    // Add 2 brokers with non-default broker tag
    String brokerTenant = "test";
    String brokerTag = TagNameUtils.getBrokerTagForTenant(brokerTenant);
    Instance brokerInstance1 =
        new Instance("1.2.3.4", 1234, InstanceType.BROKER, List.of(brokerTag), null, 0, 0, 0, 0,
            false);
    Instance brokerInstance2 =
        new Instance("2.3.4.5", 2345, InstanceType.BROKER, List.of(brokerTag), null, 0, 0, 0, 0,
            false);
    adminClient.getInstanceClient().createInstance(brokerInstance1.toJsonString());
    adminClient.getInstanceClient().createInstance(brokerInstance2.toJsonString());

    // Add a table to the default tenant
    adminClient.getSchemaClient().createSchema(createDummySchema(RAW_TABLE_NAME).toSingleLineJsonString());
    adminClient.getTableClient()
        .createTable(new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build().toJsonString(),
            null);

    // Add a second table to the non-default tenant
    String rawTableName2 = "testTable2";
    String offlineTableName2 = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName2);
    adminClient.getSchemaClient().createSchema(createDummySchema(rawTableName2).toSingleLineJsonString());
    adminClient.getTableClient().createTable(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName2).setBrokerTenant(brokerTenant).build()
            .toJsonString(), null);

    // There should be 2 tables returned when querying default tenant for servers w/o specifying ?type=server
    listTablesResponse = JsonUtils.stringToJsonNode(adminClient.getTenantClient()
        .getTenantTables(TagNameUtils.DEFAULT_TENANT_NAME, null, false));
    JsonNode tables = listTablesResponse.get("tables");
    assertEquals(tables.size(), 2);
    Set<String> tableSet = new HashSet<>();
    tableSet.add(tables.get(0).asText());
    tableSet.add(tables.get(1).asText());
    assertEquals(tableSet, Sets.newHashSet(OFFLINE_TABLE_NAME, offlineTableName2));

    // There should be 2 tables returned when specifying ?type=server as that is the default
    listTablesResponse = JsonUtils.stringToJsonNode(
        adminClient.getTenantClient().getTenantTables(TagNameUtils.DEFAULT_TENANT_NAME, "server", false));
    tables = listTablesResponse.get("tables");
    assertEquals(tables.size(), 2);
    tableSet = new HashSet<>();
    tableSet.add(tables.get(0).asText());
    tableSet.add(tables.get(1).asText());
    assertEquals(tableSet, Sets.newHashSet(OFFLINE_TABLE_NAME, offlineTableName2));

    // There should be only 1 table returned when specifying ?type=broker for the default tenant
    listTablesResponse = JsonUtils.stringToJsonNode(
        adminClient.getTenantClient().getTenantTables(TagNameUtils.DEFAULT_TENANT_NAME, "broker", false));
    tables = listTablesResponse.get("tables");
    assertEquals(tables.size(), 1);
    assertEquals(tables.get(0).asText(), OFFLINE_TABLE_NAME);

    // There should be only 1 table returned when specifying ?type=broker for the non-default tenant
    listTablesResponse = JsonUtils.stringToJsonNode(
        adminClient.getTenantClient().getTenantTables(brokerTenant, "broker", false));
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
    adminClient.getInstanceClient().dropInstance("Broker_1.2.3.4_1234");
    adminClient.getInstanceClient().dropInstance("Broker_2.3.4.5_2345");
  }

  @Test
  public void testListInstance()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    JsonNode instanceList = JsonUtils.stringToJsonNode(
        adminClient.getTenantClient().getTenantInstances(TagNameUtils.DEFAULT_TENANT_NAME, null, null));
    assertEquals(instanceList.get("ServerInstances").size(), DEFAULT_NUM_SERVER_INSTANCES);
    assertEquals(instanceList.get("BrokerInstances").size(), DEFAULT_NUM_BROKER_INSTANCES);
  }

  @Test
  public void testToggleTenantState()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    // Create an offline table
    adminClient.getSchemaClient().createSchema(createDummySchema(RAW_TABLE_NAME).toSingleLineJsonString());
    adminClient.getTableClient().createTable(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(DEFAULT_MIN_NUM_REPLICAS)
            .build().toJsonString(), null);

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
    adminClient.getTenantClient().setTenantState(TagNameUtils.DEFAULT_TENANT_NAME, "server", "disable");
    checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, 0);

    // Enable server instances
    adminClient.getTenantClient().setTenantState(TagNameUtils.DEFAULT_TENANT_NAME, "server", "enable");
    checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, DEFAULT_NUM_SERVER_INSTANCES);

    // Disable broker instances
    adminClient.getTenantClient().setTenantState(TagNameUtils.DEFAULT_TENANT_NAME, "broker", "disable");
    checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, 0);

    // Enable broker instances
    adminClient.getTenantClient().setTenantState(TagNameUtils.DEFAULT_TENANT_NAME, "broker", "enable");
    checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        DEFAULT_NUM_BROKER_INSTANCES);

    // Check exception in case of enum mismatch of State
    try {
      adminClient.getTenantClient().setTenantState(TagNameUtils.DEFAULT_TENANT_NAME, "broker", "random");
      fail("Passing invalid state to tenant toggle state does not fail.");
    } catch (Throwable e) {
      // PinotAdminClient wraps validation errors in PinotAdminException or a transport RuntimeException
      String message = e.getMessage();
      assertTrue(message != null
          && message.contains("State mentioned random is wrong. Valid States: Enable, Disable"));
    }

    // Delete table and schema
    DEFAULT_INSTANCE.dropOfflineTable(RAW_TABLE_NAME);
    DEFAULT_INSTANCE.deleteSchema(RAW_TABLE_NAME);
    DEFAULT_INSTANCE.waitForEVToDisappear(OFFLINE_TABLE_NAME);
  }

  @Test
  public void testTenantQuotaConfig()
      throws Exception {
    String tenant = TagNameUtils.DEFAULT_TENANT_NAME;
    String baseUrl = DEFAULT_INSTANCE.getControllerBaseApiUrl();
    String url = baseUrl + "/tenants/" + tenant + "/quotas";
    PinotHelixResourceManager resourceManager = DEFAULT_INSTANCE.getHelixResourceManager();

    try {
      // Not set yet
      assertHttpStatus(() -> sendGetRequest(url), 404);
      assertHttpStatus(() -> sendDeleteRequest(url), 404);

      // Set and read back
      QuotaConfig quotaConfig = new QuotaConfig("10G", null, QuotaEnforcementMode.SHADOW, null);
      sendPutRequest(url, quotaConfig.toJsonString());
      assertEquals(JsonUtils.stringToObject(sendGetRequest(url), QuotaConfig.class), quotaConfig);
      assertEquals(resourceManager.getTenantQuotaConfig(tenant), quotaConfig);

      // Overwrite, with both storage and query quota since the default tenant is both a server and a broker tenant.
      // Raising the storage limit without a mode keeps the SHADOW mode, so it does not start enforcing it.
      QuotaConfig newQuotaConfig = new QuotaConfig("20G", "100", QuotaEnforcementMode.SHADOW, null);
      sendPutRequest(url, new QuotaConfig("20G", "100").toJsonString());
      assertEquals(resourceManager.getTenantQuotaConfig(tenant), newQuotaConfig);

      // Invalid configs are rejected and do not overwrite the existing one
      assertHttpStatus(() -> sendPutRequest(url, new QuotaConfig(null, null).toJsonString()), 400);
      assertHttpStatus(() -> sendPutRequest(url, "{\"storage\": \"invalid\"}"), 400);
      assertHttpStatus(() -> sendPutRequest(url, "null"), 400);
      assertHttpStatus(
          () -> sendPutRequest(url, new QuotaConfig(null, null, null, QuotaEnforcementMode.SHADOW).toJsonString()),
          400);
      assertEquals(resourceManager.getTenantQuotaConfig(tenant), newQuotaConfig);

      // Unknown tenant
      assertHttpStatus(() -> sendPutRequest(baseUrl + "/tenants/unknownTenant/quotas", newQuotaConfig.toJsonString()),
          404);

      // Setting the quota of one role keeps the quota of the other role
      sendPutRequest(url, new QuotaConfig(null, "200", null, QuotaEnforcementMode.SHADOW).toJsonString());
      assertEquals(resourceManager.getTenantQuotaConfig(tenant),
          new QuotaConfig("20G", "200", QuotaEnforcementMode.SHADOW, QuotaEnforcementMode.SHADOW));
      sendPutRequest(url, new QuotaConfig("30G", null, QuotaEnforcementMode.ENFORCED, null).toJsonString());
      assertEquals(resourceManager.getTenantQuotaConfig(tenant),
          new QuotaConfig("30G", "200", QuotaEnforcementMode.ENFORCED, QuotaEnforcementMode.SHADOW));

      // Deleting the quota of one role keeps the quota of the other role
      assertHttpStatus(() -> sendDeleteRequest(url + "?type=minion"), 400);
      sendDeleteRequest(url + "?type=broker");
      assertEquals(resourceManager.getTenantQuotaConfig(tenant),
          new QuotaConfig("30G", null, QuotaEnforcementMode.ENFORCED, null));
      assertHttpStatus(() -> sendDeleteRequest(url + "?type=broker"), 404);
      sendDeleteRequest(url + "?type=server");
      assertNull(resourceManager.getTenantQuotaConfig(tenant));
      assertHttpStatus(() -> sendGetRequest(url), 404);
      assertHttpStatus(() -> sendDeleteRequest(url + "?type=server"), 404);

      // Delete all
      sendPutRequest(url, newQuotaConfig.toJsonString());
      sendDeleteRequest(url);
      assertNull(resourceManager.getTenantQuotaConfig(tenant));
      assertHttpStatus(() -> sendGetRequest(url), 404);
    } finally {
      // Don't leak quota defaults on the shared default tenant into other tests
      if (resourceManager.hasTenantQuotaConfig(tenant)) {
        resourceManager.deleteTenantQuotaConfig(tenant);
      }
    }
  }

  @Test
  public void testBrokerTenantQuotaConfig()
      throws Exception {
    // A broker-only tenant accepts query quota defaults but rejects storage quota defaults
    String tenant = "quotaBrokerTenant";
    PinotAdminClient adminClient = getOrCreateAdminClient();
    Instance brokerInstance =
        new Instance("3.4.5.6", 3456, InstanceType.BROKER, List.of(TagNameUtils.getBrokerTagForTenant(tenant)), null,
            0, 0, 0, 0, false);
    adminClient.getInstanceClient().createInstance(brokerInstance.toJsonString());
    String url = DEFAULT_INSTANCE.getControllerBaseApiUrl() + "/tenants/" + tenant + "/quotas";
    PinotHelixResourceManager resourceManager = DEFAULT_INSTANCE.getHelixResourceManager();
    try {
      assertHttpStatus(() -> sendPutRequest(url, new QuotaConfig("10G", null).toJsonString()), 400);
      assertHttpStatus(() -> sendPutRequest(url, new QuotaConfig("10G", "100").toJsonString()), 400);
      QuotaConfig quotaConfig = new QuotaConfig(null, "100", null, QuotaEnforcementMode.SHADOW);
      sendPutRequest(url, quotaConfig.toJsonString());
      assertEquals(resourceManager.getTenantQuotaConfig(tenant), quotaConfig);
    } finally {
      if (resourceManager.hasTenantQuotaConfig(tenant)) {
        resourceManager.deleteTenantQuotaConfig(tenant);
      }
      adminClient.getInstanceClient().dropInstance("Broker_3.4.5.6_3456");
    }
  }

  private static void assertHttpStatus(ThrowingRunnable request, int expectedStatusCode) {
    IOException e = expectThrows(IOException.class, request);
    assertTrue(e.getCause() instanceof HttpErrorStatusException, "Unexpected exception: " + e);
    assertEquals(((HttpErrorStatusException) e.getCause()).getStatusCode(), expectedStatusCode);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    DEFAULT_INSTANCE.cleanup();
  }
}
