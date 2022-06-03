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
package org.apache.pinot.controller.helix.core;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.InvalidTableConfigException;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


@Test(groups = "stateless")
public class PinotHelixResourceManagerStatelessTest extends ControllerTest {
  private static final int BASE_SERVER_ADMIN_PORT = 10000;
  private static final int NUM_INSTANCES = 5;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);

    startController(properties);
    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_INSTANCES, false);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_INSTANCES, false, BASE_SERVER_ADMIN_PORT);

    // Create server tenant on all Servers
    Tenant serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_INSTANCES, NUM_INSTANCES, 0);
    _helixResourceManager.createServerTenant(serverTenant);

    // Enable lead controller resource
    enableResourceConfigForLeadControllerResource(true);
  }

  @Test
  public void testValidateDimTableTenantConfig() {
    // Create broker tenant on 3 Brokers
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 3, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    String rawTableName = "testTable";

    // Dim table missing broker
    TableConfig dimTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).build();
    dimTableConfig.setTenantConfig(new TenantConfig(null, SERVER_TENANT_NAME, null));
    try {
      _helixResourceManager.validateTableTenantConfig(dimTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Dim table (offline) deployed to realtime tenant
    dimTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, null));
    _helixResourceManager.validateTableTenantConfig(dimTableConfig);
  }

  @Test
  public void testValidateTenantConfig() {
    // Create broker tenant on 3 Brokers
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 3, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    // Empty broker tag (DefaultTenant_BROKER)
    try {
      _helixResourceManager.validateTableTenantConfig(offlineTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Empty server tag (DefaultTenant_OFFLINE)
    offlineTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, null, null));
    try {
      _helixResourceManager.validateTableTenantConfig(offlineTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Valid tenant config without tagOverrideConfig
    offlineTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, null));
    _helixResourceManager.validateTableTenantConfig(offlineTableConfig);

    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();

    // Empty server tag (serverTenant_REALTIME)
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Incorrect CONSUMING server tag (serverTenant_BROKER)
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getBrokerTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Empty CONSUMING server tag (serverTenant_REALTIME)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME), null);
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Incorrect COMPLETED server tag (serverTenant_BROKER)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getBrokerTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Empty COMPLETED server tag (serverTenant_REALTIME)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Valid tenant config with tagOverrideConfig
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);

    untagBrokers();
    assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), NUM_INSTANCES);
  }

  private void untagBrokers() {
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixResourceManager.updateInstanceTags(brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE, false);
    }
  }

  @Test
  public void testUpdateBrokerResource()
      throws Exception {
    // Create broker tenant on 3 brokers
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 3, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    String brokerTag = TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME);
    List<InstanceConfig> instanceConfigs = HelixHelper.getInstanceConfigs(_helixManager);
    List<String> taggedBrokers = HelixHelper.getInstancesWithTag(instanceConfigs, brokerTag);
    assertEquals(taggedBrokers.size(), 3);
    List<String> untaggedBrokers =
        HelixHelper.getInstancesWithTag(instanceConfigs, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    assertEquals(untaggedBrokers.size(), 2);

    // Add a table
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    _helixResourceManager.addTable(offlineTableConfig);
    checkBrokerResource(taggedBrokers);

    // Untag a tagged broker with instance update
    String brokerToUntag = taggedBrokers.remove(ThreadLocalRandom.current().nextInt(3));
    Instance instance =
        new Instance("localhost", brokerToUntag.charAt(brokerToUntag.length() - 1) - '0', InstanceType.BROKER,
            Collections.singletonList(CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE), null, 0, 0, 0, 0, false);
    assertTrue(_helixResourceManager.updateInstance(brokerToUntag, instance, true).isSuccessful());
    untaggedBrokers.add(brokerToUntag);
    checkBrokerResource(taggedBrokers);

    // Tag an untagged broker with tags update
    String brokerToTag = untaggedBrokers.remove(ThreadLocalRandom.current().nextInt(3));
    assertTrue(_helixResourceManager.updateInstanceTags(brokerToTag, brokerTag, true).isSuccessful());
    taggedBrokers.add(brokerToTag);
    checkBrokerResource(taggedBrokers);

    // Add a new broker instance
    Instance newBrokerInstance =
        new Instance("localhost", 5, InstanceType.BROKER, Collections.singletonList(brokerTag), null, 0, 0, 0, 0,
            false);
    assertTrue(_helixResourceManager.addInstance(newBrokerInstance, true).isSuccessful());
    String newBrokerId = InstanceUtils.getHelixInstanceId(newBrokerInstance);
    taggedBrokers.add(newBrokerId);
    checkBrokerResource(taggedBrokers);

    // Untag the new broker and update the broker resource
    assertTrue(
        _helixResourceManager.updateInstanceTags(newBrokerId, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE, false)
            .isSuccessful());
    assertTrue(_helixResourceManager.updateBrokerResource(newBrokerId).isSuccessful());
    taggedBrokers.remove(taggedBrokers.size() - 1);
    checkBrokerResource(taggedBrokers);

    // Drop the new broker and delete the table
    assertTrue(_helixResourceManager.dropInstance(newBrokerId).isSuccessful());
    _helixResourceManager.deleteOfflineTable(OFFLINE_TABLE_NAME);

    IdealState brokerResource = HelixHelper.getBrokerIdealStates(_helixAdmin, getHelixClusterName());
    assertTrue(brokerResource.getPartitionSet().isEmpty());

    untagBrokers();
  }

  private void checkBrokerResource(List<String> expectedBrokers) {
    IdealState brokerResource = HelixHelper.getBrokerIdealStates(_helixAdmin, getHelixClusterName());
    assertEquals(brokerResource.getPartitionSet().size(), 1);
    Map<String, String> instanceStateMap = brokerResource.getInstanceStateMap(OFFLINE_TABLE_NAME);
    assertEquals(instanceStateMap.keySet(), new HashSet<>(expectedBrokers));
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
