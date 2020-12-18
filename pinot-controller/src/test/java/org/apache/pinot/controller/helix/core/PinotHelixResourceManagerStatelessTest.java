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

import com.google.common.collect.BiMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;
import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT;
import static org.apache.pinot.common.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;
import static org.apache.pinot.common.utils.CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;
import static org.apache.pinot.controller.helix.core.PinotHelixResourceManager.InvalidTableConfigException;


public class PinotHelixResourceManagerStatelessTest extends ControllerTest {
  private static final int BASE_SERVER_ADMIN_PORT = 10000;
  private static final int NUM_INSTANCES = 5;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);

  private static final String SEGMENTS_REPLACE_TEST_TABLE_NAME = "segmentsReplaceTestTable";
  private static final String OFFLINE_SEGMENTS_REPLACE_TEST_TABLE_NAME =
      TableNameBuilder.OFFLINE.tableNameWithType(SEGMENTS_REPLACE_TEST_TABLE_NAME);

  private static final int CONNECTION_TIMEOUT_IN_MILLISECOND = 10_000;
  private static final int MAX_TIMEOUT_IN_MILLISECOND = 5_000;
  private static final int MAXIMUM_NUMBER_OF_CONTROLLER_INSTANCES = 10;
  private static final long TIMEOUT_IN_MS = 10_000L;

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
  public void testValidateTenantConfig() {
    // Create broker tenant on 3 Brokers
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 3, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    String rawTableName = "testTable";
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).build();

    // Empty broker tag (DefaultTenant_BROKER)
    try {
      _helixResourceManager.validateTableTenantConfig(offlineTableConfig);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Empty server tag (DefaultTenant_OFFLINE)
    offlineTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, null, null));
    try {
      _helixResourceManager.validateTableTenantConfig(offlineTableConfig);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Valid tenant config without tagOverrideConfig
    offlineTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, null));
    _helixResourceManager.validateTableTenantConfig(offlineTableConfig);

    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(rawTableName).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();

    // Empty server tag (serverTenant_REALTIME)
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Incorrect CONSUMING server tag (serverTenant_BROKER)
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getBrokerTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Empty CONSUMING server tag (serverTenant_REALTIME)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME), null);
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Incorrect COMPLETED server tag (serverTenant_BROKER)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getBrokerTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Empty COMPLETED server tag (serverTenant_REALTIME)
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    try {
      _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);
      Assert.fail("Expected InvalidTableConfigException");
    } catch (InvalidTableConfigException e) {
      // expected
    }

    // Valid tenant config with tagOverrideConfig
    tagOverrideConfig = new TagOverrideConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME),
        TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME));
    realtimeTableConfig.setTenantConfig(new TenantConfig(BROKER_TENANT_NAME, SERVER_TENANT_NAME, tagOverrideConfig));
    _helixResourceManager.validateTableTenantConfig(realtimeTableConfig);

    untagBrokers();
    Assert.assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), NUM_INSTANCES);
  }

  private void untagBrokers() {
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin.removeInstanceTag(getHelixClusterName(), brokerInstance,
          TagNameUtils.getBrokerTagForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(getHelixClusterName(), brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
