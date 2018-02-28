/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core;

import com.google.common.collect.BiMap;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import java.util.List;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotHelixResourceManagerTest extends ControllerTest {
  private static final int BASE_SERVER_ADMIN_PORT = 10000;
  private static final int NUM_INSTANCES = 5;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(TABLE_NAME);

  private final String _helixClusterName = getHelixClusterName();

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTenantIsolationEnabled(false);
    startController(config);

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(_helixClusterName,
        ZkStarter.DEFAULT_ZK_STR, NUM_INSTANCES, false);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(_helixClusterName, ZkStarter.DEFAULT_ZK_STR,
        NUM_INSTANCES, false, BASE_SERVER_ADMIN_PORT);

    // Create server tenant on all Servers
    Tenant serverTenant = new Tenant.TenantBuilder(SERVER_TENANT_NAME).setRole(TenantRole.SERVER)
        .setOfflineInstances(NUM_INSTANCES)
        .build();
    _helixResourceManager.createServerTenant(serverTenant);
  }

  @Test
  public void testGetInstanceEndpoints() {
    Set<String> servers = _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME);
    BiMap<String, String> endpoints = _helixResourceManager.getDataInstanceAdminEndpoints(servers);
    for (int i = 0; i < NUM_INSTANCES; i++) {
      Assert.assertTrue(endpoints.inverse().containsKey("localhost:" + String.valueOf(BASE_SERVER_ADMIN_PORT + i)));
    }
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags() throws Exception {
    // Create broker tenant on 3 Brokers
    Tenant brokerTenant =
        new Tenant.TenantBuilder(BROKER_TENANT_NAME).setRole(TenantRole.BROKER).setTotalInstances(3).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Create the table
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNumReplicas(3)
        .setBrokerTenant(BROKER_TENANT_NAME)
        .setServerTenant(SERVER_TENANT_NAME)
        .build();
    _helixResourceManager.addTable(tableConfig);

    // Check that the BrokerResource ideal state has 3 Brokers assigned to the table
    IdealState idealState = _helixResourceManager.getHelixAdmin()
        .getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 3);

    // Untag all Brokers assigned to broker tenant
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin.removeInstanceTag(_helixClusterName, brokerInstance,
          ControllerTenantNameBuilder.getBrokerTenantNameForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Rebuilding the broker tenant should update the ideal state size
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 0);

    // Create broker tenant on 5 Brokers
    brokerTenant.setNumberOfInstances(5);
    _helixResourceManager.createBrokerTenant(brokerTenant);

    // Rebuilding the broker tenant should update the ideal state size
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(OFFLINE_TABLE_NAME);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap(OFFLINE_TABLE_NAME).size(), 5);

    // Untag all Brokers for other tests
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixAdmin.removeInstanceTag(_helixClusterName, brokerInstance,
          ControllerTenantNameBuilder.getBrokerTenantNameForTenant(BROKER_TENANT_NAME));
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Delete the table
    _helixResourceManager.deleteOfflineTable(TABLE_NAME);
  }

  @Test
  public void testRetrieveMetadata() throws Exception {
    String segmentName = "testSegment";

    // Test retrieving OFFLINE segment ZK metadata
    {
      OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
      offlineSegmentZKMetadata.setTableName(OFFLINE_TABLE_NAME);
      offlineSegmentZKMetadata.setSegmentName(segmentName);
      ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
      List<OfflineSegmentZKMetadata> retrievedMetadataList =
          _helixResourceManager.getOfflineSegmentMetadata(OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      OfflineSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getTableName(), OFFLINE_TABLE_NAME);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
    }

    // Test retrieving REALTIME segment ZK metadata
    {
      RealtimeSegmentZKMetadata realtimeMetadata = new RealtimeSegmentZKMetadata();
      realtimeMetadata.setTableName(REALTIME_TABLE_NAME);
      realtimeMetadata.setSegmentName(segmentName);
      realtimeMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      ZKMetadataProvider.setRealtimeSegmentZKMetadata(_propertyStore, realtimeMetadata);
      List<RealtimeSegmentZKMetadata> retrievedMetadataList =
          _helixResourceManager.getRealtimeSegmentMetadata(REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedMetadataList.size(), 1);
      RealtimeSegmentZKMetadata retrievedMetadata = retrievedMetadataList.get(0);
      Assert.assertEquals(retrievedMetadata.getTableName(), REALTIME_TABLE_NAME);
      Assert.assertEquals(retrievedMetadata.getSegmentName(), segmentName);
      Assert.assertEquals(realtimeMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
    }
  }


  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
