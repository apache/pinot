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
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import java.util.List;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PinotHelixResourceManagerTest {
  ZkStarter.ZookeeperInstance zkServer;
  private PinotHelixResourceManager pinotHelixResourceManager;
  private final static String ZK_SERVER = ZkStarter.DEFAULT_ZK_STR;
  private final static String HELIX_CLUSTER_NAME = "PinotHelixResourceManagerTest";
  private HelixManager helixZkManager;
  private HelixAdmin helixAdmin;
  private final int adminPortStart = 10000;
  private final int numInstances = 10;
  @BeforeClass
  public void setUp()
      throws Exception {
    zkServer = ZkStarter.startLocalZkServer();
    final String instanceId = "localhost_helixController";
    pinotHelixResourceManager =
        new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null, 10000L, false, /*isUpdateStateModel=*/false);
    pinotHelixResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId, /*isUpdateStateModel=*/false);
    helixAdmin = helixZkManager.getClusterManagmentTool();
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,ZK_SERVER, numInstances);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, numInstances, true, adminPortStart);
  }

  @AfterClass
  public void tearDown() {
    pinotHelixResourceManager.stop();
    ZkStarter.stopLocalZkServer(zkServer);
  }

  @Test
  public void testGetInstanceEndpoints()
      throws InterruptedException {
    Set<String> servers = pinotHelixResourceManager.getAllInstancesForServerTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME);
    BiMap<String, String> endpoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(servers);
//    for (Map.Entry<String, String> endpointEntry : endpoints.entrySet()) {
//      System.out.println(endpointEntry.getKey() + " -- " + endpointEntry.getValue());
//    }

    for (int i = 0; i < numInstances; i++) {
      Assert.assertTrue(endpoints.inverse().containsKey("localhost:" + String.valueOf(adminPortStart + i)));
    }
  }

  @Test
  public void testRebuildBrokerResourceFromHelixTags() throws Exception {
    AbstractTableConfig tableConfig = AbstractTableConfig.init(
        ControllerRequestBuilderUtil.buildCreateOfflineTableJSON("faketable", "serverTenant", "brokerTenant", 3)
            .toString());

    Tenant tenant = new Tenant();
    tenant.setTenantName("brokerTenant");
    tenant.setTenantRole("BROKER");
    tenant.setNumberOfInstances(3);
    pinotHelixResourceManager.createBrokerTenant(tenant);
    pinotHelixResourceManager.addTable(tableConfig);

    // Check that the broker ideal state has 3 brokers assigned to it for faketable_OFFLINE
    IdealState idealState = pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap("faketable_OFFLINE").size(), 3);

    // Retag all instances current assigned to brokerTenant to be unassigned
    Set<String> brokerInstances = pinotHelixResourceManager.getAllInstancesForBrokerTenant("brokerTenant");
    for (String brokerInstance : brokerInstances) {
      pinotHelixResourceManager.getHelixAdmin().removeInstanceTag(HELIX_CLUSTER_NAME, brokerInstance,
          "brokerTenant_BROKER");
      pinotHelixResourceManager.getHelixAdmin().addInstanceTag(HELIX_CLUSTER_NAME, brokerInstance,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Rebuilding the broker tenant should update the ideal state size
    pinotHelixResourceManager.rebuildBrokerResourceFromHelixTags("faketable_OFFLINE");
    idealState = pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap("faketable_OFFLINE").size(), 0);

    // Tag five instances
    int instancesRemainingToTag = 5;
    List<String> instances = pinotHelixResourceManager.getAllInstances();
    for (String instance : instances) {
      if (instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        pinotHelixResourceManager.getHelixAdmin()
            .removeInstanceTag(HELIX_CLUSTER_NAME, instance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
        pinotHelixResourceManager.getHelixAdmin()
            .addInstanceTag(HELIX_CLUSTER_NAME, instance, "brokerTenant_BROKER");
        instancesRemainingToTag--;
        if (instancesRemainingToTag == 0) {
          break;
        }
      }
    }

    // Rebuilding the broker tenant should update the ideal state size
    pinotHelixResourceManager.rebuildBrokerResourceFromHelixTags("faketable_OFFLINE");
    idealState = pinotHelixResourceManager.getHelixAdmin()
        .getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceStateMap("faketable_OFFLINE").size(), 5);

    // Untag all instances for other tests
    for (String instance : instances) {
      if (instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        pinotHelixResourceManager.getHelixAdmin()
            .removeInstanceTag(HELIX_CLUSTER_NAME, instance, "brokerTenant_BROKER");
        pinotHelixResourceManager.getHelixAdmin()
            .addInstanceTag(HELIX_CLUSTER_NAME, instance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }

    // Delete table
    pinotHelixResourceManager.deleteOfflineTable("faketable");
  }
}
