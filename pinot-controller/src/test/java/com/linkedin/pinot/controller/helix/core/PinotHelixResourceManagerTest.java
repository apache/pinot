/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
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
  private final int numInstances = 3;
  @BeforeClass
  public void setUp()
      throws Exception {
    zkServer = ZkStarter.startLocalZkServer();
    final String instanceId = "localhost_helixController";
    pinotHelixResourceManager =
        new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null, 10000L, true);
    pinotHelixResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
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
    for (Map.Entry<String, String> endpointEntry : endpoints.entrySet()) {
      System.out.println(endpointEntry.getKey() + " -- " + endpointEntry.getValue());
    }

    for (int i = 0; i < numInstances; i++) {
      Assert.assertTrue(endpoints.inverse().containsKey("localhost:" + String.valueOf(adminPortStart + i)));
    }
  }

}
