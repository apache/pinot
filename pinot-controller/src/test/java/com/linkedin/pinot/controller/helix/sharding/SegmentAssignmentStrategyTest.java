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
package com.linkedin.pinot.controller.helix.sharding;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.api.pojos.Tenant;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;


public class SegmentAssignmentStrategyTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentAssignmentStrategyTest.class);

  private final static String ZK_SERVER = ZkTestUtils.DEFAULT_ZK_STR;
  private final static String HELIX_CLUSTER_NAME = "TestSegmentAssignmentStrategyHelix";
  private final static String TABLE_NAME_BALANCED = "testResourceBalanced";
  private final static String TABLE_NAME_RANDOM = "testResourceRandom";
  private final static String BROKER_TENANT_NAME = "testBrokerTenant";
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private ZkClient _zkClient;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private final int _numServerInstance = 30;
  private final int _numBrokerInstance = 5;

  @BeforeTest
  public void setup() throws Exception {
    ZkTestUtils.startLocalZkServer();
    _zkClient = new ZkClient(ZK_SERVER);
    final String zkPath = "/" + HELIX_CLUSTER_NAME;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    final String instanceId = "localhost_helixController";
    _pinotHelixResourceManager = new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null);
    _pinotHelixResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    //

    ControllerRequestBuilderUtil
        .addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, _numServerInstance);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER,
        _numBrokerInstance);
    Thread.sleep(3000);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            .size(), _numServerInstance);

    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 5, 0, 0);
    _pinotHelixResourceManager.createBrokerTenant(brokerTenant);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, BROKER_TENANT_NAME + "_BROKER").size(), 5);
  }

  @AfterTest
  public void tearDown() {
    _pinotHelixResourceManager.stop();
    _zkClient.close();
    ZkTestUtils.stopLocalZkServer();
  }

  @Test
  public void testRandomSegmentAssignmentStrategy() throws Exception {
    final int numReplicas = 2;

    // Create server tenant
    String serverTenantName = "randomServerTenant";
    Tenant serverTenant = new Tenant(TenantRole.SERVER, serverTenantName, 20, 20, 0);
    _pinotHelixResourceManager.createServerTenant(serverTenant);

    // Adding table
    String OfflineTableConfigJson =
        ControllerRequestBuilderUtil.buildCreateOfflineTableV2JSON(TABLE_NAME_RANDOM, serverTenantName, BROKER_TENANT_NAME, numReplicas, "RandomAssignmentStrategy").toString();
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(OfflineTableConfigJson);
    _pinotHelixResourceManager.addTable(offlineTableConfig);

    Thread.sleep(3000);
    for (int i = 0; i < 10; ++i) {
      addOneSegment(TABLE_NAME_RANDOM);
      Thread.sleep(2000);
      final Set<String> taggedInstances = _pinotHelixResourceManager.getAllInstancesForServerTenant(serverTenantName);
      final Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
      for (final String instance : taggedInstances) {
        instance2NumSegmentsMap.put(instance, 0);
      }
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME_RANDOM));
      Assert.assertEquals(externalView.getPartitionSet().size(), i + 1);
      for (final String segmentId : externalView.getPartitionSet()) {
        Assert.assertEquals(externalView.getStateMap(segmentId).size(), numReplicas);
      }

    }
  }

  @Test
  public void testBalanceNumSegmentAssignmentStrategy() throws Exception {
    final int numReplicas = 3;
    final int totalInstances = 6;
    // Creating server tenant
    String serverTenantName = "balanceServerTenant";
    Tenant serverTenant = new Tenant(TenantRole.SERVER, serverTenantName, 6, 6, 0);
    _pinotHelixResourceManager.createServerTenant(serverTenant);
    // Adding table
    String OfflineTableConfigJson =
        ControllerRequestBuilderUtil.buildCreateOfflineTableV2JSON(TABLE_NAME_BALANCED, serverTenantName, BROKER_TENANT_NAME, numReplicas, "BalanceNumSegmentAssignmentStrategy")
            .toString();
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(OfflineTableConfigJson);
    _pinotHelixResourceManager.addTable(offlineTableConfig);

    Thread.sleep(3000);
    for (int i = 0; i < 10; ++i) {
      addOneSegment(TABLE_NAME_BALANCED);
      Thread.sleep(2000);
      final Set<String> taggedInstances = _pinotHelixResourceManager.getAllInstancesForServerTenant(serverTenantName);
      final Map<String, Integer> instance2NumSegmentsMap = new HashMap<String, Integer>();
      for (final String instance : taggedInstances) {
        instance2NumSegmentsMap.put(instance, 0);
      }
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME_BALANCED));
      for (final String segmentId : externalView.getPartitionSet()) {
        for (final String instance : externalView.getStateMap(segmentId).keySet()) {
          instance2NumSegmentsMap.put(instance, instance2NumSegmentsMap.get(instance) + 1);
        }
      }
      final int totalSegments = (i + 1) * numReplicas;
      final int minNumSegmentsPerInstance = totalSegments / totalInstances;
      int maxNumSegmentsPerInstance = minNumSegmentsPerInstance;
      if ((minNumSegmentsPerInstance * totalInstances) < totalSegments) {
        maxNumSegmentsPerInstance = maxNumSegmentsPerInstance + 1;
      }
      for (final String instance : instance2NumSegmentsMap.keySet()) {
        Assert.assertTrue(instance2NumSegmentsMap.get(instance) >= minNumSegmentsPerInstance);
        Assert.assertTrue(instance2NumSegmentsMap.get(instance) <= maxNumSegmentsPerInstance);
      }
    }

    _helixAdmin.dropResource(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME_BALANCED));
  }

  private void addOneSegment(String tableName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(tableName);
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotHelixResourceManager.addSegmentV2(segmentMetadata, "downloadUrl");
  }

}
