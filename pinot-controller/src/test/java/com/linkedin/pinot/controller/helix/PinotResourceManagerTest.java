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
package com.linkedin.pinot.controller.helix;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.json.JSONObject;
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
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;


public class PinotResourceManagerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotResourceManagerTest.class);

  private PinotHelixResourceManager _pinotHelixResourceManager;
  private final static String ZK_SERVER = ZkTestUtils.DEFAULT_ZK_STR;
  private final static String HELIX_CLUSTER_NAME = "TestPinotResourceManager";
  private final static String BROKER_TENANT_NAME = "testBrokerTenant";
  private final static String SERVER_TENANT_NAME = "testServerTenant";
  private final static String TABLE_NAME = "testTable";
  private ZkClient _zkClient;

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance;

  @BeforeTest
  public void setUp() throws Exception {
    ZkTestUtils.startLocalZkServer();
    _zkClient = new ZkClient(ZK_SERVER);

    final String instanceId = "localhost_helixController";
    _pinotHelixResourceManager = new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null);
    _pinotHelixResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    /////////////////////////
    _numInstance = 1;
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, _numInstance);
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, 1);
    Thread.sleep(3000);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE)
            .size(), _numInstance);

    // Create broker tenant
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 1, 0, 0);
    _pinotHelixResourceManager.createBrokerTenant(brokerTenant);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, BROKER_TENANT_NAME + "_BROKER").size(), 1);

    // Create server tenant
    Tenant serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, 1, 1, 0);
    _pinotHelixResourceManager.createServerTenant(serverTenant);

    // Adding table
    String OfflineTableConfigJson =
        ControllerRequestBuilderUtil.buildCreateOfflineTableV2JSON(TABLE_NAME, SERVER_TENANT_NAME, BROKER_TENANT_NAME, 1).toString();
    AbstractTableConfig offlineTableConfig = AbstractTableConfig.init(OfflineTableConfigJson);
    _pinotHelixResourceManager.addTable(offlineTableConfig);

  }

  @AfterTest
  public void tearDown() {
    _pinotHelixResourceManager.stop();
    _zkClient.close();
    ZkTestUtils.stopLocalZkServer();
  }

  @Test
  public void testAddingAndDeletingSegments() throws Exception {
    for (int i = 1; i <= 5; i++) {
      addOneSegment(TABLE_NAME);
      Thread.sleep(2000);
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME));
      Assert.assertEquals(externalView.getPartitionSet().size(), i);
    }
    final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME));
    int i = 4;
    for (final String segmentId : externalView.getPartitionSet()) {
      deleteOneSegment(TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME), segmentId);
      Thread.sleep(2000);
      Assert.assertEquals(_helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(TABLE_NAME)).getPartitionSet()
          .size(), i);
      i--;
    }
  }

  public void testWithCmdLines() throws Exception {

    final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      final String command = br.readLine();
      if ((command != null) && command.equals("exit")) {
        tearDown();
      }
      if ((command != null) && command.equals("add")) {
        addOneSegment(TABLE_NAME);
      }
      if ((command != null) && command.startsWith("delete")) {
        final String segment2delete = command.split(" ")[1];
        deleteOneSegment(TABLE_NAME, segment2delete);
      }
    }
  }

  private void addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName);
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotHelixResourceManager.addSegmentV2(segmentMetadata, "downloadUrl");
  }

  private void deleteOneSegment(String resource, String segment) {
    LOGGER.info("Trying to delete Segment : " + segment + " from resource : " + resource);
    _pinotHelixResourceManager.deleteSegment(resource, segment);
  }

}
