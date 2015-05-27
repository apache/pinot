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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.helix.manager.zk.ZkClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;


public class ControllerSentinelTestV2 extends ControllerTest {

  private static final String HELIX_CLUSTER_NAME = "ControllerSentinelTestV2";
  static ZkClient _zkClient = null;

  private PinotHelixResourceManager _pinotResourceManager;

  @BeforeClass
  public void setup() throws Exception {
    startZk();
    _zkClient = new ZkClient(ZkTestUtils.DEFAULT_ZK_STR);
    startController();
    _pinotResourceManager = _controllerStarter.getHelixResourceManager();
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkTestUtils.DEFAULT_ZK_STR, 20, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkTestUtils.DEFAULT_ZK_STR, 20, true);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    try {
      if (_zkClient.exists("/" + HELIX_CLUSTER_NAME)) {
        _zkClient.deleteRecursive("/" + HELIX_CLUSTER_NAME);
      }
    } catch (Exception e) {
    }
    _zkClient.close();
    stopZk();
  }

  @Test
  public void testOfflineTableLifeCycle() throws JSONException, UnsupportedEncodingException, IOException {
    // Create offline table creation request
    String tableName = "testTable";
    JSONObject payload = ControllerRequestBuilderUtil.buildCreateOfflineTableJSON(tableName, null, null, 3);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(), payload.toString());
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE).getPartitionSet().size(), 1);
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE).getInstanceSet(tableName + "_OFFLINE").size(), 20);

    // Adding segments
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, tableName + "_OFFLINE").getNumPartitions(), i);
      addOneOfflineSegment(tableName);
      Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, tableName + "_OFFLINE").getNumPartitions(), i + 1);
    }

    // Delete table
    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableDelete(tableName));
    Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE).getPartitionSet().size(), 0);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME)).size(),
        20);
    Assert.assertEquals(
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME)).size(), 20);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, ControllerTenantNameBuilder.getOfflineTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME)).size(),
        20);
  }

  private void addOneOfflineSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName);
    _pinotResourceManager.addSegment(segmentMetadata, "downloadUrl");
  }

  @Override
  protected String getHelixClusterName() {
    return HELIX_CLUSTER_NAME;
  }

}
