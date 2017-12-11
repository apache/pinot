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
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import java.io.IOException;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerSentinelTestV2 extends ControllerTest {
  private final String _helixClusterName = getHelixClusterName();

  @BeforeClass
  public void setup() throws Exception {
    startZk();
    startController();
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(_helixClusterName,
        ZkStarter.DEFAULT_ZK_STR, 20, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(_helixClusterName, ZkStarter.DEFAULT_ZK_STR,
        20, true);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  @Test
  public void testOfflineTableLifeCycle() throws IOException, JSONException {
    // Create offline table creation request
    String tableName = "testTable";
    String tableJSONConfigString =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName)
            .setNumReplicas(3)
            .build()
            .toJSONConfigString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableJSONConfigString);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 1);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getInstanceSet(tableName + "_OFFLINE")
            .size(), 20);

    // Adding segments
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(
          _helixAdmin.getResourceIdealState(_helixClusterName, tableName + "_OFFLINE").getNumPartitions(), i);
      addOneOfflineSegment(tableName);
      Assert.assertEquals(
          _helixAdmin.getResourceIdealState(_helixClusterName, tableName + "_OFFLINE").getNumPartitions(), i + 1);
    }

    // Delete table
    sendDeleteRequest(_controllerRequestURLBuilder.forTableDelete(tableName));
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 0);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getBrokerTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME))
        .size(), 20);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getRealtimeTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME))
        .size(), 20);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName,
        ControllerTenantNameBuilder.getOfflineTenantNameForTenant(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME))
        .size(), 20);
  }

  private void addOneOfflineSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName);
    _helixResourceManager.addNewSegment(segmentMetadata, "downloadUrl");
  }
}
