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
package org.apache.pinot.controller.helix;

import java.io.IOException;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerSentinelTestV2 extends ControllerTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(20, true);
    addFakeServerInstancesToAutoJoinHelixCluster(20, true);
  }

  @Test
  public void testOfflineTableLifeCycle()
      throws IOException {
    // Create offline table creation request
    String tableName = "testTable";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setNumReplicas(3).build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet().size(), 1);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getInstanceSet(tableName + "_OFFLINE").size(), 20);

    // Adding segments
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(
          _helixAdmin.getResourceIdealState(getHelixClusterName(), tableName + "_OFFLINE").getNumPartitions(), i);
      _helixResourceManager
          .addNewSegment(tableName, SegmentMetadataMockUtils.mockSegmentMetadata(tableName), "downloadUrl");
      Assert.assertEquals(
          _helixAdmin.getResourceIdealState(getHelixClusterName(), tableName + "_OFFLINE").getNumPartitions(), i + 1);
    }

    // Delete table
    sendDeleteRequest(_controllerRequestURLBuilder.forTableDelete(tableName));
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet().size(), 0);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(),
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), 20);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(),
        TagNameUtils.getRealtimeTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), 20);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(),
        TagNameUtils.getOfflineTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), 20);
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
  }
}
