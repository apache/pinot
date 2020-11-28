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
import java.util.Set;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerTestUtils.*;


public class ControllerSentinelTestV2 {
  private static final String TABLE_NAME = "sentinalTable";

  @Test
  public void testOfflineTableLifeCycle()
      throws IOException {
    // Create offline table creation request

    // AKL_TODO
    // Not sure why segmentTable_OFFLINE is showing up here since it has been explictly deleted in SegmentLineageCleanupTest,
    // but we cleanup here again.
    Set<String> existingPartitions = getHelixAdmin().getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet();
    for (String partition : existingPartitions) {
      getHelixResourceManager().deleteOfflineTable(partition);
    }

    System.out.println(getHelixAdmin().getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet());

    Assert.assertEquals(getHelixAdmin().getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet().size(), 0);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(MIN_NUM_REPLICAS).build();
    sendPostRequest(getControllerRequestURLBuilder().forTableCreate(), tableConfig.toJsonString());

    System.out.println("Partition Set2: " + getHelixAdmin().getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet());

    Assert.assertEquals(
        getHelixAdmin().getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet().size(), 1);
    Assert.assertEquals(
        getHelixAdmin().getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getInstanceSet(TABLE_NAME + "_OFFLINE").size(), NUM_BROKER_INSTANCES);

    // Adding segments
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(
          getHelixAdmin().getResourceIdealState(getHelixClusterName(), TABLE_NAME + "_OFFLINE").getNumPartitions(), i);
      getHelixResourceManager()
          .addNewSegment(TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME), "downloadUrl");
      Assert.assertEquals(
          getHelixAdmin().getResourceIdealState(getHelixClusterName(), TABLE_NAME + "_OFFLINE").getNumPartitions(), i + 1);
    }

    // Delete table
    sendDeleteRequest(getControllerRequestURLBuilder().forTableDelete(TABLE_NAME));
    Assert.assertEquals(
        getHelixAdmin().getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet().size(), 0);

    Assert.assertEquals(getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(),
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), NUM_BROKER_INSTANCES);
    Assert.assertEquals(getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(),
        TagNameUtils.getRealtimeTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), NUM_BROKER_INSTANCES);
    Assert.assertEquals(getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(),
        TagNameUtils.getOfflineTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), NUM_BROKER_INSTANCES);
  }

  @AfterTest
  public void tearDown() {
    getHelixResourceManager().deleteOfflineTable(TABLE_NAME);
  }
}
