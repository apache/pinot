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
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerSentinelTestV2 {
  private static final String TABLE_NAME = "sentinalTable";

  @BeforeClass
  public void setUp() throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testOfflineTableLifeCycle() throws IOException {
    // Create offline table creation request
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(
            ControllerTestUtils.MIN_NUM_REPLICAS).build();
    ControllerTestUtils
        .sendPostRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableCreate(), tableConfig.toJsonString());
    Assert.assertEquals(
        ControllerTestUtils
            .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 1);
    Assert.assertEquals(
        ControllerTestUtils
            .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getInstanceSet(TABLE_NAME + "_OFFLINE")
            .size(), ControllerTestUtils.NUM_BROKER_INSTANCES);

    // Adding segments
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(
          ControllerTestUtils
              .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), TABLE_NAME + "_OFFLINE").getNumPartitions(), i);
      ControllerTestUtils.getHelixResourceManager().addNewSegment(TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME),
          "downloadUrl");
      Assert.assertEquals(
          ControllerTestUtils
              .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), TABLE_NAME + "_OFFLINE").getNumPartitions(),
          i + 1);
    }

    // Delete table
    ControllerTestUtils.sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableDelete(TABLE_NAME));
    Assert.assertEquals(
        ControllerTestUtils
            .getHelixAdmin().getResourceIdealState(ControllerTestUtils.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 0);

    Assert.assertEquals(
        ControllerTestUtils.getHelixAdmin().getInstancesInClusterWithTag(ControllerTestUtils.getHelixClusterName(),
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), ControllerTestUtils.NUM_BROKER_INSTANCES);
    Assert.assertEquals(
        ControllerTestUtils.getHelixAdmin().getInstancesInClusterWithTag(ControllerTestUtils.getHelixClusterName(),
        TagNameUtils.getRealtimeTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), ControllerTestUtils.NUM_BROKER_INSTANCES);
    Assert.assertEquals(
        ControllerTestUtils.getHelixAdmin().getInstancesInClusterWithTag(ControllerTestUtils.getHelixClusterName(),
        TagNameUtils.getOfflineTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), ControllerTestUtils.NUM_BROKER_INSTANCES);
  }

  @AfterTest
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
