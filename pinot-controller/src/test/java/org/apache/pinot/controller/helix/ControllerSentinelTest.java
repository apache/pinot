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
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ControllerSentinelTest extends ControllerTest {
  private static final String TABLE_NAME = "sentinelTable";

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testOfflineTableLifeCycle()
      throws IOException {
    // Create schema
    sendPostRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forSchemaCreate(),
        createDummySchema(TABLE_NAME).toPrettyJsonString());
    // Create offline table creation request
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNumReplicas(DEFAULT_MIN_NUM_REPLICAS)
            .build();
    sendPostRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableCreate(), tableConfig.toJsonString());
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
        .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet().size(), 1);
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
        .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getInstanceSet(TABLE_NAME + "_OFFLINE").size(), DEFAULT_NUM_BROKER_INSTANCES);

    // Adding segments
    for (int i = 0; i < 10; i++) {
      assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
          .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), TABLE_NAME + "_OFFLINE")
          .getNumPartitions(), i);
      DEFAULT_INSTANCE.getHelixResourceManager().addNewSegment(TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME),
          SegmentMetadataMockUtils.mockSegmentMetadata(TABLE_NAME), "downloadUrl");
      assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
          .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), TABLE_NAME + "_OFFLINE")
          .getNumPartitions(), i + 1);
    }

    // Delete table
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableDelete(TABLE_NAME));
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
        .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet().size(), 0);

    assertEquals(DEFAULT_INSTANCE.getHelixAdmin().getInstancesInClusterWithTag(DEFAULT_INSTANCE.getHelixClusterName(),
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), DEFAULT_NUM_BROKER_INSTANCES);
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin().getInstancesInClusterWithTag(DEFAULT_INSTANCE.getHelixClusterName(),
        TagNameUtils.getOfflineTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), DEFAULT_NUM_SERVER_INSTANCES);
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin().getInstancesInClusterWithTag(DEFAULT_INSTANCE.getHelixClusterName(),
        TagNameUtils.getRealtimeTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME)).size(), DEFAULT_NUM_SERVER_INSTANCES);
  }

  @AfterClass
  public void tearDown() {
    DEFAULT_INSTANCE.cleanup();
  }
}
