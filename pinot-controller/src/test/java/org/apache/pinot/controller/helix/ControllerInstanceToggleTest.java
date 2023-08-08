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
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ControllerInstanceToggleTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "toggleTable";
  private static final long TIMEOUT_MS = 10_000L;
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String SERVER_TAG_NAME = TagNameUtils.getOfflineTagForTenant(null);
  private static final String BROKER_TAG_NAME = TagNameUtils.getBrokerTagForTenant(null);

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testInstanceToggle()
      throws Exception {
    // Create an offline table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(DEFAULT_MIN_NUM_REPLICAS)
            .build();
    sendPostRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableCreate(), tableConfig.toJsonString());
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
        .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet().size(), 1);
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
        .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getInstanceSet(OFFLINE_TABLE_NAME).size(), DEFAULT_NUM_BROKER_INSTANCES);

    // Add segments
    for (int i = 0; i < DEFAULT_NUM_SERVER_INSTANCES; i++) {
      DEFAULT_INSTANCE.getHelixResourceManager()
          .addNewSegment(OFFLINE_TABLE_NAME, SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME),
              "downloadUrl");
      assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
          .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), OFFLINE_TABLE_NAME).getNumPartitions(), i + 1);
    }

    // Disable server instances
    int numEnabledInstances = DEFAULT_NUM_SERVER_INSTANCES;
    for (String instanceName : DEFAULT_INSTANCE.getHelixAdmin()
        .getInstancesInClusterWithTag(DEFAULT_INSTANCE.getHelixClusterName(), SERVER_TAG_NAME)) {
      toggleInstanceState(instanceName, "disable");
      numEnabledInstances--;
      checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, numEnabledInstances);
    }

    // Enable server instances
    for (String instanceName : DEFAULT_INSTANCE.getHelixAdmin()
        .getInstancesInClusterWithTag(DEFAULT_INSTANCE.getHelixClusterName(), SERVER_TAG_NAME)) {
      toggleInstanceState(instanceName, "ENABLE");
      numEnabledInstances++;
      checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, numEnabledInstances);
    }

    // Disable broker instances
    numEnabledInstances = DEFAULT_NUM_BROKER_INSTANCES;
    for (String instanceName : DEFAULT_INSTANCE.getHelixAdmin()
        .getInstancesInClusterWithTag(DEFAULT_INSTANCE.getHelixClusterName(), BROKER_TAG_NAME)) {
      toggleInstanceState(instanceName, "Disable");
      numEnabledInstances--;
      checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, numEnabledInstances);
    }

    // Enable broker instances
    for (String instanceName : DEFAULT_INSTANCE.getHelixAdmin()
        .getInstancesInClusterWithTag(DEFAULT_INSTANCE.getHelixClusterName(), BROKER_TAG_NAME)) {
      toggleInstanceState(instanceName, "Enable");
      numEnabledInstances++;
      checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, numEnabledInstances);
    }

    // Delete table
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableDelete(RAW_TABLE_NAME));
    assertEquals(DEFAULT_INSTANCE.getHelixAdmin()
        .getResourceIdealState(DEFAULT_INSTANCE.getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
        .getPartitionSet().size(), 0);
  }

  private void toggleInstanceState(String instanceName, String state) {
    // It may take time for an instance to toggle the state.
    TestUtils.waitForCondition(aVoid -> {
      try {
        sendPutRequest(
            DEFAULT_INSTANCE.getControllerRequestURLBuilder().forInstanceState(instanceName) + "?state=" + state);
      } catch (IOException ioe) {
        // receive non-200 status code
        return false;
      }
      return true;
    }, TIMEOUT_MS, "Failed to toggle instance state: '" + state + "' for instance: " + instanceName);
  }

  @AfterClass
  public void tearDown() {
    DEFAULT_INSTANCE.cleanup();
  }
}
