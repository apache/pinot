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
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerInstanceToggleTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String RAW_TABLE_NAME = "toggleTable";
  private static final long TIMEOUT_MS = 10_000L;
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String SERVER_TAG_NAME = TagNameUtils.getOfflineTagForTenant(null);
  private static final String BROKER_TAG_NAME = TagNameUtils.getBrokerTagForTenant(null);

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testInstanceToggle()
      throws Exception {
    // Create an offline table
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(
            TEST_INSTANCE.MIN_NUM_REPLICAS).build();
    ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forTableCreate(),
            tableConfig.toJsonString());
    Assert.assertEquals(
        TEST_INSTANCE
            .getHelixAdmin().getResourceIdealState(TEST_INSTANCE.getHelixClusterName(),
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 1);
    Assert.assertEquals(
        TEST_INSTANCE
            .getHelixAdmin().getResourceIdealState(TEST_INSTANCE.getHelixClusterName(),
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getInstanceSet(OFFLINE_TABLE_NAME)
            .size(), TEST_INSTANCE.NUM_BROKER_INSTANCES);

    // Add segments
    for (int i = 0; i < TEST_INSTANCE.NUM_SERVER_INSTANCES; i++) {
      TEST_INSTANCE.getHelixResourceManager().addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(RAW_TABLE_NAME), "downloadUrl");
      Assert.assertEquals(
          TEST_INSTANCE
              .getHelixAdmin().getResourceIdealState(TEST_INSTANCE.getHelixClusterName(), OFFLINE_TABLE_NAME)
              .getNumPartitions(), i + 1);
    }

    // Disable server instances
    int numEnabledInstances = TEST_INSTANCE.NUM_SERVER_INSTANCES;
    for (String instanceName : TEST_INSTANCE
        .getHelixAdmin().getInstancesInClusterWithTag(TEST_INSTANCE.getHelixClusterName(), SERVER_TAG_NAME)) {
      toggleInstanceState(instanceName, "disable");
      numEnabledInstances--;
      checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, numEnabledInstances);
    }

    // Enable server instances
    for (String instanceName : TEST_INSTANCE
        .getHelixAdmin().getInstancesInClusterWithTag(TEST_INSTANCE.getHelixClusterName(), SERVER_TAG_NAME)) {
      toggleInstanceState(instanceName, "ENABLE");
      numEnabledInstances++;
      checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, numEnabledInstances);
    }

    // Disable broker instances
    for (String instanceName : TEST_INSTANCE
        .getHelixAdmin().getInstancesInClusterWithTag(TEST_INSTANCE.getHelixClusterName(), BROKER_TAG_NAME)) {
      toggleInstanceState(instanceName, "Disable");
      numEnabledInstances--;
      checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, numEnabledInstances);
    }

    // Enable broker instances
    for (String instanceName : TEST_INSTANCE
        .getHelixAdmin().getInstancesInClusterWithTag(TEST_INSTANCE.getHelixClusterName(), BROKER_TAG_NAME)) {
      toggleInstanceState(instanceName, "Enable");
      numEnabledInstances++;
      checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, numEnabledInstances);
    }

    // Delete table
    ControllerTest
        .sendDeleteRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forTableDelete(RAW_TABLE_NAME));
    Assert.assertEquals(
        TEST_INSTANCE
            .getHelixAdmin().getResourceIdealState(TEST_INSTANCE.getHelixClusterName(),
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 0);
  }

  private void toggleInstanceState(String instanceName, String state) {
    // It may take time for an instance to toggle the state.
    TestUtils.waitForCondition(aVoid -> {
      try {
        ControllerTest
            .sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forInstanceState(instanceName),
                state);
      } catch (IOException ioe) {
        // receive non-200 status code
        return false;
      }
      return true;
    }, TIMEOUT_MS, "Failed to toggle instance state: '" + state + "' for instance: " + instanceName);
  }

  private void checkNumOnlineInstancesFromExternalView(String resourceName, int expectedNumOnlineInstances)
      throws InterruptedException {
    long endTime = System.currentTimeMillis() + TIMEOUT_MS;
    while (System.currentTimeMillis() < endTime) {
      ExternalView resourceExternalView = TEST_INSTANCE
          .getHelixAdmin().getResourceExternalView(TEST_INSTANCE.getHelixClusterName(), resourceName);
      Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
      if (instanceSet.size() == expectedNumOnlineInstances) {
        return;
      }
      Thread.sleep(100L);
    }
    Assert.fail("Failed to reach " + expectedNumOnlineInstances + " online instances for resource: " + resourceName);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
