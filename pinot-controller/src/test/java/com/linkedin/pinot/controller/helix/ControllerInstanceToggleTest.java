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
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerInstanceToggleTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String SERVER_TENANT_NAME = ControllerTenantNameBuilder.getOfflineTenantNameForTenant(null);
  private static final String BROKER_TENANT_NAME = ControllerTenantNameBuilder.getBrokerTenantNameForTenant(null);
  private static final int NUM_INSTANCES = 3;

  private final String _helixClusterName = getHelixClusterName();

  @BeforeClass
  public void setup() throws Exception {
    startZk();
    startController();
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(_helixClusterName,
        ZkStarter.DEFAULT_ZK_STR, NUM_INSTANCES, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(_helixClusterName, ZkStarter.DEFAULT_ZK_STR,
        NUM_INSTANCES, true);
  }

  @Test
  public void testInstanceToggle() throws Exception {
    // Create an offline table
    String tableJSONConfigString =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
            .setNumReplicas(NUM_INSTANCES)
            .build()
            .toJSONConfigString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableJSONConfigString);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 1);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getInstanceSet(OFFLINE_TABLE_NAME)
            .size(), NUM_INSTANCES);

    // Add segments
    for (int i = 0; i < NUM_INSTANCES; i++) {
      _helixResourceManager.addNewSegment(new SimpleSegmentMetadata(RAW_TABLE_NAME), "downloadUrl");
      Assert.assertEquals(_helixAdmin.getResourceIdealState(_helixClusterName, OFFLINE_TABLE_NAME).getNumPartitions(),
          i + 1);
    }

    // Disable server instances
    int numEnabledInstances = NUM_INSTANCES;
    for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, SERVER_TENANT_NAME)) {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceState(instanceName), "disable");
      checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, --numEnabledInstances);
    }

    // Enable server instances
    for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, SERVER_TENANT_NAME)) {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceState(instanceName), "ENABLE");
      checkNumOnlineInstancesFromExternalView(OFFLINE_TABLE_NAME, ++numEnabledInstances);
    }

    // Disable broker instances
    for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, BROKER_TENANT_NAME)) {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceState(instanceName), "Disable");
      checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, --numEnabledInstances);
    }

    // Enable broker instances
    for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, BROKER_TENANT_NAME)) {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceState(instanceName), "Enable");
      checkNumOnlineInstancesFromExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, ++numEnabledInstances);
    }

    // Delete table
    sendDeleteRequest(_controllerRequestURLBuilder.forTableDelete(RAW_TABLE_NAME));
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet()
            .size(), 0);
  }

  private void checkNumOnlineInstancesFromExternalView(String resourceName, int expectedNumOnlineInstances)
      throws InterruptedException {
    long endTime = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < endTime) {
      ExternalView resourceExternalView = _helixAdmin.getResourceExternalView(_helixClusterName, resourceName);
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
    stopController();
    stopZk();
  }
}
