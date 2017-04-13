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
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import java.util.Set;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ControllerInstanceToggleTest extends ControllerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerInstanceToggleTest.class);
  private static final String HELIX_CLUSTER_NAME = "ControllerInstanceToggleTest";
  static ZkClient _zkClient = null;

  private PinotHelixResourceManager _pinotResourceManager;

  @BeforeClass
  public void setup() throws Exception {
    startZk();
    _zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    startController();
    _pinotResourceManager = _controllerStarter.getHelixResourceManager();
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 20, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 20, true);
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

  private void pollForEqualityAssertion(Function<Object, Object> value, Object expectedValue, long timeoutMillis,
      long iterationSleepMillis) {
    long timeLimit = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < timeLimit) {
      // Exit if the equality predicate holds
      if (expectedValue.equals(value.apply(null))) {
        return;
      }

      Uninterruptibles.sleepUninterruptibly(iterationSleepMillis, TimeUnit.MILLISECONDS);
    }

    Assert.assertEquals(value.apply(null), expectedValue);
  }

  @Test
  public void testInstanceToggle()
      throws Exception {
    // Create offline table creation request
    String tableName = "testTable";
    String tableJSONConfigString =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName)
            .setNumReplicas(20)
            .build()
            .toJSONConfigString();
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableCreate(),
        tableJSONConfigString);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet().size(), 1);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getInstanceSet(tableName + "_OFFLINE").size(), 20);

    // Adding segments
    for (int i = 0; i < 10; ++i) {
      Assert.assertEquals(_helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, tableName + "_OFFLINE")
          .getNumPartitions(), i);
      addOneOfflineSegment(tableName);

      final int expectedSegmentCount = i + 1;
      pollForEqualityAssertion(new Function<Object, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable Object input) {
          return _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, tableName + "_OFFLINE").getNumPartitions();
        }
      }, expectedSegmentCount, 2000, 10);
    }

    // Disable Instance
    int i = 20;
    for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE")) {
      ExternalView resourceExternalView =
          _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, tableName + "_OFFLINE");
      Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
      Assert.assertEquals(instanceSet.size(), i--);
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceState(instanceName), "disable");

      final int expectedInstanceSetSize = i;
      pollForEqualityAssertion(new Function<Object, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable Object input) {
          ExternalView resourceExternalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, tableName + "_OFFLINE");
          Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
          return instanceSet.size();
        }
      }, expectedInstanceSetSize, 2000, 10);
      LOGGER.trace("Current running server instance: " + instanceSet.size());
    }

    // Enable Instance
    i = 0;
    for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE")) {
      ExternalView resourceExternalView =
          _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, tableName + "_OFFLINE");
      Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
      Assert.assertEquals(instanceSet.size(), i++);
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceState(instanceName), "ENABLE");

      final int expectedInstanceSetSize = i;
      pollForEqualityAssertion(new Function<Object, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable Object input) {
          ExternalView resourceExternalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, tableName + "_OFFLINE");
          Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
          return instanceSet.size();
        }
      }, expectedInstanceSetSize, 2000, 10);
      LOGGER.trace("Current running server instance: " + instanceSet.size());
    }

    // Disable BrokerInstance
    i = 20;
    for (String instanceName : _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER")) {
      ExternalView resourceExternalView =
          _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
      Assert.assertEquals(instanceSet.size(), i--);
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceState(instanceName), "Disable");

      final int expectedInstanceSetSize = i;
      pollForEqualityAssertion(new Function<Object, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable Object input) {
          ExternalView resourceExternalView =
              _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
          Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
          return instanceSet.size();
        }
      }, expectedInstanceSetSize, 2000, 10);
      LOGGER.trace("Current running broker instance: " + instanceSet.size());
    }

    // Enable BrokerInstance
    i = 0;
    for (final String instanceName : _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER")) {
      ExternalView resourceExternalView =
          _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
      Assert.assertEquals(instanceSet.size(), i++);
      sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceState(instanceName), "Enable");

      final int expectedInstanceSetSize = i;
      pollForEqualityAssertion(new Function<Object, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable Object input) {
          ExternalView resourceExternalView =
              _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
          Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
          return instanceSet.size();
        }
      }, expectedInstanceSetSize, 2000, 10);
      LOGGER.trace("Current running broker instance: " + instanceSet.size());
    }
    // Delete table
    sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forTableDelete(tableName));
    Thread.sleep(2000);
    Assert.assertEquals(
        _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getPartitionSet().size(), 0);

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
