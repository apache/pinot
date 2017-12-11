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
package com.linkedin.pinot.broker.broker;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.broker.broker.helix.DefaultHelixBrokerConfig;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.broker.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.util.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class HelixBrokerStarterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixBrokerStarterTest.class);
  private static final int SEGMENT_COUNT = 6;
  private PinotHelixResourceManager _pinotResourceManager;
  private final static String HELIX_CLUSTER_NAME = "TestHelixBrokerStarter";
  private final static String DINING_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType("dining");
  private final static String COFFEE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType("coffee");

  private ZkClient _zkClient;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private HelixBrokerStarter _helixBrokerStarter;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  @BeforeTest
  public void setUp() throws Exception {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    _zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    final String instanceId = "localhost_helixController";
    _pinotResourceManager =
        new PinotHelixResourceManager(ZkStarter.DEFAULT_ZK_STR, HELIX_CLUSTER_NAME, instanceId, null, 10000L, true, /*isUpdateStateModel=*/false);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZkStarter.DEFAULT_ZK_STR);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId, /*isUpdateStateModel=*/false);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();

    final Configuration pinotHelixBrokerProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    pinotHelixBrokerProperties.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, 8943);
    _helixBrokerStarter =
        new HelixBrokerStarter(HELIX_CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR, pinotHelixBrokerProperties);

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 5, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME,
        ZkStarter.DEFAULT_ZK_STR, 1, true);

    while (_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_OFFLINE").size() == 0 ||
        _helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size() == 0) {
      Thread.sleep(100);
    }

    final String tableName = "dining";
    TableConfig tableConfig =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName).build();
    _pinotResourceManager.addTable(tableConfig);

    for (int i = 1; i <= 5; i++) {
      addOneSegment(tableName);
    }

    Thread.sleep(1000);

    final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME,
        TableNameBuilder.OFFLINE.tableNameWithType(tableName));

    Assert.assertEquals(externalView.getPartitionSet().size(), 5);
  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  @Test
  public void testResourceAndTagAssignment() throws Exception {
    IdealState idealState;

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(), 6);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet(DINING_TABLE_NAME).size(), SEGMENT_COUNT);

    ExternalView externalView =
        _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(externalView.getStateMap(DINING_TABLE_NAME).size(), SEGMENT_COUNT);

    HelixExternalViewBasedRouting helixExternalViewBasedRouting = _helixBrokerStarter.getHelixExternalViewBasedRouting();
    Field brokerRoutingTableBuilderMapField;
    brokerRoutingTableBuilderMapField = HelixExternalViewBasedRouting.class.getDeclaredField("_routingTableBuilderMap");
    brokerRoutingTableBuilderMapField.setAccessible(true);

    final Map<String, RoutingTableBuilder> brokerRoutingTableBuilderMap =
        (Map<String, RoutingTableBuilder>)brokerRoutingTableBuilderMapField.get(helixExternalViewBasedRouting);

    // Wait up to 30s for routing table to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return brokerRoutingTableBuilderMap.size() == 1;
      }
    }, 30000L);

    Assert.assertEquals(Arrays.toString(brokerRoutingTableBuilderMap.keySet().toArray()), "[dining_OFFLINE]");

    final String tableName = "coffee";
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(tableName)
        .setBrokerTenant("testBroker")
        .setServerTenant("testServer")
        .build();
    _pinotResourceManager.addTable(tableConfig);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(), 6);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet(COFFEE_TABLE_NAME).size(), SEGMENT_COUNT);
    Assert.assertEquals(idealState.getInstanceSet(DINING_TABLE_NAME).size(), SEGMENT_COUNT);

    // Wait up to 30s for broker external view to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)
            .getStateMap(COFFEE_TABLE_NAME).size() == SEGMENT_COUNT;
      }
    }, 30000L);

    externalView =
        _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(externalView.getStateMap(COFFEE_TABLE_NAME).size(), SEGMENT_COUNT);

    // Wait up to 30s for routing table to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return brokerRoutingTableBuilderMap.size() == 2;
      }
    }, 30000L);

    Object[] tableArray = brokerRoutingTableBuilderMap.keySet().toArray();
    Arrays.sort(tableArray);
    Assert.assertEquals(Arrays.toString(tableArray), "[coffee_OFFLINE, dining_OFFLINE]");

    Assert.assertEquals(
        brokerRoutingTableBuilderMap.get(DINING_TABLE_NAME).getRoutingTables().get(0).values().iterator().next().size(),
        5);

    final String dataResource = DINING_TABLE_NAME;
    addOneSegment(dataResource);

    // Wait up to 30s for external view to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, DINING_TABLE_NAME).getPartitionSet().size() ==
            SEGMENT_COUNT;
      }
    }, 30000L);

    externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, DINING_TABLE_NAME);
    Assert.assertEquals(externalView.getPartitionSet().size(), SEGMENT_COUNT);
    tableArray = brokerRoutingTableBuilderMap.keySet().toArray();
    Arrays.sort(tableArray);
    Assert.assertEquals(Arrays.toString(tableArray), "[coffee_OFFLINE, dining_OFFLINE]");

    // Wait up to 30s for routing table to reach the expected size
    waitForPredicate(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Map<String, List<String>> routingTable =
            brokerRoutingTableBuilderMap.get(DINING_TABLE_NAME).getRoutingTables().get(0);
        return routingTable.values().iterator().next().size() == SEGMENT_COUNT;
      }
    }, 30000L);

    Assert.assertEquals(brokerRoutingTableBuilderMap.get(DINING_TABLE_NAME).getRoutingTables().get(0)
        .values().iterator().next().size(), SEGMENT_COUNT);
  }

  private void waitForPredicate(Callable<Boolean> predicate, long timeout) {
    long deadline = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (predicate.call()) {
          return;
        }
      } catch (Exception e) {
        // Do nothing
      }

      Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }
  }

  private void addOneSegment(String tableName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(tableName);
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotResourceManager.addNewSegment(segmentMetadata, "http://localhost:something");
  }

}
