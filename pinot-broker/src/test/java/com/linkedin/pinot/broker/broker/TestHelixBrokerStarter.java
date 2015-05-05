/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.broker.broker.helix.DefaultHelixBrokerConfig;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.query.utils.SimpleSegmentMetadata;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;


public class TestHelixBrokerStarter {

  private static Logger LOGGER = LoggerFactory.getLogger(TestHelixBrokerStarter.class);
  private PinotHelixResourceManager _pinotResourceManager;
  private final static String HELIX_CLUSTER_NAME = "TestHelixBrokerStarter";

  private ZkClient _zkClient;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private HelixBrokerStarter _helixBrokerStarter;

  @BeforeTest
  public void setUp() throws Exception {
    ZkTestUtils.startLocalZkServer();
    _zkClient = new ZkClient(ZkTestUtils.DEFAULT_ZK_STR);
    final String instanceId = "localhost_helixController";
    _pinotResourceManager = new PinotHelixResourceManager(ZkTestUtils.DEFAULT_ZK_STR, HELIX_CLUSTER_NAME, instanceId, null);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZkTestUtils.DEFAULT_ZK_STR);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    Thread.sleep(3000);
    final Configuration pinotHelixBrokerProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf();
    pinotHelixBrokerProperties.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, 8943);
    _helixBrokerStarter =
        new HelixBrokerStarter(HELIX_CLUSTER_NAME, ZkTestUtils.DEFAULT_ZK_STR, pinotHelixBrokerProperties);

    Thread.sleep(1000);
    addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkTestUtils.DEFAULT_ZK_STR, 5);
    addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkTestUtils.DEFAULT_ZK_STR, 1);

    _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(6, "broker_colocated"));

    final String dataResource = "company";
    final DataResource resource = createOfflineClusterConfig(1, 1, dataResource, "BalanceNumSegmentAssignmentStrategy");
    _pinotResourceManager.handleCreateNewDataResource(resource);

    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE,
        CommonConstants.Helix.DataSourceRequestType.ADD_TABLE_TO_RESOURCE);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, dataResource);
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, "testTable");
    props.put(CommonConstants.Helix.DataSource.RESOURCE_TYPE, CommonConstants.Helix.ResourceType.OFFLINE.name());

    final DataResource addTableResource = DataResource.fromMap(props);
    _pinotResourceManager.handleAddTableToDataResource(addTableResource);

    for (int i = 1; i <= 5; i++) {
      addOneSegment(dataResource);
      Thread.sleep(2000);
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME,
          BrokerRequestUtils.getOfflineResourceNameForResource(dataResource));
      Assert.assertEquals(externalView.getPartitionSet().size(), i);
    }
  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
    ZkTestUtils.stopLocalZkServer();
  }

  @Test
  public void testResourceAndTagAssignment() throws Exception {
    final String COMPANY_RESOURCE_NAME = BrokerRequestUtils.getOfflineResourceNameForResource("company");
    final String CAP_RESOURCE_NAME = BrokerRequestUtils.getOfflineResourceNameForResource("cap");
    PinotResourceManagerResponse res;
    IdealState idealState;

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet(COMPANY_RESOURCE_NAME).size(), 6);

    ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(externalView.getStateMap(COMPANY_RESOURCE_NAME).size(), 6);
    Thread.sleep(2000);
    HelixExternalViewBasedRouting helixExternalViewBasedRouting = _helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert.assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[company_O]");

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig(CAP_RESOURCE_NAME, 6, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet(CAP_RESOURCE_NAME).size(), 6);
    Assert.assertEquals(idealState.getInstanceSet(COMPANY_RESOURCE_NAME).size(), 6);

    Thread.sleep(3000);
    externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(externalView.getStateMap(CAP_RESOURCE_NAME).size(), 6);
    helixExternalViewBasedRouting = _helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert
        .assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[company_O]");

    Set<String> serverSet = helixExternalViewBasedRouting.getBrokerRoutingTable().get(COMPANY_RESOURCE_NAME).get(0).getServerSet();
    Assert.assertEquals(
        helixExternalViewBasedRouting.getBrokerRoutingTable().get(COMPANY_RESOURCE_NAME).get(0)
            .getSegmentSet(serverSet.iterator().next()).size(), 5);

    final String dataResource = COMPANY_RESOURCE_NAME;
    addOneSegment(dataResource);

    Thread.sleep(2000);
    externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, COMPANY_RESOURCE_NAME);
    Assert.assertEquals(externalView.getPartitionSet().size(), 6);
    helixExternalViewBasedRouting = _helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert.assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[company_O]");

    serverSet = helixExternalViewBasedRouting.getBrokerRoutingTable().get(COMPANY_RESOURCE_NAME).get(0).getServerSet();
    Assert.assertEquals(
        helixExternalViewBasedRouting.getBrokerRoutingTable().get(COMPANY_RESOURCE_NAME).get(0)
            .getSegmentSet(serverSet.iterator().next()).size(), 6);

  }

  public void testWithCmdLines() throws Exception {

    final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      final String command = br.readLine();
      if ((command != null) && command.equals("exit")) {
        tearDown();
      }
    }
  }

  public static BrokerDataResource createBrokerDataResourceConfig(String resourceName, int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Broker.DataResource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Broker.DataResource.NUM_BROKER_INSTANCES, numInstances + "");
    props.put(CommonConstants.Broker.DataResource.TAG, tag);
    final BrokerDataResource res = BrokerDataResource.fromMap(props);
    return res;
  }

  public static BrokerTagResource createBrokerTagResourceConfig(int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Broker.TagResource.TAG, tag);
    props.put(CommonConstants.Broker.TagResource.NUM_BROKER_INSTANCES, numInstances + "");
    final BrokerTagResource res = BrokerTagResource.fromMap(props);
    return res;
  }

  public static DataResource createOfflineClusterConfig(int numInstances, int numReplicas, String resourceName,
      String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE, CommonConstants.Helix.DataSourceRequestType.CREATE);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, resourceName);
    props.put(CommonConstants.Helix.DataSource.TIME_COLUMN_NAME, "days");
    props.put(CommonConstants.Helix.DataSource.TIME_TYPE, "daysSinceEpoch");
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_DATA_INSTANCES, String.valueOf(numInstances));
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_COPIES, String.valueOf(numReplicas));
    props.put(CommonConstants.Helix.DataSource.RETENTION_TIME_UNIT, "DAYS");
    props.put(CommonConstants.Helix.DataSource.RETENTION_TIME_VALUE, "30");
    props.put(CommonConstants.Helix.DataSource.PUSH_FREQUENCY, "daily");
    props.put(CommonConstants.Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, segmentAssignmentStrategy);
    props.put(CommonConstants.Helix.DataSource.BROKER_TAG_NAME, "colocated");
    props.put(CommonConstants.Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, "6");
    props.put(CommonConstants.Helix.DataSource.RESOURCE_TYPE, CommonConstants.Helix.ResourceType.OFFLINE.name());
    final DataResource res = DataResource.fromMap(props);
    return res;
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Server_localhost_" + i;

      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptySegmentOnlineOfflineStateModelFactory();
      stateMachineEngine.registerStateModelFactory(EmptySegmentOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      helixZkManager.connect();
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Broker_localhost_" + i;

      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new EmptyBrokerOnlineOfflineStateModelFactory();
      stateMachineEngine.registerStateModelFactory(EmptyBrokerOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      helixZkManager.connect();
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
  }

  private void addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotResourceManager.addSegment(segmentMetadata, "http://localhost:something");
  }

  public static void main(String[] args) throws IOException {
    final List<HelixBrokerStarter> pinotHelixStarters = new ArrayList<HelixBrokerStarter>();
    final Configuration pinotHelixProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf();

    HelixBrokerStarter pinotHelixBrokerStarter;
    try {
      pinotHelixProperties.addProperty("instanceId", "localhost_111");
      pinotHelixBrokerStarter = new HelixBrokerStarter("sprintDemoCluster", "dpatel-ld:2181", pinotHelixProperties);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      String command = br.readLine();
      if (command.equals("exit")) {

      }
    }
  }
}
