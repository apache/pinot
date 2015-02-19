package com.linkedin.pinot.broker.broker;

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
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import com.linkedin.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;


public class TestHelixBrokerStarter {

  private static Logger LOGGER = LoggerFactory.getLogger(TestHelixBrokerStarter.class);
  private PinotHelixResourceManager _pinotResourceManager;
  private final static String ZK_SERVER = "localhost:2181";
  private final static String HELIX_CLUSTER_NAME = "TestHelixBrokerStarter";

  private final ZkClient _zkClient = new ZkClient(ZK_SERVER);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private List<HelixBrokerStarter> _helixBrokerStarters;

  @BeforeTest
  public void setUp() throws Exception {
    final String zkPath = "/" + HELIX_CLUSTER_NAME;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    final String instanceId = "localhost_helixController";
    _pinotResourceManager = new PinotHelixResourceManager(ZK_SERVER, HELIX_CLUSTER_NAME, instanceId, null);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(ZK_SERVER);
    _helixZkManager = HelixSetupUtils.setup(HELIX_CLUSTER_NAME, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    Thread.sleep(3000);
    _helixBrokerStarters = addHelixBrokerInstancesToAutoJoinHelixCluster(1);
    addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_SERVER, 5);
    addDataServerInstancesToAutoJoinHelixCluster(1);

    _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(6, "broker_colocated"));

    final String dataResource = "company";
    final DataResource resource = createOfflineClusterConfig(1, 1, dataResource, "BalanceNumSegmentAssignmentStrategy");
    _pinotResourceManager.handleCreateNewDataResource(resource);

    final Map<String, String> props = new HashMap<String, String>();
    props.put(CommonConstants.Helix.DataSource.REQUEST_TYPE,
        CommonConstants.Helix.DataSourceRequestType.ADD_TABLE_TO_RESOURCE);
    props.put(CommonConstants.Helix.DataSource.RESOURCE_NAME, dataResource);
    props.put(CommonConstants.Helix.DataSource.TABLE_NAME, "testTable");

    final DataResource addTableResource = DataResource.fromMap(props);
    _pinotResourceManager.handleAddTableToDataResource(addTableResource);

    for (int i = 1; i <= 5; i++) {
      addOneSegment(dataResource);
      Thread.sleep(2000);
      final ExternalView externalView = _helixAdmin.getResourceExternalView(HELIX_CLUSTER_NAME, dataResource);
      Assert.assertEquals(externalView.getPartitionSet().size(), i);
    }
  }

  @AfterTest
  public void tearDown() {
    _pinotResourceManager.stop();
    _zkClient.close();
  }

  @Test
  public void testResourceAndTagAssignment() throws Exception {
    PinotResourceManagerResponse res;
    IdealState idealState;

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 6);

    Thread.sleep(3000);
    final HelixBrokerStarter helixBrokerStarter = _helixBrokerStarters.get(0);
    HelixExternalViewBasedRouting helixExternalViewBasedRouting = helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert.assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[company]");

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig("cap", 6, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_colocated").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(HELIX_CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 6);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 6);

    Thread.sleep(3000);
    helixExternalViewBasedRouting = helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert
        .assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[company]");

    Set<String> serverSet = helixExternalViewBasedRouting.getBrokerRoutingTable().get("company").get(0).getServerSet();
    Assert.assertEquals(
        helixExternalViewBasedRouting.getBrokerRoutingTable().get("company").get(0)
            .getSegmentSet(serverSet.iterator().next()).size(), 5);

    final String dataResource = "company";
    addOneSegment(dataResource);
    Thread.sleep(2000);
    helixExternalViewBasedRouting = helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert
        .assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[company]");

    serverSet = helixExternalViewBasedRouting.getBrokerRoutingTable().get("company").get(0).getServerSet();
    Assert.assertEquals(
        helixExternalViewBasedRouting.getBrokerRoutingTable().get("company").get(0)
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
    final DataResource res = DataResource.fromMap(props);
    return res;
  }

  private List<HelixBrokerStarter> addHelixBrokerInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    final List<HelixBrokerStarter> pinotHelixStarters = new ArrayList<HelixBrokerStarter>();
    final Configuration pinotHelixBrokerProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf();

    final HelixBrokerStarter pinotHelixBrokerStarter =
        new HelixBrokerStarter(HELIX_CLUSTER_NAME, ZK_SERVER, pinotHelixBrokerProperties);
    pinotHelixStarters.add(pinotHelixBrokerStarter);
    Thread.sleep(1000);

    return pinotHelixStarters;
  }

  private List<HelixServerStarter> addDataServerInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    final List<HelixServerStarter> pinotHelixStarters = new ArrayList<HelixServerStarter>();
    for (int i = 0; i < numInstances; ++i) {
      final HelixServerStarter pinotHelixStarter =
          new HelixServerStarter(HELIX_CLUSTER_NAME, ZK_SERVER, new PropertiesConfiguration());
      pinotHelixStarters.add(pinotHelixStarter);
      Thread.sleep(1000);
    }
    return pinotHelixStarters;
  }

  public static void addFakeDataInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String instanceId = "Server_localhost_" + i;

      final HelixManager helixZkManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer);
      final StateMachineEngine stateMachineEngine = helixZkManager.getStateMachineEngine();
      final StateModelFactory<?> stateModelFactory = new SegmentOnlineOfflineStateModelFactory();
      stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelDef(),
          stateModelFactory);
      helixZkManager.connect();
      helixZkManager.getClusterManagmentTool().addInstanceTag(helixClusterName, instanceId,
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(String helixClusterName, String zkServer,
      int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixManager =
          HelixManagerFactory.getZKHelixManager(helixClusterName, brokerId, InstanceType.PARTICIPANT, zkServer);
      helixManager.connect();
      helixManager.getClusterManagmentTool().addInstanceTag(helixClusterName, brokerId,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      Thread.sleep(1000);
    }
  }

  private void addOneSegment(String resourceName) {
    final SegmentMetadata segmentMetadata = new SimpleSegmentMetadata(resourceName, "testTable");
    LOGGER.info("Trying to add IndexSegment : " + segmentMetadata.getName());
    _pinotResourceManager.addSegment(segmentMetadata, "http://localhost:something");
  }

  private void deleteOneSegment(String resource, String segment) {
    LOGGER.info("Trying to delete Segment : " + segment + " from resource : " + resource);
    _pinotResourceManager.deleteSegment(resource, segment);
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
