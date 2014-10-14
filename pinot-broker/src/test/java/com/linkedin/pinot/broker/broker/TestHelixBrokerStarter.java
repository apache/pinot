package com.linkedin.pinot.broker.broker;

import java.io.BufferedReader;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.broker.broker.helix.DefaultHelixBrokerConfig;
import com.linkedin.pinot.broker.broker.helix.HelixBrokerStarter;
import com.linkedin.pinot.common.segment.SegmentMetadata;
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
import com.linkedin.pinot.server.starter.helix.PinotHelixStarter;


public class TestHelixBrokerStarter {

  private static Logger LOGGER = LoggerFactory.getLogger(TestHelixBrokerStarter.class);
  private PinotHelixResourceManager _pinotResourceManager;
  private final String _zkServer = "localhost:2181";
  private final String _helixClusterName = "TestHelixBrokerStarter";

  private final ZkClient _zkClient = new ZkClient(_zkServer);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private List<HelixBrokerStarter> _helixbBrokerStarters;

  @BeforeTest
  private void setUp() throws Exception {
    final String zkPath = "/" +  _helixClusterName;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    final String instanceId = "localhost_helixController";
    _pinotResourceManager = new PinotHelixResourceManager(_zkServer, _helixClusterName, instanceId);
    _pinotResourceManager.start();

    final String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkServer);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    Thread.sleep(3000);
    _helixbBrokerStarters = addInstancesToAutoJoinHelixCluster(1);
    addFakeInstancesToAutoJoinHelixCluster(5);
    addDataServerInstancesToAutoJoinHelixCluster(1);

    final String dataResource = "company";
    final DataResource resource = createOfflineClusterConfig(1, 1, dataResource, "BalanceNumSegmentAssignmentStrategy");
    _pinotResourceManager.createDataResource(resource);

    for (int i = 1; i <= 5; i++) {
      addOneSegment(dataResource);
      Thread.sleep(2000);
      final ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, dataResource);
      Assert.assertEquals(externalView.getPartitionSet().size(), i);
    }
  }

  @AfterTest
  private void tearDown() {
    _pinotResourceManager.stop();
    final String zkPath = "/" + _helixClusterName;
    if (_zkClient.exists(zkPath)) {
      _zkClient.deleteRecursive(zkPath);
    }
    _zkClient.close();
  }

  @Test
  public void testResourceAndTagAssignment() throws Exception {
    PinotResourceManagerResponse res;
    IdealState idealState;

    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(6, "broker_colocated"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);

    res =
        _pinotResourceManager
        .createBrokerDataResource(createBrokerDataResourceConfig("company", 6, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 6);

    Thread.sleep(3000);
    final HelixBrokerStarter helixBrokerStarter = _helixbBrokerStarters.get(0);
    HelixExternalViewBasedRouting helixExternalViewBasedRouting = helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert.assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[company]");

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig("cap", 6, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 6);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 6);

    Thread.sleep(3000);
    helixExternalViewBasedRouting = helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert
    .assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[cap, company]");

    Set<String> serverSet = helixExternalViewBasedRouting.getBrokerRoutingTable().get("company").get(0).getServerSet();
    Assert.assertEquals(
        helixExternalViewBasedRouting.getBrokerRoutingTable().get("company").get(0)
        .getSegmentSet(serverSet.iterator().next()).size(), 5);

    final String dataResource = "company";
    addOneSegment(dataResource);
    Thread.sleep(2000);
    helixExternalViewBasedRouting = helixBrokerStarter.getHelixExternalViewBasedRouting();
    Assert
    .assertEquals(Arrays.toString(helixExternalViewBasedRouting.getDataResourceSet().toArray()), "[cap, company]");

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
    props.put("resourceName", resourceName);
    props.put("numBrokerInstances", numInstances + "");
    props.put("tag", tag);
    final BrokerDataResource res = BrokerDataResource.fromMap(props);
    return res;
  }

  public static BrokerTagResource createBrokerTagResourceConfig(int numInstances, String tag) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("tag", tag);
    props.put("numBrokerInstances", numInstances + "");
    final BrokerTagResource res = BrokerTagResource.fromMap(props);
    return res;
  }

  private List<HelixBrokerStarter> addInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    final List<HelixBrokerStarter> pinotHelixStarters = new ArrayList<HelixBrokerStarter>();
    final Configuration pinotHelixProperties = DefaultHelixBrokerConfig.getDefaultBrokerConf();

    final HelixBrokerStarter pinotHelixBrokerStarter =
        new HelixBrokerStarter(_helixClusterName, _zkServer, pinotHelixProperties);
    pinotHelixStarters.add(pinotHelixBrokerStarter);
    Thread.sleep(1000);

    return pinotHelixStarters;
  }

  private void addFakeInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      final String brokerId = "Broker_localhost_" + i;
      final HelixManager helixManager =
          HelixManagerFactory.getZKHelixManager(_helixClusterName, brokerId, InstanceType.PARTICIPANT, _zkServer);
      helixManager.connect();
      helixManager.getClusterManagmentTool().addInstanceTag(_helixClusterName, brokerId,
          PinotHelixResourceManager.KEY_OF_UNTAGGED_BROKER_RESOURCE);
      Thread.sleep(1000);
    }
  }

  private List<PinotHelixStarter> addDataServerInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    final List<PinotHelixStarter> pinotHelixStarters = new ArrayList<PinotHelixStarter>();
    for (int i = 0; i < numInstances; ++i) {
      final PinotHelixStarter pinotHelixStarter =
          new PinotHelixStarter(_helixClusterName, _zkServer, new PropertiesConfiguration());
      pinotHelixStarters.add(pinotHelixStarter);
      Thread.sleep(1000);
    }
    return pinotHelixStarters;
  }

  public static BrokerDataResource createBrokerConfig(String resourceName) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("numBrokerInstances", "1");
    props.put("isCoLocated", "true");
    final BrokerDataResource res = BrokerDataResource.fromMap(props);
    return res;
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

  public static DataResource createOfflineClusterConfig(int numInstancesPerReplica, int numReplicas,
      String resourceName, String segmentAssignmentStrategy) {
    final Map<String, String> props = new HashMap<String, String>();
    props.put("resourceName", resourceName);
    props.put("tableName", "testTable");
    props.put("timeColumnName", "days");
    props.put("timeType", "daysSinceEpoch");
    props.put("numInstances", String.valueOf(numInstancesPerReplica));
    props.put("numReplicas", String.valueOf(numReplicas));
    props.put("retentionTimeUnit", "DAYS");
    props.put("retentionTimeValue", "30");
    props.put("pushFrequency", "daily");
    props.put("segmentAssignmentStrategy", segmentAssignmentStrategy);
    final DataResource res = DataResource.fromMap(props);
    return res;
  }
}
