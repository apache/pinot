package com.linkedin.pinot.controller.helix;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;


public class TestBrokerWithPinotResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(TestBrokerWithPinotResourceManager.class);

  private PinotHelixResourceManager _pinotResourceManager;
  private final String _zkServer = "localhost:2181";
  private final String _helixClusterName = "TestBrokerWithPinotResourceManager";

  private final ZkClient _zkClient = new ZkClient(_zkServer);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _resourceName = "testResource";

  @BeforeTest
  private void setUp() throws Exception {
    String zkPath = "/" + HelixConfig.HELIX_ZK_PATH_PREFIX;
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
    addInstancesToAutoJoinHelixCluster(5);

  }

  private void addInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    for (int i = 0; i < numInstances; ++i) {
      String brokerId = "Broker_localhost_" + i;
      HelixManager helixManager =
          HelixManagerFactory.getZKHelixManager(_helixClusterName, brokerId, InstanceType.PARTICIPANT, _zkServer
              + "/pinot-helix");
      helixManager.connect();
      helixManager.getClusterManagmentTool().addInstanceTag(_helixClusterName, brokerId,
          PinotHelixResourceManager.KEY_OF_UNTAGGED_BROKER_RESOURCE);
      Thread.sleep(1000);
    }
  }

  @AfterTest
  private void tearDown() {
    _pinotResourceManager.stop();
  }

  @Test
  public void testTagAssignment() throws Exception {

    PinotResourceManagerResponse res;
    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(1, "broker_tag0"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 4);
    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(2, "broker_tag1"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag1").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 2);
    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(3, "broker_tag2"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, false);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag1").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 2);
    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(3, "tag2"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, false);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag1").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 2);
    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(3, "broker_tag1"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag0").size(), 1);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag1").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 1);
    res = _pinotResourceManager.deleteBrokerResourceTag("broker_tag0");
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_tag1").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 2);
    res = _pinotResourceManager.deleteBrokerResourceTag("broker_tag1");
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 5);
  }

  @Test
  public void testResourceAndTagAssignment() throws Exception {
    PinotResourceManagerResponse res;
    IdealState idealState;

    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(2, "broker_mirror"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 3);

    res = _pinotResourceManager.createBrokerResourceTag(createBrokerTagResourceConfig(3, "broker_colocated"));
    System.out.println(res);
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig("mirror", 2, "broker_mirror"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);

    res =
        _pinotResourceManager
            .createBrokerDataResource(createBrokerDataResourceConfig("company", 2, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig("scin", 3, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig("cap", 1, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 1);

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig("cap", 3, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 3);

    res = _pinotResourceManager.createBrokerDataResource(createBrokerDataResourceConfig("cap", 2, "broker_colocated"));
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 2);

    res = _pinotResourceManager.deleteBrokerDataResource("company");
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 3);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 0);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 3);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 2);

    res = _pinotResourceManager.deleteBrokerResourceTag("broker_colocated");
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 0);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 3);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 2);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 0);

    res = _pinotResourceManager.deleteBrokerResourceTag("broker_mirror");
    Assert.assertEquals(res.status == STATUS.success, true);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_mirror").size(), 0);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_colocated").size(), 0);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, "broker_untagged").size(), 5);
    idealState = _helixAdmin.getResourceIdealState(_helixClusterName, PinotHelixResourceManager.BROKER_RESOURCE);
    Assert.assertEquals(idealState.getInstanceSet("mirror").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("company").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("scin").size(), 0);
    Assert.assertEquals(idealState.getInstanceSet("cap").size(), 0);
  }

  public void testWithCmdLines() throws Exception {

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      String command = br.readLine();
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
}
