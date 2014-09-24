package com.linkedin.pinot.controller.helix;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.pinot.controller.helix.api.PinotStandaloneResource;
import com.linkedin.pinot.controller.helix.api.request.UpdateResourceConfigUpdateRequest;
import com.linkedin.pinot.controller.helix.core.CreateRequestJSONTransformer;
import com.linkedin.pinot.controller.helix.core.HelixSetupUtils;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixStateModelFactory;
import com.linkedin.pinot.controller.helix.core.PinotHelixStateModelGenerator;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.controller.helix.util.ClusterCreateJSONUtil;


public class TestPinotResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(TestPinotResourceManager.class);
  private PinotHelixResourceManager _pinotResourceManager;
  private String _zkServer = "localhost:2181";
  private String _helixClusterName = "pinotClusterOne";

  private ZkClient _zkClient = new ZkClient(_zkServer);
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private int _numInstance;

  private static String UNTAGGED = "untagged";

  @BeforeTest
  private void setUp() throws Exception {

    if (_zkClient.exists("/pinot-helix")) {
      _zkClient.deleteRecursive("/pinot-helix");
    }
    String instanceId = "localhost:helixController";
    _pinotResourceManager = new PinotHelixResourceManager(_zkServer, _helixClusterName, instanceId);
    _pinotResourceManager.start();

    String helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkServer);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, helixZkURL, instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
  }

  private void addInstancesToJoinHelixCluster(int numInstances) {
    for (int i = 0; i < numInstances; ++i) {
      String instanceId = "localhost:" + i;
      InstanceConfig instanceConfig = new InstanceConfig(instanceId);
      _helixAdmin.addInstance(_helixClusterName, instanceConfig);
      _helixAdmin.addInstanceTag(_helixClusterName, instanceId, UNTAGGED);
    }
  }

  private List<HelixManager> addInstancesToAutoJoinHelixCluster(int numInstances) throws Exception {
    List<HelixManager> helixManagers = new ArrayList<HelixManager>();
    for (int i = 0; i < numInstances; ++i) {
      String instanceId = "localhost_" + i;
      HelixManager helixManager =
          HelixManagerFactory.getZKHelixManager(_helixClusterName, instanceId, InstanceType.PARTICIPANT, _zkServer
              + "/pinot-helix");
      StateMachineEngine stateMachineEngine = helixManager.getStateMachineEngine();
      stateMachineEngine.registerStateModelFactory(PinotHelixStateModelGenerator.PINOT_HELIX_STATE_MODEL,
          new PinotHelixStateModelFactory(instanceId, _helixClusterName));
      // new OnlineOfflineStateModelFactory(0));
      helixManager.connect();
      _helixAdmin.addInstanceTag(_helixClusterName, instanceId, UNTAGGED);
      helixManagers.add(helixManager);
    }
    return helixManagers;
  }

  @AfterTest
  private void tearDown() {
    _pinotResourceManager.stop();
    if (_zkClient.exists("/pinot-helix")) {
      _zkClient.deleteRecursive("/pinot-helix");
    }
  }

  @Test
  public void testCreateNewResource() throws Exception {
    _numInstance = 20;
    addInstancesToJoinHelixCluster(_numInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    String resourceName = "testCreateNewResource";
    JSONObject serverJSON = ClusterCreateJSONUtil.createOfflineClusterJSON(2, 3, resourceName);
    JSONObject externalJSON = new JSONObject();
    externalJSON.put("d2ClusterName", "testCreateNewResourceD2");
    PinotStandaloneResource resource = CreateRequestJSONTransformer.buildOfflineResource(serverJSON, externalJSON);
    _pinotResourceManager.createResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);
  }

  @Test
  public void testDropExistedResource() throws Exception {
    _numInstance = 20;
    addInstancesToJoinHelixCluster(_numInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    String resourceName = "testCreateNewConfigForExistedResource";
    JSONObject serverJSON = ClusterCreateJSONUtil.createOfflineClusterJSON(2, 3, resourceName);
    JSONObject externalJSON = new JSONObject();
    externalJSON.put("d2ClusterName", "testCreateNewConfigForExistedResourceD2");
    PinotStandaloneResource resource = CreateRequestJSONTransformer.buildOfflineResource(serverJSON, externalJSON);
    _pinotResourceManager.createResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);

    _pinotResourceManager.deleteResource(resource.getTag());
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 0);
  }

  @Test
  public void testCreateExistedResourceWithParticipate() throws Exception {
    _numInstance = 20;
    List<HelixManager> helixManagers = addInstancesToAutoJoinHelixCluster(_numInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    String resourceName = "testCreateNewConfigForExistedResource";
    JSONObject serverJSON = ClusterCreateJSONUtil.createOfflineClusterJSON(2, 3, resourceName);
    JSONObject externalJSON = new JSONObject();
    externalJSON.put("d2ClusterName", "testCreateNewConfigForExistedResourceD2");
    PinotStandaloneResource resource = CreateRequestJSONTransformer.buildOfflineResource(serverJSON, externalJSON);
    _pinotResourceManager.createResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);
    _pinotResourceManager
        .startInstances(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()));
    Thread.sleep(3000);
    //    _pinotResourceManager.deleteResource(resource.getTag());
    //    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    //    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 0);
  }

  @Test
  public void testCreateNewConfigForExistedResource() throws Exception {
    _numInstance = 20;
    addInstancesToJoinHelixCluster(_numInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    String resourceName = "testCreateNewConfigForExistedResource";
    JSONObject serverJSON = ClusterCreateJSONUtil.createOfflineClusterJSON(1, 1, resourceName);
    JSONObject externalJSON = new JSONObject();
    externalJSON.put("d2ClusterName", "testCreateNewConfigForExistedResourceD2");
    PinotStandaloneResource resource = CreateRequestJSONTransformer.buildOfflineResource(serverJSON, externalJSON);
    _pinotResourceManager.createResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);

    // Add new resource configs
    UpdateResourceConfigUpdateRequest updateResourceConfigRequest = new UpdateResourceConfigUpdateRequest(resourceName);
    updateResourceConfigRequest.setBounceService(true);
    for (int i = 0; i < 10; ++i) {
      updateResourceConfigRequest.addProperty("resource.test.config." + i, "testValue" + i);
    }
    _pinotResourceManager.updateResource(updateResourceConfigRequest);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);

  }

  @Test
  public void testUpdateConfigForExistedResource() throws Exception {
    _numInstance = 20;
    addInstancesToJoinHelixCluster(_numInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    String resourceName = "testCreateNewConfigForExistedResource";
    JSONObject serverJSON = ClusterCreateJSONUtil.createOfflineClusterJSON(1, 1, resourceName);
    JSONObject externalJSON = new JSONObject();
    externalJSON.put("d2ClusterName", "testCreateNewConfigForExistedResourceD2");
    PinotStandaloneResource resource = CreateRequestJSONTransformer.buildOfflineResource(serverJSON, externalJSON);
    _pinotResourceManager.createResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);

    // Add new resource configs
    UpdateResourceConfigUpdateRequest updateResourceConfigRequest = new UpdateResourceConfigUpdateRequest(resourceName);
    updateResourceConfigRequest.setBounceService(true);
    for (int i = 0; i < 10; ++i) {
      updateResourceConfigRequest.addProperty("resource.test.config." + i, "testValue" + i);
    }
    _pinotResourceManager.updateResource(updateResourceConfigRequest);

    // Update existed resource configs
    UpdateResourceConfigUpdateRequest newUpdateResourceConfigRequest =
        new UpdateResourceConfigUpdateRequest(resourceName);
    updateResourceConfigRequest.setBounceService(true);
    for (int i = 0; i < 10; ++i) {
      newUpdateResourceConfigRequest.addProperty("resource.test.config." + i, "testValue" + (i * 10));
    }
    _pinotResourceManager.updateResource(newUpdateResourceConfigRequest);
  }

  @Test
  public void testDeleteConfigForExistedResource() throws Exception {
    _numInstance = 20;
    addInstancesToJoinHelixCluster(_numInstance);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance);
    String resourceName = "testCreateNewConfigForExistedResource";
    JSONObject serverJSON = ClusterCreateJSONUtil.createOfflineClusterJSON(1, 1, resourceName);
    JSONObject externalJSON = new JSONObject();
    externalJSON.put("d2ClusterName", "testCreateNewConfigForExistedResourceD2");
    PinotStandaloneResource resource = CreateRequestJSONTransformer.buildOfflineResource(serverJSON, externalJSON);
    _pinotResourceManager.createResource(resource);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED).size(), _numInstance - 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag()).size(), 6);

    // Add new resource configs
    UpdateResourceConfigUpdateRequest updateResourceConfigRequest = new UpdateResourceConfigUpdateRequest(resourceName);
    updateResourceConfigRequest.setBounceService(true);
    // Delete exsited resource configs
    for (int i = 0; i < 10; ++i) {
      updateResourceConfigRequest.removeProperty("resource.test.config." + i);
    }
    _pinotResourceManager.updateResource(updateResourceConfigRequest);
  }

}
