package com.linkedin.pinot.controller.helix.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.controller.helix.api.PinotStandaloneResource;
import com.linkedin.pinot.controller.helix.api.request.UpdateResourceConfigUpdateRequest;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;


public class PinotHelixResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);

  private static String UNTAGGED = "untagged";

  private String _zkBaseUrl;
  private String _helixClusterName;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _helixZkURL;
  private String _instanceId;

  @SuppressWarnings("unused")
  private PinotHelixResourceManager() {

  }

  public PinotHelixResourceManager(String zkURL, String helixClusterName, String controllerInstanceId) {
    this._zkBaseUrl = zkURL;
    this._helixClusterName = helixClusterName;
    this._instanceId = controllerInstanceId;
  }

  public void start() throws Exception {
    ZkClient zkClient = new ZkClient(_zkBaseUrl);
    if (!zkClient.exists("/" + HelixConfig.HELIX_ZK_PATH_PREFIX)) {
      zkClient.createPersistent("/" + HelixConfig.HELIX_ZK_PATH_PREFIX);
    }
    this._helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkBaseUrl);
    this._helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    this._helixAdmin = this._helixZkManager.getClusterManagmentTool();
  }

  public void stop() {
    this._helixZkManager.disconnect();
  }

  public synchronized void createResource(PinotStandaloneResource resource) {
    try {
      // lets add resource configs
      HelixHelper.updateResourceConfigsFor(resource.getResourceProperties(), resource.getTag(), _helixClusterName,
          _helixAdmin);

      // lets add instances now with their configs
      List<String> unTaggedInstanceList = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED);

      int numInstanceToUse = resource.getNumReplicas() * resource.getNumInstancesPerReplica();
      LOGGER.info("Trying to allocate " + numInstanceToUse + " instances.");
      if (unTaggedInstanceList.size() < numInstanceToUse) {
        throw new UnsupportedOperationException("Cannot allocate enough hardware resource.");
      }
      for (int i = 0; i < numInstanceToUse; ++i) {
        LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + resource.getTag());
        _helixAdmin.removeInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), UNTAGGED);
        _helixAdmin.addInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), resource.getTag());
      }
      // now lets build an ideal state
      LOGGER.info("building empty ideal state for resource : " + resource.getTag());
      IdealState idealState = PinotResourceIdealStateBuilder.buildEmptyIdealStateFor(resource);
      _helixAdmin.addResource(_helixClusterName, resource.getTag(), idealState);

      LOGGER.info("successfully added the resource : " + resource.getTag() + " to the cluster");
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e.toString());
      // dropping all instances
      List<String> taggedInstanceList = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getTag());
      for (String instance : taggedInstanceList) {
        LOGGER.info("untag instance : " + instance.toString());
        _helixAdmin.removeInstanceTag(_helixClusterName, instance, resource.getTag());
        _helixAdmin.addInstanceTag(_helixClusterName, instance, UNTAGGED);
      }
      _helixAdmin.dropResource(_helixClusterName, resource.getTag());
      throw new RuntimeException("Error creating cluster, have successfull rolled back", e);
    }
  }

  public boolean updateResource(UpdateResourceConfigUpdateRequest updateResourceConfigRequest) {
    String resourceTag = updateResourceConfigRequest.getResourceName();
    Map<String, String> propsToUpdate = updateResourceConfigRequest.getPropertiesMapToUpdate();
    HelixHelper.updateResourceConfigsFor(propsToUpdate, resourceTag, _helixClusterName, _helixAdmin);
    Set<String> propsKeyToDelete = updateResourceConfigRequest.getPropertiesKeySetToDelete();
    for (String configKey : propsKeyToDelete) {
      HelixHelper.deleteResourcePropertyFromHelix(_helixAdmin, _helixClusterName, resourceTag, configKey);
    }
    if (updateResourceConfigRequest.isBounceService()) {
      restartResource(resourceTag);
    }
    return true;
  }

  public void deleteResource(String resourceTag) {
    List<String> taggedInstanceList = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resourceTag);
    for (String instance : taggedInstanceList) {
      LOGGER.info("untag instance : " + instance.toString());
      _helixAdmin.removeInstanceTag(_helixClusterName, instance, resourceTag);
      _helixAdmin.addInstanceTag(_helixClusterName, instance, UNTAGGED);
    }
    _helixAdmin.dropResource(_helixClusterName, resourceTag);
  }

  public void restartResource(String resourceTag) {
    Set<String> allInstances =
        HelixHelper.getAllInstancesForResource(HelixHelper.getResourceIdealState(_helixZkManager, resourceTag));
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, _helixClusterName, _helixAdmin, false);
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, _helixClusterName, _helixAdmin, true);
  }

  public void addSegment() {

  }

  public void updateSegment() {

  }

  public void deleteSegment() {

  }

  public void dropAllSegments() {

  }

  public void startInstances(List<String> instances) {
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, _helixClusterName, _helixAdmin, false);
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, _helixClusterName, _helixAdmin, true);
  }

  @Override
  public String toString() {
    return "yay! i am alive and kicking, clusterName is : " + _helixClusterName + " zk url is : " + _helixZkURL;
  }

}
