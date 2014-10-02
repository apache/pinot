package com.linkedin.pinot.controller.helix.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.api.pojos.Resource;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * @author Dhaval Patel<dpatel@linkedin.com
 * Sep 30, 2014
 */
public class PinotHelixResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);

  public static String UNTAGGED = "untagged";

  private String zkBaseUrl;
  private String helixClusterName;
  private HelixManager helixZkManager;
  private HelixAdmin helixAdmin;
  private String instanceId;

  @SuppressWarnings("unused")
  private PinotHelixResourceManager() {

  }

  public PinotHelixResourceManager(String zkURL, String helixClusterName, String controllerInstanceId) {
    zkBaseUrl = zkURL;
    this.helixClusterName = helixClusterName;
    instanceId = controllerInstanceId;
  }

  public void start() throws Exception {
    helixZkManager = HelixSetupUtils.setup(helixClusterName, zkBaseUrl, instanceId);
    helixAdmin = helixZkManager.getClusterManagmentTool();
  }

  public void stop() {
    helixZkManager.disconnect();
  }


  public Resource getDataResource(String resourceName) {
    final Map<String, String> configs = HelixHelper.getResourceConfigsFor(helixClusterName, resourceName, helixAdmin);
    return Resource.fromMap(configs);
  }

  public synchronized void createDataResource(Resource resource) {
    try {

      // lets add instances now with their configs
      final List<String> unTaggedInstanceList = helixAdmin.getInstancesInClusterWithTag(helixClusterName, UNTAGGED);

      final int numInstanceToUse = resource.getNumReplicas() * resource.getNumInstances();
      LOGGER.info("Trying to allocate " + numInstanceToUse + " instances.");
      if (unTaggedInstanceList.size() < numInstanceToUse) {
        throw new UnsupportedOperationException("Cannot allocate enough hardware resource.");
      }
      for (int i = 0; i < numInstanceToUse; ++i) {
        LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + resource.getResourceName());
        helixAdmin.removeInstanceTag(helixClusterName, unTaggedInstanceList.get(i), UNTAGGED);
        helixAdmin.addInstanceTag(helixClusterName, unTaggedInstanceList.get(i), resource.getResourceName());
      }

      // now lets build an ideal state
      LOGGER.info("building empty ideal state for resource : " + resource.getResourceName());
      final IdealState idealState = PinotResourceIdealStateBuilder.buildEmptyIdealStateFor(resource, helixAdmin, helixClusterName);
      helixAdmin.addResource(helixClusterName, resource.getResourceName(), idealState);
      LOGGER.info("successfully added the resource : " + resource.getResourceName() + " to the cluster");

      // lets add resource configs
      HelixHelper.updateResourceConfigsFor(resource.toMap(), resource.getResourceName(), helixClusterName, helixAdmin);

    } catch (final Exception e) {
      e.printStackTrace();
      LOGGER.error(e.toString());
      // dropping all instances
      final List<String> taggedInstanceList = helixAdmin.getInstancesInClusterWithTag(helixClusterName, resource.getResourceName());
      for (final String instance : taggedInstanceList) {
        LOGGER.info("untag instance : " + instance.toString());
        helixAdmin.removeInstanceTag(helixClusterName, instance, resource.getResourceName());
        helixAdmin.addInstanceTag(helixClusterName, instance, UNTAGGED);
      }
      helixAdmin.dropResource(helixClusterName, resource.getResourceName());
      throw new RuntimeException("Error creating cluster, have successfull rolled back", e);
    }
  }

  //  public boolean updateResource(UpdateResourceConfigUpdateRequest updateResourceConfigRequest) {
  //    final String resourceTag = updateResourceConfigRequest.getResourceName();
  //    final Map<String, String> propsToUpdate = updateResourceConfigRequest.getPropertiesMapToUpdate();
  //    HelixHelper.updateResourceConfigsFor(propsToUpdate, resourceTag, _helixClusterName, _helixAdmin);
  //    final Set<String> propsKeyToDelete = updateResourceConfigRequest.getPropertiesKeySetToDelete();
  //    for (final String configKey : propsKeyToDelete) {
  //      HelixHelper.deleteResourcePropertyFromHelix(_helixAdmin, _helixClusterName, resourceTag, configKey);
  //    }
  //    if (updateResourceConfigRequest.isBounceService()) {
  //      restartResource(resourceTag);
  //    }
  //    return true;
  //  }

  public void deleteResource(String resourceTag) {
    final List<String> taggedInstanceList = helixAdmin.getInstancesInClusterWithTag(helixClusterName, resourceTag);
    for (final String instance : taggedInstanceList) {
      LOGGER.info("untag instance : " + instance.toString());
      helixAdmin.removeInstanceTag(helixClusterName, instance, resourceTag);
      helixAdmin.addInstanceTag(helixClusterName, instance, UNTAGGED);
    }
    helixAdmin.dropResource(helixClusterName, resourceTag);
  }

  public void restartResource(String resourceTag) {
    final Set<String> allInstances = HelixHelper.getAllInstancesForResource(HelixHelper.getResourceIdealState(helixZkManager, resourceTag));
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, helixClusterName, helixAdmin, false);
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, helixClusterName, helixAdmin, true);
  }

  public void addSegment(IndexSegment indexSegment) {
    final IdealState idealState = PinotResourceIdealStateBuilder.addNewSegmentToIdealStateFor(indexSegment, helixAdmin, helixClusterName);
    helixAdmin.setResourceIdealState(helixClusterName, indexSegment.getSegmentMetadata().getResourceName(), idealState);
  }

  public PinotResourceManagerResponse addInstance(Instance instance) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    final List<String> instances = HelixHelper.getAllInstances(helixAdmin, helixClusterName);
    if (instances.contains(instance.toInstanceId())) {
      resp.status = STATUS.failure;
      resp.errorMessage = "instance already exist";
      return resp;
    } else {
      helixAdmin.addInstance(helixClusterName, instance.toInstanceConfig());
      resp.status = STATUS.success;
      resp.errorMessage = "";
      return resp;
    }
  }

  public void updateSegment() {

  }

  public void deleteSegment() {

  }

  public void dropAllSegments() {

  }

  public void startInstances(List<String> instances) {
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, helixClusterName, helixAdmin, false);
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, helixClusterName, helixAdmin, true);
  }

  @Override
  public String toString() {
    return "yay! i am alive and kicking, clusterName is : " + helixClusterName + " zk url is : " + zkBaseUrl;
  }

}
