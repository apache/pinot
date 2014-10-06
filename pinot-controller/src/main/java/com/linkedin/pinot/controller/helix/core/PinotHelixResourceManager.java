package com.linkedin.pinot.controller.helix.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;


/**
 * @author Dhaval Patel<dpatel@linkedin.com
 * Sep 30, 2014
 */
public class PinotHelixResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);

  public static String UNTAGGED = "untagged";

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
    _zkBaseUrl = zkURL;
    _helixClusterName = helixClusterName;
    _instanceId = controllerInstanceId;
  }

  public void start() throws Exception {
    final ZkClient zkClient = new ZkClient(_zkBaseUrl);
    if (!zkClient.exists("/" + HelixConfig.HELIX_ZK_PATH_PREFIX)) {
      zkClient.createPersistent("/" + HelixConfig.HELIX_ZK_PATH_PREFIX, true);
    }
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkBaseUrl);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
  }

  public void stop() {
    _helixZkManager.disconnect();
  }

  public DataResource getDataResource(String resourceName) {
    final Map<String, String> configs = HelixHelper.getResourceConfigsFor(_helixClusterName, resourceName, _helixAdmin);
    return DataResource.fromMap(configs);
  }

  public synchronized PinotResourceManagerResponse createDataResource(DataResource resource) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {

      // lets add instances now with their configs
      final List<String> unTaggedInstanceList = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, UNTAGGED);

      final int numInstanceToUse = resource.getNumReplicas() * resource.getNumInstancesPerReplica();
      LOGGER.info("Trying to allocate " + numInstanceToUse + " instances.");
      System.out.println("Current untagged boxes: " + unTaggedInstanceList.size());
      if (unTaggedInstanceList.size() < numInstanceToUse) {
        throw new UnsupportedOperationException("Cannot allocate enough hardware resource.");
      }
      for (int i = 0; i < numInstanceToUse; ++i) {
        LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + resource.getResourceName());
        _helixAdmin.removeInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), UNTAGGED);
        _helixAdmin.addInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), resource.getResourceName());
      }

      // now lets build an ideal state
      LOGGER.info("building empty ideal state for resource : " + resource.getResourceName());

      final IdealState idealState = PinotResourceIdealStateBuilder.buildEmptyIdealStateFor(resource, _helixAdmin, _helixClusterName);
      LOGGER.info("adding resource via the admin");
      _helixAdmin.addResource(_helixClusterName, resource.getResourceName(), idealState);
      LOGGER.info("successfully added the resource : " + resource.getResourceName() + " to the cluster");

      // lets add resource configs
      HelixHelper.updateResourceConfigsFor(resource.toMap(), resource.getResourceName(), _helixClusterName, _helixAdmin);
      res.status = STATUS.success;

    } catch (final Exception e) {
      res.errorMessage = e.getMessage();
      res.status = STATUS.failure;
      e.printStackTrace();
      LOGGER.error(e.toString());
      // dropping all instances
      final List<String> taggedInstanceList = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getResourceName());
      for (final String instance : taggedInstanceList) {
        LOGGER.info("untag instance : " + instance.toString());
        _helixAdmin.removeInstanceTag(_helixClusterName, instance, resource.getResourceName());
        _helixAdmin.addInstanceTag(_helixClusterName, instance, UNTAGGED);
      }
      _helixAdmin.dropResource(_helixClusterName, resource.getResourceName());
      throw new RuntimeException("Error creating cluster, have successfull rolled back", e);
    }
    return res;
  }

  public PinotResourceManagerResponse updateResource(DataResource incomingResource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();

    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(incomingResource.getResourceName())) {
      resp.status = STATUS.failure;
      resp.errorMessage = String.format("Resource (%s) does not exist", incomingResource.getResourceName());
      return resp;
    }

    final Map<String, String> configs =
        HelixHelper.getResourceConfigsFor(_helixClusterName, incomingResource.getResourceName(), _helixAdmin);
    final DataResource existingResource = DataResource.fromMap(configs);

    if (incomingResource.equals(existingResource)) {
      resp.status = STATUS.failure;
      resp.errorMessage =
          String.format("Resource (%s) already has all the properties that are expected to be there", incomingResource.getResourceName());
      return resp;
    }

    if (incomingResource.instancEequals(existingResource)) {
      HelixHelper.updateResourceConfigsFor(incomingResource.toMap(), incomingResource.getResourceName(), _helixClusterName, _helixAdmin);
      resp.status = STATUS.success;
      resp.errorMessage =
          String.format("Resource (%s) properties have been updated", incomingResource.getResourceName());
      return resp;
    }

    return resp;
  }

  public PinotResourceManagerResponse deleteResource(String resourceTag) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();

    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(resourceTag)) {
      res.status = STATUS.failure;
      res.errorMessage = String.format("Resource (%s) does not exist", resourceTag);
      return res;
    }

    final List<String> taggedInstanceList = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resourceTag);
    for (final String instance : taggedInstanceList) {
      LOGGER.info("untagging instance : " + instance.toString());
      _helixAdmin.removeInstanceTag(_helixClusterName, instance, resourceTag);
      _helixAdmin.addInstanceTag(_helixClusterName, instance, UNTAGGED);
    }

    // dropping resource
    _helixAdmin.dropResource(_helixClusterName, resourceTag);

    res.status = STATUS.success;
    return res;
  }

  public void restartResource(String resourceTag) {
    final Set<String> allInstances =
        HelixHelper.getAllInstancesForResource(HelixHelper.getResourceIdealState(_helixZkManager, resourceTag));
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, _helixClusterName, _helixAdmin, false);
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, _helixClusterName, _helixAdmin, true);
  }

  /**
   * For adding a new segment.
   * Helix will compute the instance to assign this segment.
   * Then update its ideal state.
   *
   * @param segmentMetadata
   */
  public void addSegment(SegmentMetadata segmentMetadata) {
    final IdealState idealState =
        PinotResourceIdealStateBuilder.addNewSegmentToIdealStateFor(segmentMetadata, _helixAdmin, _helixClusterName);
    _helixAdmin.setResourceIdealState(_helixClusterName, segmentMetadata.getResourceName(), idealState);
  }

  public PinotResourceManagerResponse addInstance(Instance instance) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    final List<String> instances = HelixHelper.getAllInstances(_helixAdmin, _helixClusterName);
    if (instances.contains(instance.toInstanceId())) {
      resp.status = STATUS.failure;
      resp.errorMessage = "instance already exist";
      return resp;
    } else {
      _helixAdmin.addInstance(_helixClusterName, instance.toInstanceConfig());
      resp.status = STATUS.success;
      resp.errorMessage = "";
      return resp;
    }
  }

  public void updateSegment() {

  }

  public void deleteSegment(String resourceName, String segmentId) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
    final Set<String> instanceNames = idealState.getInstanceSet(segmentId);
    for (final String instanceName : instanceNames) {
      _helixAdmin.enablePartition(false, _helixClusterName, instanceName, resourceName, Arrays.asList(segmentId));
    }
    idealState = PinotResourceIdealStateBuilder.removeSegmentFromIdealStateFor(resourceName, segmentId, _helixAdmin, _helixClusterName);
    _helixAdmin.setResourceIdealState(_helixClusterName, resourceName, idealState);
    for (final String instanceName : instanceNames) {
      _helixAdmin.enablePartition(true, _helixClusterName, instanceName, resourceName, Arrays.asList(segmentId));
    }

  }

  public void dropAllSegments() {

  }

  public void startInstances(List<String> instances) {
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, _helixClusterName, _helixAdmin, false);
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, _helixClusterName, _helixAdmin, true);
  }

  @Override
  public String toString() {
    return "yay! i am alive and kicking, clusterName is : " + _helixClusterName + " zk url is : " + _zkBaseUrl;
  }
}
