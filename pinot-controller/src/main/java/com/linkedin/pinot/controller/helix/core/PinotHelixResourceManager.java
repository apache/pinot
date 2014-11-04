package com.linkedin.pinot.controller.helix.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;


/**
 * @author Dhaval Patel<dpatel@linkedin.com
 * Sep 30, 2014
 */
public class PinotHelixResourceManager {
  private static Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);

  private String _zkBaseUrl;
  private String _helixClusterName;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _helixZkURL;
  private String _instanceId;
  private ZkClient _zkClient;
  private ZkHelixPropertyStore<ZNRecord> propertyStore;

  @SuppressWarnings("unused")
  private PinotHelixResourceManager() {

  }

  public PinotHelixResourceManager(String zkURL, String helixClusterName, String controllerInstanceId) {
    _zkBaseUrl = zkURL;
    _helixClusterName = helixClusterName;
    _instanceId = controllerInstanceId;
  }

  public synchronized void start() throws Exception {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkBaseUrl);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _zkClient =
        new ZkClient(StringUtil.join("/", StringUtils.chomp(_zkBaseUrl, "/"), _helixClusterName, "PROPERTYSTORE"),
            ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    propertyStore = new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkClient), "/", null);
  }

  public synchronized void stop() {
    _helixZkManager.disconnect();
  }

  public DataResource getDataResource(String resourceName) {
    final Map<String, String> configs = HelixHelper.getResourceConfigsFor(_helixClusterName, resourceName, _helixAdmin);
    return DataResource.fromResourceConfigMap(configs);
  }

  public List<String> getAllResourceNames() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  public synchronized PinotResourceManagerResponse handleCreateNewDataResource(DataResource resource) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {
      createNewDataResource(resource);
      handleBrokerResource(resource);
    } catch (final Exception e) {
      res.errorMessage = e.getMessage();
      res.status = STATUS.failure;
      e.printStackTrace();
      LOGGER.error(e.toString());
      revertDataResource(resource);
      throw new RuntimeException("Error creating cluster, have successfull rolled back", e);
    }
    res.status = STATUS.success;
    return res;

  }

  public void createNewDataResource(DataResource resource) {
    final List<String> unTaggedInstanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);

    final int numInstanceToUse = resource.getNumberOfDataInstances();
    LOGGER.info("Trying to allocate " + numInstanceToUse + " instances.");
    LOGGER.info("Current untagged boxes: " + unTaggedInstanceList.size());
    if (unTaggedInstanceList.size() < numInstanceToUse) {
      throw new UnsupportedOperationException("Cannot allocate enough hardware resource.");
    }
    for (int i = 0; i < numInstanceToUse; ++i) {
      LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + resource.getResourceName());
      _helixAdmin.removeInstanceTag(_helixClusterName, unTaggedInstanceList.get(i),
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), resource.getResourceName());
    }

    // now lets build an ideal state
    LOGGER.info("building empty ideal state for resource : " + resource.getResourceName());

    final IdealState idealState =
        PinotResourceIdealStateBuilder.buildEmptyIdealStateFor(resource, _helixAdmin, _helixClusterName);
    LOGGER.info("adding resource via the admin");
    _helixAdmin.addResource(_helixClusterName, resource.getResourceName(), idealState);
    LOGGER.info("successfully added the resource : " + resource.getResourceName() + " to the cluster");

    // lets add resource configs
    HelixHelper.updateResourceConfigsFor(resource.toResourceConfigMap(), resource.getResourceName(), _helixClusterName,
        _helixAdmin);
  }

  private void handleBrokerResource(DataResource resource) {
    final BrokerTagResource brokerTagResource =
        new BrokerTagResource(resource.getNumberOfBrokerInstances(), resource.getBrokerTagName());
    final BrokerDataResource brokerDataResource = new BrokerDataResource(resource.getResourceName(), brokerTagResource);

    PinotResourceManagerResponse createBrokerResourceResp = null;
    if (!HelixHelper.getBrokerTagList(_helixAdmin, _helixClusterName).contains(brokerTagResource.getTag())) {
      createBrokerResourceResp = createBrokerResourceTag(brokerTagResource);
    } else if (HelixHelper.getBrokerTag(_helixAdmin, _helixClusterName, brokerTagResource.getTag())
        .getNumBrokerInstances() < brokerTagResource.getNumBrokerInstances()) {
      createBrokerResourceResp = updateBrokerResourceTag(brokerTagResource);
    }
    if ((createBrokerResourceResp == null) || createBrokerResourceResp.isSuccessfull()) {
      createBrokerResourceResp = createBrokerDataResource(brokerDataResource);
      if (!createBrokerResourceResp.isSuccessfull()) {
        throw new UnsupportedOperationException("Failed to update broker resource : "
            + createBrokerResourceResp.errorMessage);
      }
    } else {
      throw new UnsupportedOperationException("Failed to create broker resource : "
          + createBrokerResourceResp.errorMessage);
    }
  }

  private void revertDataResource(DataResource resource) {
    final List<String> taggedInstanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getResourceName());
    for (final String instance : taggedInstanceList) {
      LOGGER.info("untag instance : " + instance.toString());
      _helixAdmin.removeInstanceTag(_helixClusterName, instance, resource.getResourceName());
      _helixAdmin.addInstanceTag(_helixClusterName, instance, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
    _helixAdmin.dropResource(_helixClusterName, resource.getResourceName());
  }

  public synchronized PinotResourceManagerResponse handleUpdateDataResource(DataResource resource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();

    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(resource.getResourceName())) {
      resp.status = STATUS.failure;
      resp.errorMessage = String.format("Resource (%s) does not exist", resource.getResourceName());
      return resp;
    }

    // Hardware assignment
    final List<String> unTaggedInstanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    final List<String> alreadyTaggedInstanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resource.getResourceName());

    if (alreadyTaggedInstanceList.size() > resource.getNumberOfDataInstances()) {
      resp.status = STATUS.failure;
      resp.errorMessage =
          String.format(
              "Reducing cluster size is not supported for now, current number instances for resource (%s) is "
                  + alreadyTaggedInstanceList.size(), resource.getResourceName());
      return resp;
    }

    final int numInstanceToUse = resource.getNumberOfDataInstances() - alreadyTaggedInstanceList.size();
    LOGGER.info("Already used boxes: " + alreadyTaggedInstanceList.size() + " instances.");
    LOGGER.info("Trying to allocate " + numInstanceToUse + " instances.");
    LOGGER.info("Current untagged boxes: " + unTaggedInstanceList.size());
    if (unTaggedInstanceList.size() < numInstanceToUse) {
      throw new UnsupportedOperationException("Cannot allocate enough hardware resource.");
    }
    for (int i = 0; i < numInstanceToUse; ++i) {
      LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + resource.getResourceName());
      _helixAdmin.removeInstanceTag(_helixClusterName, unTaggedInstanceList.get(i),
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), resource.getResourceName());
    }

    // now lets build an ideal state
    LOGGER.info("recompute ideal state for resource : " + resource.getResourceName());
    final IdealState idealState =
        PinotResourceIdealStateBuilder
            .updateExpandedDataResourceIdealStateFor(resource, _helixAdmin, _helixClusterName);
    LOGGER.info("update resource via the admin");
    _helixAdmin.setResourceIdealState(_helixClusterName, resource.getResourceName(), idealState);
    LOGGER.info("successfully update the resource : " + resource.getResourceName() + " to the cluster");
    HelixHelper.updateResourceConfigsFor(resource.toResourceConfigMap(), resource.getResourceName(), _helixClusterName,
        _helixAdmin);
    resp.status = STATUS.success;
    return resp;

  }

  public synchronized PinotResourceManagerResponse handleUpdateDataResourceConfig(DataResource resource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    HelixHelper.updateResourceConfigsFor(resource.toResourceConfigMap(), resource.getResourceName(), _helixClusterName,
        _helixAdmin);
    resp.status = STATUS.success;
    resp.errorMessage = String.format("Resource (%s) properties have been updated", resource.getResourceName());
    return resp;
  }

  public synchronized PinotResourceManagerResponse handleUpdateBrokerResource(DataResource resource) {
    String currentResourceBrokerTag =
        HelixHelper.getResourceConfigsFor(_helixClusterName, resource.getResourceName(), _helixAdmin).get(
            CommonConstants.Helix.DataSource.BROKER_TAG_NAME);
    ;
    if (!currentResourceBrokerTag.equals(resource.getBrokerTagName())) {
      final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
      resp.status = STATUS.failure;
      resp.errorMessage =
          "Current broker tag for resource : " + resource + " is " + currentResourceBrokerTag
              + ", not match updated request broker tag : " + resource.getBrokerTagName();
      return resp;
    }
    BrokerTagResource brokerTagResource =
        new BrokerTagResource(resource.getNumberOfBrokerInstances(), resource.getBrokerTagName());
    PinotResourceManagerResponse updateBrokerResourceTagResp = updateBrokerResourceTag(brokerTagResource);
    if (updateBrokerResourceTagResp.isSuccessfull()) {
      HelixHelper.updateResourceConfigsFor(resource.toResourceConfigMap(), resource.getResourceName(),
          _helixClusterName,
          _helixAdmin);
      return createBrokerDataResource(new BrokerDataResource(resource.getResourceName(), brokerTagResource));
    } else {
      return updateBrokerResourceTagResp;
    }
  }

  public synchronized PinotResourceManagerResponse deleteResource(String resourceTag) {
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
      _helixAdmin.addInstanceTag(_helixClusterName, instance, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }

    // dropping resource
    _helixAdmin.dropResource(_helixClusterName, resourceTag);

    res.status = STATUS.success;
    return res;
  }

  public synchronized void restartResource(String resourceTag) {
    final Set<String> allInstances =
        HelixHelper.getAllInstancesForResource(HelixHelper.getResourceIdealState(_helixZkManager, resourceTag));
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, _helixClusterName, _helixAdmin, false);
    HelixHelper.toggleInstancesWithInstanceNameSet(allInstances, _helixClusterName, _helixAdmin, true);
  }

  /*
   *  fetch list of segments assigned to a give resource from ideal state
   */
  public Set<String> getAllSegmentsForResource(String resource) {
    final IdealState state = HelixHelper.getResourceIdealState(_helixZkManager, resource);
    return state.getPartitionSet();
  }

  /**
   *
   * @param resource
   * @param segmentId
   * @return
   */
  public Map<String, String> getMetadataFor(String resource, String segmentId) {
    final ZNRecord record =
        propertyStore.get("/" + StringUtil.join("/", resource, segmentId), null, AccessOption.PERSISTENT);
    return record.getSimpleFields();
  }

  /**
   * For adding a new segment.
   * Helix will compute the instance to assign this segment.
   * Then update its ideal state.
   *
   * @param segmentMetadata
   */
  public synchronized PinotResourceManagerResponse addSegment(SegmentMetadata segmentMetadata, String downloadUrl) {

    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {

      final ZNRecord record = new ZNRecord(segmentMetadata.getName());

      record.setSimpleFields(segmentMetadata.toMap());
      record.setSimpleField(V1Constants.SEGMENT_DOWNLOAD_URL, downloadUrl);

      propertyStore.create("/" + segmentMetadata.getResourceName() + "/" + segmentMetadata.getName(), record,
          AccessOption.PERSISTENT);
      LOGGER.info("Added segment : " + segmentMetadata.getName() + " to Property store");

      final IdealState idealState =
          PinotResourceIdealStateBuilder.addNewSegmentToIdealStateFor(segmentMetadata, _helixAdmin, _helixClusterName);
      _helixAdmin.setResourceIdealState(_helixClusterName, segmentMetadata.getResourceName(), idealState);
      res.status = STATUS.success;
    } catch (final Exception e) {
      res.status = STATUS.failure;
      res.errorMessage = e.getMessage();
      e.printStackTrace();
    }
    return res;
  }

  public synchronized PinotResourceManagerResponse addInstance(Instance instance) {
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

  public PinotResourceManagerResponse updateSegment() {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    // TODO(xiafu) : disable and enable Segment, has to be refine in future.
    // Has to implement versioning here.
    return resp;
  }

  /**
   * TODO(xiafu) : Due to the issue of helix, if remove the segment from idealState directly, the node won't get transaction,
   * so we first disable the segment, then remove it from idealState, then enable the segment. This will trigger transactions.
   * After the helix patch, we can refine the logic here by directly drop the segment from idealState.
   *
   * @param resourceName
   * @param segmentId
   * @return
   */
  public synchronized PinotResourceManagerResponse deleteSegment(String resourceName, String segmentId) {

    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      final Set<String> instanceNames = idealState.getInstanceSet(segmentId);
      for (final String instanceName : instanceNames) {
        _helixAdmin.enablePartition(false, _helixClusterName, instanceName, resourceName, Arrays.asList(segmentId));
      }
      idealState =
          PinotResourceIdealStateBuilder.removeSegmentFromIdealStateFor(resourceName, segmentId, _helixAdmin,
              _helixClusterName);
      _helixAdmin.setResourceIdealState(_helixClusterName, resourceName, idealState);
      for (final String instanceName : instanceNames) {
        _helixAdmin.enablePartition(true, _helixClusterName, instanceName, resourceName, Arrays.asList(segmentId));
      }
      final String pathToPropertyStore = "/" + StringUtil.join("/", resourceName, segmentId);
      propertyStore.remove(pathToPropertyStore, AccessOption.PERSISTENT);
      LOGGER.info("Delete segment : " + segmentId + " from Property store.");
      res.status = STATUS.success;
    } catch (final Exception e) {
      res.status = STATUS.failure;
      res.errorMessage = e.getMessage();
    }
    return res;
  }

  // **** Start Broker level operations ****
  /**
   * Assign untagged broker instances to a given tag.
   * Broker tag is required to have prefix : "broker_".
   *
   * @param brokerTagResource
   * @return
   */
  public synchronized PinotResourceManagerResponse createBrokerResourceTag(BrokerTagResource brokerTagResource) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    if (!brokerTagResource.getTag().startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_RESOURCE_TAG)) {
      res.status = STATUS.failure;
      res.errorMessage =
          "Broker tag is required to have prefix : " + CommonConstants.Helix.PREFIX_OF_BROKER_RESOURCE_TAG;
      LOGGER.error(res.errorMessage);
      return res;
    }
    // @xiafu : should this be contains or equals to
    if (HelixHelper.getBrokerTagList(_helixAdmin, _helixClusterName).contains(brokerTagResource.getTag())) {
      return updateBrokerResourceTag(brokerTagResource);
    }
    final List<String> untaggedBrokerInstances =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    if (untaggedBrokerInstances.size() < brokerTagResource.getNumBrokerInstances()) {
      res.status = STATUS.failure;
      res.errorMessage =
          "Failed to create tag : " + brokerTagResource.getTag() + ", Current number of untagged broker instances : "
              + untaggedBrokerInstances.size() + ", required : " + brokerTagResource.getNumBrokerInstances();
      LOGGER.error(res.errorMessage);
      return res;
    }
    for (int i = 0; i < brokerTagResource.getNumBrokerInstances(); ++i) {
      _helixAdmin.removeInstanceTag(_helixClusterName, untaggedBrokerInstances.get(i),
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, untaggedBrokerInstances.get(i), brokerTagResource.getTag());
    }
    HelixHelper.updateBrokerTag(_helixAdmin, _helixClusterName, brokerTagResource);
    res.status = STATUS.success;
    return res;
  }

  /**
   * This will take care of update a brokerResource tag.
   * Adding a new untagged broker instance.
   *
   * @param brokerTagResource
   * @return
   */
  private synchronized PinotResourceManagerResponse updateBrokerResourceTag(BrokerTagResource brokerTagResource) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    final BrokerTagResource currentBrokerTag =
        HelixHelper.getBrokerTag(_helixAdmin, _helixClusterName, brokerTagResource.getTag());
    if (currentBrokerTag.getNumBrokerInstances() < brokerTagResource.getNumBrokerInstances()) {
      // Add more broker instances to this tag
      final int numBrokerInstancesToTag =
          brokerTagResource.getNumBrokerInstances() - currentBrokerTag.getNumBrokerInstances();
      final List<String> untaggedBrokerInstances =
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      if (untaggedBrokerInstances.size() < numBrokerInstancesToTag) {
        res.status = STATUS.failure;
        res.errorMessage =
            "Failed to allocate broker instances to Tag : " + brokerTagResource.getTag()
                + ", Current number of untagged broker instances : " + untaggedBrokerInstances.size()
                + ", current number of tagged instances : " + currentBrokerTag.getNumBrokerInstances()
                + ", updated number of tagged instances : " + brokerTagResource.getNumBrokerInstances();
        LOGGER.error(res.errorMessage);
        return res;
      }
      for (int i = 0; i < numBrokerInstancesToTag; ++i) {
        _helixAdmin.removeInstanceTag(_helixClusterName, untaggedBrokerInstances.get(i),
            CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
        _helixAdmin.addInstanceTag(_helixClusterName, untaggedBrokerInstances.get(i), brokerTagResource.getTag());
      }
    } else {
      // Remove broker instances from this tag
      // TODO(xiafu) : There is rebalancing work and dataResource config changes around the removing.
      final int numBrokerInstancesToRemove =
          currentBrokerTag.getNumBrokerInstances() - brokerTagResource.getNumBrokerInstances();
      final List<String> taggedBrokerInstances =
          _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTagResource.getTag());
      unTagBrokerInstance(taggedBrokerInstances.subList(0, numBrokerInstancesToRemove), brokerTagResource.getTag());
    }
    HelixHelper.updateBrokerTag(_helixAdmin, _helixClusterName, brokerTagResource);
    res.status = STATUS.success;
    return res;
  }

  public synchronized PinotResourceManagerResponse deleteBrokerResourceTag(String brokerTag) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    if (!HelixHelper.getBrokerTagList(_helixAdmin, _helixClusterName).contains(brokerTag)) {
      res.status = STATUS.failure;
      res.errorMessage = "Broker resource tag is not existed!";
      return res;
    }
    // Remove broker instances from this tag
    final List<String> taggedBrokerInstances = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTag);
    unTagBrokerInstance(taggedBrokerInstances, brokerTag);
    HelixHelper.deleteBrokerTagFromResourceConfig(_helixAdmin, _helixClusterName, brokerTag);
    res.status = STATUS.success;
    return res;
  }

  private void unTagBrokerInstance(List<String> brokerInstancesToUntag, String brokerTag) {
    final IdealState brokerIdealState = HelixHelper.getBrokerIdealStates(_helixAdmin, _helixClusterName);
    final List<String> dataResourceList = new ArrayList(brokerIdealState.getPartitionSet());
    for (final String dataResource : dataResourceList) {
      final Set<String> instances = brokerIdealState.getInstanceSet(dataResource);
      brokerIdealState.getPartitionSet().remove(dataResource);
      for (final String instance : instances) {
        if (!brokerInstancesToUntag.contains(instance)) {
          brokerIdealState.setPartitionState(dataResource, instance, "ONLINE");
        }
      }

    }
    _helixAdmin.setResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE,
        brokerIdealState);
    for (final String brokerInstanceToUntag : brokerInstancesToUntag) {
      _helixAdmin.removeInstanceTag(_helixClusterName, brokerInstanceToUntag, brokerTag);
      _helixAdmin.addInstanceTag(_helixClusterName, brokerInstanceToUntag,
          CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }
  }

  public synchronized PinotResourceManagerResponse createBrokerDataResource(BrokerDataResource brokerDataResource) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {
      LOGGER.info("Trying to update BrokerDataResource with config: \n" + brokerDataResource.toString());
      if (_helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerDataResource.getTag()).isEmpty()) {
        res.status = STATUS.failure;
        res.errorMessage = "broker resource tag : " + brokerDataResource.getTag() + " is not existed!";
        LOGGER.error(res.toString());
        return res;
      }

      LOGGER.info("Trying to update BrokerDataResource Config!");
      HelixHelper.updateBrokerDataResource(_helixAdmin, _helixClusterName, brokerDataResource);

      LOGGER.info("Trying to update BrokerDataResource IdealState!");
      final IdealState idealState =
          PinotResourceIdealStateBuilder.addBrokerResourceToIdealStateFor(brokerDataResource, _helixAdmin,
              _helixClusterName);
      if (idealState != null) {
        _helixAdmin
            .setResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, idealState);
      }
      res.status = STATUS.success;
    } catch (final Exception e) {
      res.status = STATUS.failure;
      res.errorMessage = e.getMessage();
      LOGGER.error(res.toString());
    }
    return res;
  }

  public synchronized PinotResourceManagerResponse deleteBrokerDataResource(String brokerDataResourceName) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {
      IdealState idealState =
          _helixAdmin.getResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      final Set<String> instanceNames = idealState.getInstanceSet(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
      for (final String instanceName : instanceNames) {
        _helixAdmin.enablePartition(false, _helixClusterName, instanceName,
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, Arrays.asList(brokerDataResourceName));
      }
      idealState =
          PinotResourceIdealStateBuilder.removeBrokerResourceFromIdealStateFor(brokerDataResourceName, _helixAdmin,
              _helixClusterName);
      _helixAdmin.setResourceIdealState(_helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, idealState);
      for (final String instanceName : instanceNames) {
        _helixAdmin.enablePartition(true, _helixClusterName, instanceName,
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, Arrays.asList(brokerDataResourceName));
      }
      HelixHelper.deleteBrokerDataResourceConfig(_helixAdmin, _helixClusterName, brokerDataResourceName);
      res.status = STATUS.success;
    } catch (final Exception e) {
      res.status = STATUS.failure;
      res.errorMessage = e.getMessage();
    }
    return res;
  }

  // **** End Broker level operations ****
  public void startInstances(List<String> instances) {
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, _helixClusterName, _helixAdmin, false);
    HelixHelper.toggleInstancesWithPinotInstanceList(instances, _helixClusterName, _helixAdmin, true);
  }

  @Override
  public String toString() {
    return "yay! i am alive and kicking, clusterName is : " + _helixClusterName + " zk url is : " + _zkBaseUrl;
  }
}
