package com.linkedin.pinot.controller.helix.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.helix.model.ExternalView;
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
import com.linkedin.pinot.controller.helix.core.utils.PinotHelixUtils;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


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
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private String _localDiskDir;
  private SegmentDeletionManager _segmentDeletionManager = null;
  private long _externalViewReflectTimeOut = 10000; // 10 seconds

  @SuppressWarnings("unused")
  private PinotHelixResourceManager() {

  }

  public PinotHelixResourceManager(String zkURL, String helixClusterName, String controllerInstanceId, String localDiskDir) {
    _zkBaseUrl = zkURL;
    _helixClusterName = helixClusterName;
    _instanceId = controllerInstanceId;
    _localDiskDir = localDiskDir;
  }

  public synchronized void start() throws Exception {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkBaseUrl);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _zkClient =
        new ZkClient(StringUtil.join("/", StringUtils.chomp(_zkBaseUrl, "/"), _helixClusterName, "PROPERTYSTORE"),
            ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    _propertyStore = new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(_zkClient), "/", null);
    _segmentDeletionManager = new SegmentDeletionManager(_localDiskDir, _helixAdmin, _helixClusterName, _propertyStore);
  }

  public synchronized void stop() {
    _helixZkManager.disconnect();
  }

  public String getBrokerInstanceFor(String resourceName) {
    final DataResource ds = getDataResource(resourceName);
    final List<String> instanceIds = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, ds.getBrokerTagName());
    if (instanceIds == null || instanceIds.size() == 0) {
      return null;
    }
    Collections.shuffle(instanceIds);
    return instanceIds.get(0);
  }

  public DataResource getDataResource(String resourceName) {
    final Map<String, String> configs = HelixHelper.getResourceConfigsFor(_helixClusterName, resourceName, _helixAdmin);
    return DataResource.fromResourceConfigMap(configs);
  }

  public List<String> getAllResourceNames() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  /**
   * Returns all resources that are actual Pinot resources, not broker resource.
   */
  public List<String> getAllPinotResourceNames() {
    List<String> resourceNames = getAllResourceNames();

    // Filter resource names that are known to be non Pinot resources (ie. brokerResource)
    ArrayList<String> pinotResourceNames = new ArrayList<String>();
    for (String resourceName : resourceNames) {
      if (CommonConstants.Helix.NON_PINOT_RESOURCE_RESOURCE_NAMES.contains(resourceName))
        continue;
      pinotResourceNames.add(resourceName);
    }

    return pinotResourceNames;
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
    final List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();

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

  private List<String> getOnlineUnTaggedServerInstanceList() {
    final List<String> instanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    final List<String> liveInstances = HelixHelper.getLiveInstances(_helixClusterName, _helixZkManager);
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  private List<String> getOnlineUnTaggedBrokerInstanceList() {
    final List<String> instanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    final List<String> liveInstances = HelixHelper.getLiveInstances(_helixClusterName, _helixZkManager);
    instanceList.retainAll(liveInstances);
    return instanceList;
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

  public synchronized PinotResourceManagerResponse handleAddTableToDataResource(DataResource resource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    final Set<String> tableNames = getAllTableNamesForResource(resource.getResourceName());
    if (tableNames.contains(resource.getTableName())) {
      resp.status = STATUS.failure;
      resp.errorMessage =
          String.format("Table name (%s) is already existed in resource (%s)", resource.getTableName(),
              resource.getResourceName());
      return resp;
    }
    tableNames.add(resource.getTableName());
    final Map<String, String> tableConfig = new HashMap<String, String>();
    tableConfig.put("tableName", StringUtils.join(tableNames, ","));
    HelixHelper.updateResourceConfigsFor(tableConfig, resource.getResourceName(), _helixClusterName, _helixAdmin);
    resp.status = STATUS.success;
    resp.errorMessage =
        String.format("Adding table name (%s) to resource (%s)", resource.getTableName(), resource.getResourceName());
    return resp;
  }

  public synchronized PinotResourceManagerResponse handleRemoveTableFromDataResource(DataResource resource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    final Set<String> tableNames = getAllTableNamesForResource(resource.getResourceName());
    if (!tableNames.contains(resource.getTableName())) {
      resp.status = STATUS.failure;
      resp.errorMessage =
          String.format("Table name (%s) is not existed in resource (%s)", resource.getTableName(),
              resource.getResourceName());
      return resp;
    }
    tableNames.remove(resource.getTableName());
    final Map<String, String> tableConfig = new HashMap<String, String>();
    tableConfig.put("tableName", StringUtils.join(tableNames, ","));
    HelixHelper.updateResourceConfigsFor(tableConfig, resource.getResourceName(), _helixClusterName, _helixAdmin);
    deleteSegmentsInTable(resource.getResourceName(), resource.getTableName());
    resp.status = STATUS.success;
    resp.errorMessage =
        String.format("Removing table name (%s) from resource (%s)", resource.getTableName(),
            resource.getResourceName());
    return resp;
  }

  private void deleteSegmentsInTable(String resourceName, String tableName) {
    for (String segmentId : getAllSegmentsForTable(resourceName, tableName)) {
      deleteSegment(resourceName, segmentId);
    }
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
    final String currentResourceBrokerTag =
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
    final BrokerTagResource brokerTagResource =
        new BrokerTagResource(resource.getNumberOfBrokerInstances(), resource.getBrokerTagName());
    final PinotResourceManagerResponse updateBrokerResourceTagResp = updateBrokerResourceTag(brokerTagResource);
    if (updateBrokerResourceTagResp.isSuccessfull()) {
      HelixHelper.updateResourceConfigsFor(resource.toResourceConfigMap(), resource.getResourceName(),
          _helixClusterName, _helixAdmin);
      return createBrokerDataResource(new BrokerDataResource(resource.getResourceName(), brokerTagResource));
    } else {
      return updateBrokerResourceTagResp;
    }
  }

  public synchronized PinotResourceManagerResponse deleteResource(String resourceTag) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();

    // Remove broker tags
    final String brokerResourceTag = "broker_" + resourceTag;
    final List<String> taggedBrokerInstanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerResourceTag);
    for (final String instance : taggedBrokerInstanceList) {
      LOGGER.info("untagging broker instance : " + instance.toString());
      _helixAdmin.removeInstanceTag(_helixClusterName, instance, brokerResourceTag);
      _helixAdmin.addInstanceTag(_helixClusterName, instance, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    }

    // Update brokerResource idealStates
    HelixHelper.deleteResourceFromBrokerResource(_helixAdmin, _helixClusterName, resourceTag);
    HelixHelper.deleteBrokerDataResourceConfig(_helixAdmin, _helixClusterName, resourceTag);

    // Delete data resource
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

    // remove from property store
    _propertyStore.remove("/" + resourceTag, 0);

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

  public List<String> getAllSegmentsForTable(String resourceName, String tableName) {
    List<ZNRecord> records = _propertyStore.getChildren(PinotHelixUtils.constructPropertyStorePathForResource(resourceName), null, AccessOption.PERSISTENT);
    List<String> segmentsInTable = new ArrayList<String>();
    for (ZNRecord record : records) {
      if (record.getSimpleField(V1Constants.MetadataKeys.Segment.TABLE_NAME).equals(tableName)) {
        segmentsInTable.add(record.getId());
      }
    }
    return segmentsInTable;
  }

  /**
   *
   * @param resource
   * @param segmentId
   * @return
   */
  public Map<String, String> getMetadataFor(String resource, String segmentId) {
    final ZNRecord record =
        _propertyStore.get(PinotHelixUtils.constructPropertyStorePathForSegment(resource, segmentId), null, AccessOption.PERSISTENT);
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
      if (!matchResourceAndTableName(segmentMetadata)) {
        res.status = STATUS.failure;
        res.errorMessage =
            "Reject segment: resource name and table name are not registered." + " Resource name: "
                + segmentMetadata.getResourceName() + ", Table name: " + segmentMetadata.getTableName() + "\n";
        return res;
      }
      if (ifSegmentExisted(segmentMetadata)) {
        if (ifRefreshAnExistedSegment(segmentMetadata)) {
          final ZNRecord record = new ZNRecord(segmentMetadata.getName());
          record.setSimpleFields(segmentMetadata.toMap());
          record.setSimpleField(V1Constants.SEGMENT_DOWNLOAD_URL, downloadUrl);
          record.setSimpleField(V1Constants.SEGMENT_REFRESH_TIME, String.valueOf(System.currentTimeMillis()));

          _propertyStore.set(PinotHelixUtils.constructPropertyStorePathForSegment(segmentMetadata), record, AccessOption.PERSISTENT);
          LOGGER.info("Refresh segment : " + segmentMetadata.getName() + " to Property store");
          if (updateExistedSegment(segmentMetadata)) {
            res.status = STATUS.success;
          }
        }
      } else {
        final ZNRecord record = new ZNRecord(segmentMetadata.getName());
        record.setSimpleFields(segmentMetadata.toMap());
        record.setSimpleField(V1Constants.SEGMENT_DOWNLOAD_URL, downloadUrl);
        record.setSimpleField(V1Constants.SEGMENT_PUSH_TIME, String.valueOf(System.currentTimeMillis()));

        _propertyStore.create(PinotHelixUtils.constructPropertyStorePathForSegment(segmentMetadata), record, AccessOption.PERSISTENT);
        LOGGER.info("Added segment : " + segmentMetadata.getName() + " to Property store");

        final IdealState idealState =
            PinotResourceIdealStateBuilder
                .addNewSegmentToIdealStateFor(segmentMetadata, _helixAdmin, _helixClusterName);
        _helixAdmin.setResourceIdealState(_helixClusterName, segmentMetadata.getResourceName(), idealState);
        res.status = STATUS.success;
      }
    } catch (final Exception e) {
      res.status = STATUS.failure;
      res.errorMessage = e.getMessage();
      e.printStackTrace();
    }
    return res;
  }

  private boolean updateExistedSegment(SegmentMetadata segmentMetadata) {
    final String resourceName = segmentMetadata.getResourceName();
    final String segmentName = segmentMetadata.getName();

    final IdealState currentIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
    final Set<String> currentInstanceSet = currentIdealState.getInstanceSet(segmentName);
    for (final String instance : currentInstanceSet) {
      currentIdealState.setPartitionState(segmentName, instance, "OFFLINE");
    }
    _helixAdmin.setResourceIdealState(_helixClusterName, resourceName, currentIdealState);
    // wait until reflect in ExternalView
    LOGGER.info("Wait until segment - " + segmentName + " to be OFFLINE in ExternalView");
    if (!ifExternalViewChangeReflectedForState(resourceName, segmentName, "OFFLINE", _externalViewReflectTimeOut)) {
      throw new RuntimeException("Cannot get OFFLINE state to be reflected on ExternalView changed for segment: " + segmentName);
    }
    for (final String instance : currentInstanceSet) {
      currentIdealState.setPartitionState(segmentName, instance, "ONLINE");
    }
    _helixAdmin.setResourceIdealState(_helixClusterName, resourceName, currentIdealState);
    // wait until reflect in ExternalView
    LOGGER.info("Wait until segment - " + segmentName + " to be ONLINE in ExternalView");
    if (!ifExternalViewChangeReflectedForState(resourceName, segmentName, "ONLINE", _externalViewReflectTimeOut)) {
      throw new RuntimeException("Cannot get ONLINE state to be reflected on ExternalView changed for segment: " + segmentName);
    }
    LOGGER.info("Refresh is done for segment - " + segmentName);
    return true;
  }

  private boolean ifExternalViewChangeReflectedForState(String resourceName, String segmentName, String targerStates, long timeOutInMills) {
    long timeOutTimeStamp = System.currentTimeMillis() + timeOutInMills;
    boolean isSucess = true;
    while (System.currentTimeMillis() < timeOutTimeStamp) {
      // Will try to read data every 2 seconds.
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
      }
      isSucess = true;
      ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, resourceName);
      Map<String, String> segmentStatsMap = externalView.getStateMap(segmentName);
      for (String instance : segmentStatsMap.keySet()) {
        if (!segmentStatsMap.get(instance).equalsIgnoreCase(targerStates)) {
          isSucess = false;
        }
      }
      if (isSucess) {
        break;
      }
    }
    return isSucess;
  }

  private boolean ifSegmentExisted(SegmentMetadata segmentMetadata) {
    if (segmentMetadata == null) {
      return false;
    }
    return _propertyStore.exists(PinotHelixUtils.constructPropertyStorePathForSegment(segmentMetadata), AccessOption.PERSISTENT);
  }

  private boolean ifRefreshAnExistedSegment(SegmentMetadata segmentMetadata) {
    final ZNRecord record =
        _propertyStore.get(PinotHelixUtils.constructPropertyStorePathForSegment(segmentMetadata), null, AccessOption.PERSISTENT);
    if (record == null) {
      return false;
    }
    final SegmentMetadata existedSegmentMetadata = new SegmentMetadataImpl(record);
    if (segmentMetadata.getIndexCreationTime() <= existedSegmentMetadata.getIndexCreationTime()) {
      return false;
    }
    if (segmentMetadata.getCrc().equals(existedSegmentMetadata.getCrc())) {
      return false;
    }
    return true;
  }

  private boolean matchResourceAndTableName(SegmentMetadata segmentMetadata) {
    if (segmentMetadata == null || segmentMetadata.getResourceName() == null || segmentMetadata.getTableName() == null) {
      LOGGER.error("SegmentMetadata or resource name or table name is null");
      return false;
    }
    if (!getAllResourceNames().contains(segmentMetadata.getResourceName())) {
      LOGGER.error("Resource is not registered");
      return false;
    }
    if (!getAllTableNamesForResource(segmentMetadata.getResourceName()).contains(segmentMetadata.getTableName())) {
      LOGGER.error("Table " + segmentMetadata.getTableName() + " is not registered for resource: "
          + segmentMetadata.getResourceName());
      return false;
    }
    return true;
  }

  private Set<String> getAllTableNamesForResource(String resourceName) {
    final Set<String> tableNameSet = new HashSet<String>();
    tableNameSet.addAll(Arrays.asList(HelixHelper.getResourceConfigsFor(_helixClusterName, resourceName, _helixAdmin)
        .get("tableName").trim().split(",")));
    LOGGER.info("Current holding tables for resource: " + resourceName + " is "
        + Arrays.toString(tableNameSet.toArray(new String[0])));
    return tableNameSet;
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

  /**
   * TODO(xiafu) : Due to the issue of helix, if remove the segment from idealState directly, the node won't get transaction,
   * so we first disable the segment, then remove it from idealState, then enable the segment. This will trigger transactions.
   * After the helix patch, we can refine the logic here by directly drop the segment from idealState.
   *
   * @param resourceName
   * @param segmentId
   * @return
   */
  public synchronized PinotResourceManagerResponse deleteSegment(final String resourceName, final String segmentId) {

    LOGGER.info("Trying to delete segment : " + segmentId);
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    try {
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      idealState =
          PinotResourceIdealStateBuilder.dropSegmentFromIdealStateFor(resourceName, segmentId, _helixAdmin,
              _helixClusterName);
      _helixAdmin.setResourceIdealState(_helixClusterName, resourceName, idealState);
      idealState =
          PinotResourceIdealStateBuilder.removeSegmentFromIdealStateFor(resourceName, segmentId, _helixAdmin,
              _helixClusterName);
      _helixAdmin.setResourceIdealState(_helixClusterName, resourceName, idealState);
      _segmentDeletionManager.deleteSegment(resourceName, segmentId);

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
    final List<String> untaggedBrokerInstances = getOnlineUnTaggedBrokerInstanceList();
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

  public ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    return _propertyStore;
  }

  public HelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  public String getHelixClusterName() {
    return _helixClusterName;
  }

  public boolean isLeader() {
    return _helixZkManager.isLeader();
  }

  public HelixManager getHelixZkManager() {
    return _helixZkManager;
  }
}
