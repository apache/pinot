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
package com.linkedin.pinot.controller.helix.core;

import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.controller.ControllerConf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;
import com.linkedin.pinot.controller.api.pojos.BrokerTagResource;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.api.pojos.Instance;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse.STATUS;
import com.linkedin.pinot.controller.helix.starter.HelixConfig;
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
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private String _localDiskDir;
  private SegmentDeletionManager _segmentDeletionManager = null;
  private long _externalViewOnlineToOfflineTimeout;

  PinotHelixAdmin _pinotHelixAdmin;
  @SuppressWarnings("unused")
  private PinotHelixResourceManager() {

  }

  public PinotHelixResourceManager(String zkURL, String helixClusterName, String controllerInstanceId,
      String localDiskDir) {
    _zkBaseUrl = zkURL;
    _helixClusterName = helixClusterName;
    _instanceId = controllerInstanceId;
    _localDiskDir = localDiskDir;
    _externalViewOnlineToOfflineTimeout = 10000L;
  }

  public PinotHelixResourceManager(ControllerConf controllerConf) {
    _zkBaseUrl = controllerConf.getZkStr();
    _helixClusterName = controllerConf.getHelixClusterName();
    _instanceId = controllerConf.getControllerHost() + "_" + controllerConf.getControllerPort();
    _localDiskDir = controllerConf.getDataDir();
    _externalViewOnlineToOfflineTimeout = controllerConf.getExternalViewOnlineToOfflineTimeout();
  }

  public synchronized void start() throws Exception {
    _helixZkURL = HelixConfig.getAbsoluteZkPathForHelix(_zkBaseUrl);
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    _propertyStore = ZkUtils.getZkPropertyStore(_helixZkManager, _helixClusterName);
    _segmentDeletionManager = new SegmentDeletionManager(_localDiskDir, _helixAdmin, _helixClusterName, _propertyStore);
    _externalViewOnlineToOfflineTimeout = 10000L;
    _pinotHelixAdmin = new PinotHelixAdmin(_helixZkURL, _helixClusterName);
  }

  public synchronized void stop() {
    _segmentDeletionManager.stop();
    _helixZkManager.disconnect();
  }

  public List<String> getBrokerInstancesFor(String resourceName) {
    String brokerTag = null;
    if (getAllResourceNames().contains(BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName))) {
      brokerTag =
          ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(),
              BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName)).getBrokerTag();
    }
    if (getAllResourceNames().contains(BrokerRequestUtils.getOfflineResourceNameForResource(resourceName))) {
      brokerTag =
          ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(),
              BrokerRequestUtils.getOfflineResourceNameForResource(resourceName)).getBrokerTag();
    }
    final List<String> instanceIds = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, brokerTag);
    return instanceIds;
  }

  public OfflineDataResourceZKMetadata getOfflineDataResourceZKMetadata(String resourceName) {
    return ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(), resourceName);
  }

  public RealtimeDataResourceZKMetadata getRealtimeDataResourceZKMetadata(String resourceName) {
    return ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(), resourceName);
  }

  public InstanceZKMetadata getInstanceZKMetadata(String instanceId) {
    return ZKMetadataProvider.getInstanceZKMetadata(getPropertyStore(), instanceId);
  }

  public List<String> getAllRealtimeResources() {
    List<String> ret = _helixAdmin.getResourcesInCluster(_helixClusterName);
    CollectionUtils.filter(ret, new Predicate() {

      @Override
      public boolean evaluate(Object object) {
        if (object == null) {
          return false;
        }

        if (object.toString().endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
          return true;
        }

        return false;
      }
    });

    return ret;
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
      if (CommonConstants.Helix.NON_PINOT_RESOURCE_RESOURCE_NAMES.contains(resourceName)) {
        continue;
      }
      pinotResourceNames.add(resourceName);
    }

    return pinotResourceNames;
  }

  public synchronized PinotResourceManagerResponse handleCreateNewDataResource(DataResource resource) {
    final PinotResourceManagerResponse res = new PinotResourceManagerResponse();
    ResourceType resourceType = null;
    try {
      resourceType = resource.getResourceType();
    } catch (Exception e) {
      res.errorMessage = "ResourceType has to be REALTIME/OFFLINE/HYBRID : " + e.getMessage();
      res.status = STATUS.failure;
      LOGGER.error(e.toString());
      throw new RuntimeException("ResourceType has to be REALTIME/OFFLINE/HYBRID.", e);
    }
    try {
      switch (resourceType) {
        case OFFLINE:
          _pinotHelixAdmin.createNewOfflineDataResource(resource);
          handleBrokerResource(resource);
          break;
        case REALTIME:
          _pinotHelixAdmin.createNewRealtimeDataResource(resource);
          handleBrokerResource(resource);
          break;
        case HYBRID:
          _pinotHelixAdmin.createNewOfflineDataResource(resource);
          _pinotHelixAdmin.createNewRealtimeDataResource(resource);
          handleBrokerResource(resource);
          break;
        default:
          break;
      }

    } catch (final Exception e) {
      res.errorMessage = e.getMessage();
      res.status = STATUS.failure;
      LOGGER.error("Caught exception while creating cluster with config " + resource.toString(), e);
      revertDataResource(resource);
      throw new RuntimeException("Error creating cluster, have successfull rolled back", e);
    }
    res.status = STATUS.success;
    return res;
  }

 




  

  private void handleBrokerResource(DataResource resource) {

    final BrokerTagResource brokerTagResource =
        new BrokerTagResource(resource.getNumberOfBrokerInstances(), resource.getBrokerTagName());
    BrokerDataResource brokerDataResource;
    ResourceType resourceType = resource.getResourceType();
    switch (resourceType) {
      case OFFLINE:
        brokerDataResource =
            new BrokerDataResource(BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName()),
                brokerTagResource);
        createBrokerDataResource(brokerDataResource, brokerTagResource);
        break;
      case REALTIME:
        brokerDataResource =
            new BrokerDataResource(BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName()),
                brokerTagResource);
        createBrokerDataResource(brokerDataResource, brokerTagResource);
        break;
      case HYBRID:
        brokerDataResource =
            new BrokerDataResource(BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName()),
                brokerTagResource);
        createBrokerDataResource(brokerDataResource, brokerTagResource);
        brokerDataResource =
            new BrokerDataResource(BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName()),
                brokerTagResource);
        createBrokerDataResource(brokerDataResource, brokerTagResource);
        break;
      default:
        break;
    }

  }

  private void createBrokerDataResource(BrokerDataResource brokerDataResource, BrokerTagResource brokerTagResource) {

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
    ResourceType resourceType = resource.getResourceType();
    switch (resourceType) {
      case OFFLINE:
        revertDataResourceFor(BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName()));
        break;
      case REALTIME:
        revertDataResourceFor(BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName()));
        break;
      case HYBRID:
        revertDataResourceFor(BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName()));
        revertDataResourceFor(BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName()));
        break;
      default:
        break;
    }
  }

  private void revertDataResourceFor(String resourceName) {
    final List<String> taggedInstanceList = _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resourceName);
    for (final String instance : taggedInstanceList) {
      LOGGER.info("untag instance : " + instance.toString());
      _helixAdmin.removeInstanceTag(_helixClusterName, instance, resourceName);
      _helixAdmin.addInstanceTag(_helixClusterName, instance, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    }
    _helixAdmin.dropResource(_helixClusterName, resourceName);
  }

  public synchronized PinotResourceManagerResponse handleUpdateDataResource(DataResource resource) {
    try {
      ResourceType resourceType = resource.getResourceType();
      String resourceName;
      switch (resourceType) {
        case OFFLINE:
          resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName());
          return handleUpdateDataResourceFor(resourceName, resource);
        case REALTIME:
          resourceName = BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName());
          return handleUpdateDataResourceFor(resourceName, resource);
        case HYBRID:
          resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName());
          PinotResourceManagerResponse res = handleUpdateDataResourceFor(resourceName, resource);
          if (res.isSuccessfull()) {
            resourceName = BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName());
            return handleUpdateDataResourceFor(resourceName, resource);
          } else {
            return res;
          }

        default:
          throw new RuntimeException("Unsupported operation for ResourceType: " + resourceType);
      }
    } catch (Exception e) {
      PinotResourceManagerResponse res = new PinotResourceManagerResponse();
      res.errorMessage = "ResourceType has to be REALTIME/OFFLINE/HYBRID : " + e.getMessage();
      res.status = STATUS.failure;
      LOGGER.error(e.toString());
      return res;
    }
  }

  /**
   * This only handles data resource changes. E.g. Number of data instances and number of data replicas.
   * 
   * @param resourceName
   * @param resource
   * @return
   */
  private synchronized PinotResourceManagerResponse handleUpdateDataResourceFor(String resourceName,
      DataResource resource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();

    if (!_helixAdmin.getResourcesInCluster(_helixClusterName).contains(resourceName)) {
      resp.status = STATUS.failure;
      resp.errorMessage = String.format("Resource (%s) does not exist", resourceName);
      return resp;
    }

    // Hardware assignment
    final List<String> unTaggedInstanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    final List<String> alreadyTaggedInstanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, resourceName);

    if (alreadyTaggedInstanceList.size() > resource.getNumberOfDataInstances()) {
      resp.status = STATUS.failure;
      resp.errorMessage =
          String.format(
              "Reducing cluster size is not supported for now, current number instances for resource (%s) is "
                  + alreadyTaggedInstanceList.size(), resourceName);
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
      LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + resourceName);
      _helixAdmin.removeInstanceTag(_helixClusterName, unTaggedInstanceList.get(i),
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), resourceName);
    }

    // now lets build an ideal state
    LOGGER.info("recompute ideal state for resource : " + resourceName);
    final IdealState idealState =
        PinotResourceIdealStateBuilder.updateExpandedDataResourceIdealStateFor(resourceName,
            resource.getNumberOfCopies(), _helixAdmin, _helixClusterName);
    LOGGER.info("update resource via the admin");
    _helixAdmin.setResourceIdealState(_helixClusterName, resourceName, idealState);
    LOGGER.info("successfully update the resource : " + resourceName + " to the cluster");
    switch (BrokerRequestUtils.getResourceTypeFromResourceName(resourceName)) {
      case REALTIME:
        RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata =
            ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(), resourceName);
        realtimeDataResourceZKMetadata.setNumDataInstances(resource.getNumberOfDataInstances());
        realtimeDataResourceZKMetadata.setNumDataReplicas(resource.getNumberOfCopies());
        ZKMetadataProvider.setRealtimeResourceZKMetadata(getPropertyStore(), realtimeDataResourceZKMetadata);

        break;
      case OFFLINE:
        OfflineDataResourceZKMetadata offlineDataResourceZKMetadata =
            ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(), resourceName);
        offlineDataResourceZKMetadata.setNumDataInstances(resource.getNumberOfDataInstances());
        offlineDataResourceZKMetadata.setNumDataReplicas(resource.getNumberOfCopies());
        ZKMetadataProvider.setOfflineResourceZKMetadata(getPropertyStore(), offlineDataResourceZKMetadata);
        break;
      default:
        throw new UnsupportedOperationException("Not supported resource type: " + resource);
    }
    resp.status = STATUS.success;
    return resp;

  }

  public synchronized PinotResourceManagerResponse handleAddTableToDataResource(DataResource resource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    boolean isSuccess = false;
    switch (resource.getResourceType()) {
      case OFFLINE:
        isSuccess = addTableToOfflineDataResource(resource.getResourceName(), resource.getTableName());
        break;
      case REALTIME:
        isSuccess = addTableToRealtimeDataResource(resource.getResourceName(), resource.getTableName());
        break;
      case HYBRID:
        isSuccess =
            addTableToOfflineDataResource(resource.getResourceName(), resource.getTableName())
                && addTableToRealtimeDataResource(resource.getResourceName(), resource.getTableName());
        break;

      default:
        resp.status = STATUS.failure;
        resp.errorMessage = String.format("ResourceType is not valid : (%s)!", resource.getResourceType());
        return resp;
    }

    if (isSuccess) {
      resp.status = STATUS.success;
      resp.errorMessage =
          String.format("Adding table name (%s) to resource (%s)", resource.getTableName(), resource.getResourceName());
      return resp;
    } else {
      resp.status = STATUS.failure;
      resp.errorMessage =
          String.format("Table name (%s) is already existed in resource (%s)", resource.getTableName(),
              resource.getResourceName());
      return resp;
    }
  }

  private boolean addTableToRealtimeDataResource(String resourceName, String tableName) {
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata =
        ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(), resourceName);
    List<String> tableList = realtimeDataResourceZKMetadata.getTableList();
    if (!tableList.contains(tableName)) {
      realtimeDataResourceZKMetadata.addToTableList(tableName);
      ZKMetadataProvider.setRealtimeResourceZKMetadata(getPropertyStore(), realtimeDataResourceZKMetadata);
      return true;
    }
    return false;
  }

  private boolean addTableToOfflineDataResource(String resourceName, String tableName) {
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata =
        ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(), resourceName);
    List<String> tableList = offlineDataResourceZKMetadata.getTableList();
    if (!tableList.contains(tableName)) {
      offlineDataResourceZKMetadata.addToTableList(tableName);
      ZKMetadataProvider.setOfflineResourceZKMetadata(getPropertyStore(), offlineDataResourceZKMetadata);
      return true;
    }
    return false;
  }

  public synchronized PinotResourceManagerResponse handleRemoveTableFromDataResource(DataResource resource) {
    return handleRemoveTableFromDataResource(resource.getResourceName(), resource.getTableName(), resource.getResourceType());
  }

  public synchronized PinotResourceManagerResponse handleRemoveTableFromDataResource(String resourceName, String tableName, ResourceType resourceType) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    boolean isSuccess = false;
    switch (resourceType) {
      case OFFLINE:
        isSuccess =
            removeTableToOfflineDataResource(BrokerRequestUtils.getOfflineResourceNameForResource(resourceName), tableName);
        break;
      case REALTIME:
        isSuccess =
            removeTableToRealtimeDataResource(BrokerRequestUtils.buildRealtimeResourceNameForResource(resourceName), tableName);
        break;
      case HYBRID:
        isSuccess = removeTableToOfflineDataResource(BrokerRequestUtils.getOfflineResourceNameForResource(resourceName), tableName)
            && removeTableToRealtimeDataResource(BrokerRequestUtils.buildRealtimeResourceNameForResource(resourceName), tableName);
        break;

      default:
        resp.status = STATUS.failure;
        resp.errorMessage = String.format("ResourceType is not valid : (%s)!", resourceType);
        return resp;
    }

    if (isSuccess) {
      resp.errorMessage = String.format("Removing table name (%s) from resource (%s)", tableName, resourceName);
      return resp;
    } else {
      resp.status = STATUS.failure;
      resp.errorMessage = String.format("Table name (%s) is not existed in resource (%s)", tableName, resourceName);
      return resp;
    }
  }

  private boolean removeTableToRealtimeDataResource(String resourceName, String tableName) {
    RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata =
        ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(), resourceName);
    List<String> tableList = realtimeDataResourceZKMetadata.getTableList();
    if (tableList.contains(tableName)) {
      tableList.remove(tableName);
      realtimeDataResourceZKMetadata.setTableList(tableList);
      ZKMetadataProvider.setRealtimeResourceZKMetadata(getPropertyStore(), realtimeDataResourceZKMetadata);
      deleteSegmentsInTable(resourceName, tableName);
      return true;
    }
    return false;
  }

  private boolean removeTableToOfflineDataResource(String resourceName, String tableName) {
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata =
        ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(), resourceName);
    List<String> tableList = offlineDataResourceZKMetadata.getTableList();
    if (tableList.contains(tableName)) {
      tableList.remove(tableName);
      offlineDataResourceZKMetadata.setTableList(tableList);
      ZKMetadataProvider.setOfflineResourceZKMetadata(getPropertyStore(), offlineDataResourceZKMetadata);
      deleteSegmentsInTable(BrokerRequestUtils.getOfflineResourceNameForResource(resourceName), tableName);
      return true;
    }
    return false;
  }

  private void deleteSegmentsInTable(String resourceName, String tableName) {
    for (String segmentId : getAllSegmentsForTable(resourceName, tableName)) {
      deleteSegment(resourceName, segmentId);
    }
  }

  /**
   * Handle update data resource config will require full configs to be updated. 
   *  
   * @param resource
   * @return
   */
  public synchronized PinotResourceManagerResponse handleUpdateDataResourceConfig(DataResource resource) {
    final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
    switch (resource.getResourceType()) {
      case REALTIME:
        RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata = ZKMetadataProvider.getRealtimeResourceZKMetadata(
            getPropertyStore(), BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName()));
        realtimeDataResourceZKMetadata = ZKMetadataUtils.updateRealtimeZKMetadataByDataResource(realtimeDataResourceZKMetadata, resource);
        ZKMetadataProvider.setRealtimeResourceZKMetadata(getPropertyStore(), realtimeDataResourceZKMetadata);
        break;
      case OFFLINE:
        OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(
            getPropertyStore(), BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName()));
        offlineDataResourceZKMetadata = ZKMetadataUtils.updateOfflineZKMetadataByDataResource(offlineDataResourceZKMetadata, resource);
        ZKMetadataProvider.setOfflineResourceZKMetadata(getPropertyStore(), offlineDataResourceZKMetadata);
        break;
      case HYBRID:
        realtimeDataResourceZKMetadata = ZKMetadataProvider.getRealtimeResourceZKMetadata(
            getPropertyStore(), BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName()));
        realtimeDataResourceZKMetadata = ZKMetadataUtils.updateRealtimeZKMetadataByDataResource(realtimeDataResourceZKMetadata, resource);
        ZKMetadataProvider.setRealtimeResourceZKMetadata(getPropertyStore(), realtimeDataResourceZKMetadata);
        offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(
            getPropertyStore(), BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName()));
        offlineDataResourceZKMetadata = ZKMetadataUtils.updateOfflineZKMetadataByDataResource(offlineDataResourceZKMetadata, resource);
        ZKMetadataProvider.setOfflineResourceZKMetadata(getPropertyStore(), offlineDataResourceZKMetadata);
        break;
      default:
        resp.status = STATUS.failure;
        resp.errorMessage = String.format("Resource type (%s) is not supported", resource.getRequestType());
        return resp;
    }
    resp.status = STATUS.success;
    resp.errorMessage = String.format("Resource (%s) properties have been updated", resource.getResourceName());
    return resp;
  }

  public synchronized PinotResourceManagerResponse handleUpdateBrokerResource(DataResource resource) {
    String resourceName = null;
    String currentResourceBrokerTag = null;
    switch (resource.getResourceType()) {
      case OFFLINE:
        resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName());
        OfflineDataResourceZKMetadata offlineDataResourceZKMetadata =
            ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(), resource.getResourceName());
        currentResourceBrokerTag = offlineDataResourceZKMetadata.getBrokerTag();

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
          offlineDataResourceZKMetadata.setNumBrokerInstance(resource.getNumberOfBrokerInstances());
          ZKMetadataProvider.setOfflineResourceZKMetadata(getPropertyStore(), offlineDataResourceZKMetadata);
          return createBrokerDataResource(new BrokerDataResource(resourceName, brokerTagResource));
        } else {
          return updateBrokerResourceTagResp;
        }

      case REALTIME:
        resourceName = BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName());
        RealtimeDataResourceZKMetadata realtimeDataResourceZKMetadata =
            ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(), resource.getResourceName());
        currentResourceBrokerTag = realtimeDataResourceZKMetadata.getBrokerTag();

        if (!currentResourceBrokerTag.equals(resource.getBrokerTagName())) {
          final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
          resp.status = STATUS.failure;
          resp.errorMessage =
              "Current broker tag for resource : " + resource + " is " + currentResourceBrokerTag
                  + ", not match updated request broker tag : " + resource.getBrokerTagName();
          return resp;
        }
        brokerTagResource =
            new BrokerTagResource(resource.getNumberOfBrokerInstances(), resource.getBrokerTagName());
        updateBrokerResourceTagResp = updateBrokerResourceTag(brokerTagResource);
        if (updateBrokerResourceTagResp.isSuccessfull()) {
          realtimeDataResourceZKMetadata.setNumBrokerInstance(resource.getNumberOfBrokerInstances());
          ZKMetadataProvider.setRealtimeResourceZKMetadata(getPropertyStore(), realtimeDataResourceZKMetadata);
          return createBrokerDataResource(new BrokerDataResource(resourceName, brokerTagResource));
        } else {
          return updateBrokerResourceTagResp;
        }

      case HYBRID:
        realtimeDataResourceZKMetadata =
            ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(), resource.getResourceName());
        offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(), resource.getResourceName());
        if (!realtimeDataResourceZKMetadata.getBrokerTag().equals(resource.getBrokerTagName())
            || !offlineDataResourceZKMetadata.getBrokerTag().equals(resource.getBrokerTagName())) {
          final PinotResourceManagerResponse resp = new PinotResourceManagerResponse();
          resp.status = STATUS.failure;
          resp.errorMessage =
              "Current broker tag for resource : " + resource + " is " + offlineDataResourceZKMetadata.getBrokerTag()
                  + "(offline) && " + realtimeDataResourceZKMetadata.getBrokerTag()
                  + "(realtime), not match updated request broker tag : " + resource.getBrokerTagName();
          return resp;
        }

        brokerTagResource =
            new BrokerTagResource(resource.getNumberOfBrokerInstances(), resource.getBrokerTagName());
        updateBrokerResourceTagResp = updateBrokerResourceTag(brokerTagResource);
        if (updateBrokerResourceTagResp.isSuccessfull()) {
          resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName());
          offlineDataResourceZKMetadata.setNumBrokerInstance(resource.getNumberOfBrokerInstances());
          ZKMetadataProvider.setOfflineResourceZKMetadata(getPropertyStore(), offlineDataResourceZKMetadata);
          PinotResourceManagerResponse resp = createBrokerDataResource(new BrokerDataResource(resourceName, brokerTagResource));
          if (resp.isSuccessfull()) {
            resourceName = BrokerRequestUtils.buildRealtimeResourceNameForResource(resource.getResourceName());
            realtimeDataResourceZKMetadata.setNumBrokerInstance(resource.getNumberOfBrokerInstances());
            ZKMetadataProvider.setRealtimeResourceZKMetadata(getPropertyStore(), realtimeDataResourceZKMetadata);
            return createBrokerDataResource(new BrokerDataResource(resourceName, brokerTagResource));
          } else {
            return resp;
          }
        } else {
          return updateBrokerResourceTagResp;
        }

      default:
        throw new RuntimeException("Error in updating broker resource: not supported resource type!");
    }
  }

  // Trying to remove only one type of data resource.
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
    ZKMetadataProvider.removeResourceSegmentsFromPropertyStore(getPropertyStore(), resourceTag);
    ZKMetadataProvider.removeResourceConfigFromPropertyStore(getPropertyStore(), resourceTag);
    // Remove groupId/PartitionId mapping for realtime resource type.
    if (BrokerRequestUtils.getResourceTypeFromResourceName(resourceTag) == ResourceType.REALTIME) {
      for (String instance : taggedInstanceList) {
        InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(getPropertyStore(), instance);
        instanceZKMetadata.removeResource(resourceTag);
        ZKMetadataProvider.setInstanceZKMetadata(getPropertyStore(), instanceZKMetadata);
      }
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

  public List<String> getAllSegmentsForTable(String resourceName, String tableName) {
    List<String> segmentsInTable = new ArrayList<String>();
    switch (BrokerRequestUtils.getResourceTypeFromResourceName(resourceName)) {
      case REALTIME:
        for (RealtimeSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getRealtimeResourceZKMetadataListForResource(getPropertyStore(), resourceName)) {
          if (segmentZKMetadata.getTableName().equals(tableName)) {
            segmentsInTable.add(segmentZKMetadata.getSegmentName());
          }
        }

        break;
      case OFFLINE:
        for (OfflineSegmentZKMetadata segmentZKMetadata : ZKMetadataProvider.getOfflineResourceZKMetadataListForResource(getPropertyStore(), resourceName)) {
          if (segmentZKMetadata.getTableName().equals(tableName)) {
            segmentsInTable.add(segmentZKMetadata.getSegmentName());
          }
        }
        break;
      default:
        break;
    }
    return segmentsInTable;
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
          OfflineSegmentZKMetadata offlineSegmentZKMetadata =
              ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, segmentMetadata.getResourceName(),
                  segmentMetadata.getName());

          offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
          offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
          offlineSegmentZKMetadata.setRefreshTime(System.currentTimeMillis());
          ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
          LOGGER.info("Refresh segment : " + offlineSegmentZKMetadata.getSegmentName() + " to Property store");
          if (updateExistedSegment(offlineSegmentZKMetadata)) {
            res.status = STATUS.success;
          }
        } else {
          String msg = "Not refreshing identical segment " + segmentMetadata.getName() + " with creation time " +
              segmentMetadata.getIndexCreationTime() + " and crc " + segmentMetadata.getCrc();
          LOGGER.info(msg);
          res.status = STATUS.success;
          res.errorMessage = msg;
        }
      } else {
        OfflineSegmentZKMetadata offlineSegmentZKMetadata = new OfflineSegmentZKMetadata();
        offlineSegmentZKMetadata = ZKMetadataUtils.updateSegmentMetadata(offlineSegmentZKMetadata, segmentMetadata);
        offlineSegmentZKMetadata.setDownloadUrl(downloadUrl);
        offlineSegmentZKMetadata.setPushTime(System.currentTimeMillis());
        ZKMetadataProvider.setOfflineSegmentZKMetadata(_propertyStore, offlineSegmentZKMetadata);
        LOGGER.info("Added segment : " + offlineSegmentZKMetadata.getSegmentName() + " to Property store");

        final IdealState idealState =
            PinotResourceIdealStateBuilder.addNewOfflineSegmentToIdealStateFor(segmentMetadata, _helixAdmin,
                _helixClusterName, getPropertyStore());
        _helixAdmin.setResourceIdealState(_helixClusterName,
            BrokerRequestUtils.getOfflineResourceNameForResource(offlineSegmentZKMetadata.getResourceName()),
            idealState);
        res.status = STATUS.success;
      }
    } catch (final Exception e) {
      res.status = STATUS.failure;
      res.errorMessage = e.getMessage();
      LOGGER.error("Caught exception while adding segment", e);
    }
    return res;
  }

  private boolean updateExistedSegment(SegmentZKMetadata segmentZKMetadata) {
    final String resourceName;
    if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
      resourceName = BrokerRequestUtils.buildRealtimeResourceNameForResource(segmentZKMetadata.getResourceName());
    } else {
      resourceName = BrokerRequestUtils.getOfflineResourceNameForResource(segmentZKMetadata.getResourceName());
    }
    final String segmentName = segmentZKMetadata.getSegmentName();

    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey idealStatePropertyKey = helixDataAccessor.keyBuilder().idealStates(resourceName);

    // Set all partitions to offline to unload them from the servers
    boolean updateSuccessful;
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "OFFLINE");
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    // Wait until the partitions are offline in the external view
    LOGGER.info("Wait until segment - " + segmentName + " to be OFFLINE in ExternalView");
    if (!ifExternalViewChangeReflectedForState(resourceName, segmentName, "OFFLINE",
        _externalViewOnlineToOfflineTimeout)) {
      LOGGER.error("Cannot get OFFLINE state to be reflected on ExternalView changed for segment: " + segmentName);
      return false;
    }

    // Set all partitions to online so that they load the new segment data
    do {
      final IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, resourceName);
      final Set<String> instanceSet = idealState.getInstanceSet(segmentName);
      for (final String instance : instanceSet) {
        idealState.setPartitionState(segmentName, instance, "ONLINE");
      }
      updateSuccessful = helixDataAccessor.updateProperty(idealStatePropertyKey, idealState);
    } while (!updateSuccessful);

    LOGGER.info("Refresh is done for segment - " + segmentName);
    return true;
  }

  private boolean ifExternalViewChangeReflectedForState(String resourceName, String segmentName, String targerStates,
      long timeOutInMills) {
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
      if (segmentStatsMap != null) {
        for (String instance : segmentStatsMap.keySet()) {
          if (!segmentStatsMap.get(instance).equalsIgnoreCase(targerStates)) {
            isSucess = false;
          }
        }
      } else {
        isSucess = false;
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
    return _propertyStore.exists(
        ZKMetadataProvider.constructPropertyStorePathForSegment(
            BrokerRequestUtils.getOfflineResourceNameForResource(segmentMetadata.getResourceName()), segmentMetadata.getName()),
        AccessOption.PERSISTENT);
  }

  private boolean ifRefreshAnExistedSegment(SegmentMetadata segmentMetadata) {
    OfflineSegmentZKMetadata offlineSegmentZKMetadata =
        ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, segmentMetadata.getResourceName(),
            segmentMetadata.getName());
    if (offlineSegmentZKMetadata == null) {
      return false;
    }
    final SegmentMetadata existedSegmentMetadata = new SegmentMetadataImpl(offlineSegmentZKMetadata);
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
    if ("realtime".equalsIgnoreCase(segmentMetadata.getIndexType())) {
      if (getAllResourceNames().contains(
          BrokerRequestUtils.buildRealtimeResourceNameForResource(segmentMetadata.getResourceName()))) {
        if (getAllTableNamesForResource(
            BrokerRequestUtils.buildRealtimeResourceNameForResource(segmentMetadata.getResourceName())).contains(
            segmentMetadata.getTableName())) {
          return true;
        }
      }
    }
    if (!getAllResourceNames().contains(
        BrokerRequestUtils.getOfflineResourceNameForResource(segmentMetadata.getResourceName()))) {
      LOGGER.error("Resource is not registered");
      return false;
    }
    if (!getAllTableNamesForResource(
        BrokerRequestUtils.getOfflineResourceNameForResource(segmentMetadata.getResourceName())).contains(
        segmentMetadata.getTableName())) {
      LOGGER.error("Table " + segmentMetadata.getTableName() + " is not registered for resource: "
          + segmentMetadata.getResourceName());
      return false;
    }
    return true;
  }

  private Set<String> getAllTableNamesForResource(String resourceName) {
    final Set<String> tableNameSet = new HashSet<String>();
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.OFFLINE_RESOURCE_SUFFIX)) {
      tableNameSet.addAll(ZKMetadataProvider.getOfflineResourceZKMetadata(getPropertyStore(), resourceName).getTableList());
    }
    if (resourceName.endsWith(CommonConstants.Broker.DataResource.REALTIME_RESOURCE_SUFFIX)) {
      tableNameSet.addAll(ZKMetadataProvider.getRealtimeResourceZKMetadata(getPropertyStore(), resourceName).getTableList());
    }
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
    final List<String> untaggedBrokerInstances = _pinotHelixAdmin.getOnlineUnTaggedBrokerInstanceList();
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
    final List<String> dataResourceList = new ArrayList<String>(brokerIdealState.getPartitionSet());
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
