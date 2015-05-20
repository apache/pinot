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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.tools.TestCommand.CommandType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.util.ZKMetadataUtils;


/**
 * HelixAdmin wrapper to perform all helix related CRUD operations
 * @author kgopalak
 *
 */
public class PinotHelixAdmin {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotHelixResourceManager.class);

  private String _zkAddress;
  private String _helixClusterName;
  private ZkClient _zkClient;
  private ZKHelixAdmin _helixAdmin;
  ZkHelixPropertyStore<ZNRecord> _propertyStore;
  HelixDataAccessor _helixDataAccessor;

  private Builder _helixPropertyKeyBuilder;

  /**
   * Initializes everything needed to interact with Helix
   * @param zkAddress
   * @param clusterName
   */
  public PinotHelixAdmin(String zkAddress, String clusterName) {
    this._zkAddress = zkAddress;
    this._helixClusterName = clusterName;
    _zkClient = new ZkClient(zkAddress);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _zkClient.waitUntilConnected(60, TimeUnit.SECONDS);
    _helixAdmin = new ZKHelixAdmin(_zkClient);

    ZkBaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
    String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    _propertyStore =
        new ZkHelixPropertyStore<ZNRecord>(baseDataAccessor, propertyStorePath, Arrays.asList(propertyStorePath));
    _helixDataAccessor = new ZKHelixDataAccessor(clusterName, baseDataAccessor);
    _helixPropertyKeyBuilder = new PropertyKey.Builder(clusterName);
  }

  /**
   * Creates new Offline Data Resource.
   * @param resource
   */
  public void createNewOfflineDataResource(DataResource resource) {
    final String offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(resource.getResourceName());
    OfflineDataResourceZKMetadata offlineDataResource = ZKMetadataUtils.getOfflineDataResourceMetadata(resource);
    final List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();
    final int numInstanceToUse = offlineDataResource.getNumDataInstances();
    LOGGER.info("Trying to allocate " + numInstanceToUse + " instances.");
    LOGGER.info("Current untagged boxes: " + unTaggedInstanceList.size());
    if (unTaggedInstanceList.size() < numInstanceToUse) {
      throw new UnsupportedOperationException("Cannot allocate enough hardware resource.");
    }
    for (int i = 0; i < numInstanceToUse; ++i) {
      LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + offlineResourceName);
      _helixAdmin.removeInstanceTag(_helixClusterName, unTaggedInstanceList.get(i),
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), offlineResourceName);
    }

    // now lets build an ideal state
    LOGGER.info("building empty ideal state for resource : " + offlineResourceName);

    final IdealState idealState =
        PinotResourceIdealStateBuilder.buildEmptyIdealStateFor(offlineResourceName,
            offlineDataResource.getNumDataReplicas(), _helixAdmin, _helixClusterName);
    LOGGER.info("adding resource via the admin");
    _helixAdmin.addResource(_helixClusterName, offlineResourceName, idealState);
    LOGGER.info("successfully added the resource : " + offlineResourceName + " to the cluster");

    // lets add resource configs
    ZKMetadataProvider.setOfflineResourceZKMetadata(_propertyStore, offlineDataResource);
    _propertyStore.create(ZKMetadataProvider.constructPropertyStorePathForResource(offlineResourceName), new ZNRecord(
        offlineResourceName), AccessOption.PERSISTENT);

  }

  /**
   *
   * @param resource
   */
  public void createNewRealtimeDataResource(DataResource resource) {
    final String realtimeResourceName =
        BrokerRequestUtils.getRealtimeResourceNameForResource(resource.getResourceName());
    RealtimeDataResourceZKMetadata realtimeDataResource = ZKMetadataUtils.getRealtimeDataResourceMetadata(resource);
    final List<String> unTaggedInstanceList = getOnlineUnTaggedServerInstanceList();

    final int numInstanceToUse = realtimeDataResource.getNumDataInstances();
    LOGGER.info("Trying to allocate " + numInstanceToUse + " instances.");
    LOGGER.info("Current untagged boxes: " + unTaggedInstanceList.size());
    if (unTaggedInstanceList.size() < numInstanceToUse) {
      throw new UnsupportedOperationException("Cannot allocate enough hardware resource.");
    }
    for (int i = 0; i < numInstanceToUse; ++i) {
      LOGGER.info("tag instance : " + unTaggedInstanceList.get(i).toString() + " to " + realtimeResourceName);
      _helixAdmin.removeInstanceTag(_helixClusterName, unTaggedInstanceList.get(i),
          CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      _helixAdmin.addInstanceTag(_helixClusterName, unTaggedInstanceList.get(i), realtimeResourceName);
    }
    // lets add resource configs
    ZKMetadataProvider.setRealtimeResourceZKMetadata(_propertyStore, realtimeDataResource);

    // now lets build an ideal state
    LOGGER.info("building empty ideal state for resource : " + realtimeResourceName);
    final IdealState idealState =
        PinotResourceIdealStateBuilder.buildInitialRealtimeIdealStateFor(realtimeResourceName, realtimeDataResource,
            _helixAdmin, _helixClusterName, _propertyStore);
    LOGGER.info("adding resource via the admin");
    _helixAdmin.addResource(_helixClusterName, realtimeResourceName, idealState);
    LOGGER.info("successfully added the resource : " + realtimeResourceName + " to the cluster");

    // Create Empty PropertyStore path
    _propertyStore.create(ZKMetadataProvider.constructPropertyStorePathForResource(realtimeResourceName), new ZNRecord(
        realtimeResourceName), AccessOption.PERSISTENT);

  }

  /**
   *
   * @param resource
   */
  public void createNewDataResource(DataResource resource) {
    if (resource.getResourceType() == Helix.TableType.OFFLINE) {
      createNewOfflineDataResource(resource);
    } else if (resource.getResourceType() == Helix.TableType.REALTIME) {
      createNewRealtimeDataResource(resource);
    } else {
      throw new UnsupportedOperationException("Hybrid Resource not supported.");
    }
  }

  /**
   * Computes the broker nodes that are untagged and free to be used.
   * @return
   */
  public List<String> getOnlineUnTaggedBrokerInstanceList() {
    final List<String> instanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
    //why only live instances?
    final List<String> liveInstances = _helixDataAccessor.getChildNames(_helixPropertyKeyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }

  /**
   * Computes the server nodes that are untagged and free to be used.
   * @return
   */
  public List<String> getOnlineUnTaggedServerInstanceList() {
    final List<String> instanceList =
        _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
    //why only live instances
    final List<String> liveInstances = _helixDataAccessor.getChildNames(_helixPropertyKeyBuilder.liveInstances());
    instanceList.retainAll(liveInstances);
    return instanceList;
  }
}
