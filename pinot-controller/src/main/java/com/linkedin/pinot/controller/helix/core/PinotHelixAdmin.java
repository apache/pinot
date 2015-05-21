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
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.CommonConstants;


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
