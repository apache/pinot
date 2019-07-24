/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;
import static org.apache.pinot.common.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;


public class LeadControllerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeadControllerManager.class);

  private Set<Integer> _partitionIndexCache;
  private String _instanceId;
  private HelixManager _helixManager;
  private volatile boolean _isLeadControllerResourceEnabled = false;
  private volatile boolean _amIHelixLeader = false;
  private volatile boolean _isShuttingDown = false;

  public LeadControllerManager(String instanceId, HelixManager helixManager) {
    _instanceId = instanceId;
    _helixManager = helixManager;
    _partitionIndexCache = ConcurrentHashMap.newKeySet();
  }

  /**
   * Checks whether the current controller is the leader for the given table. Return true if current controller is the leader for this table.
   * Otherwise check whether the current controller is helix leader if the resource is disabled.
   * @param tableName table name with/without table type.
   */
  public boolean isLeaderForTable(String tableName) {
    if (_isLeadControllerResourceEnabled) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      int partitionIndex =
          LeadControllerUtils.getPartitionIdForTable(rawTableName, NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
      return _partitionIndexCache.contains(partitionIndex);
    } else {
      // Checks if it's Helix leader if lead controller resource is disabled.
      return _amIHelixLeader;
    }
  }

  /**
   * Given a partition name, marks current controller as lead controller for this partition by caching the partition index to current controller.
   * @param partitionName partition name in lead controller resource, e.g. leadControllerResource_0.
   */
  public synchronized void addPartitionLeader(String partitionName) {
    LOGGER.info("Add Partition: {} to LeadControllerManager", partitionName);
    int partitionIndex = LeadControllerUtils.extractPartitionIndex(partitionName);
    _partitionIndexCache.add(partitionIndex);
  }

  /**
   * Given a partition name, removes current controller as lead controller for this partition by removing the partition index from current controller.
   * @param partitionName partition name in lead controller resource, e.g. leadControllerResource_0.
   */
  public synchronized void removePartitionLeader(String partitionName) {
    LOGGER.info("Remove Partition: {} from LeadControllerManager", partitionName);
    int partitionIndex = LeadControllerUtils.extractPartitionIndex(partitionName);
    _partitionIndexCache.remove(partitionIndex);
  }

  /**
   * Checks from ZK if the current controller host is Helix cluster leader.
   */
  private boolean isHelixLeader() {
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    PropertyKey propertyKey = helixDataAccessor.keyBuilder().controllerLeader();
    LiveInstance liveInstance = helixDataAccessor.getProperty(propertyKey);
    String helixLeaderInstanceId = liveInstance.getInstanceName();
    return _instanceId.equals(CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE + helixLeaderInstanceId);
  }

  /**
   * Checks from ZK if resource config of leadControllerResource is enabled.
   */
  public boolean isLeadControllerResourceEnabled() {
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    PropertyKey propertyKey = helixDataAccessor.keyBuilder().resourceConfig(LEAD_CONTROLLER_RESOURCE_NAME);
    ResourceConfig resourceConfig = helixDataAccessor.getProperty(propertyKey);
    String enableResource = resourceConfig.getSimpleConfig(LeadControllerUtils.RESOURCE_ENABLED);
    return Boolean.parseBoolean(enableResource);
  }

  /**
   * Marks the cached indices invalid and isShuttingDown to be true.
   * Adding the synchronized block here and in the following callback methods
   * to make sure that {@link HelixManager} won't be closed when the callback changes happened.
   */
  public synchronized void stop() {
    _partitionIndexCache.clear();
    _isShuttingDown = true;
  }

  /**
   * Callback on changes in the controller. Should be registered to the controller callback. This callback is not needed when the resource is enabled.
   * However, the resource can be disabled sometime while the cluster is in operation, so we keep it here. Plus, it does not add much overhead.
   * At some point in future when we stop supporting the disabled resource, we will remove this line altogether and the logic that goes with it.
   */
  synchronized void onHelixControllerChange() {
    if (_isShuttingDown) {
      return;
    }
    if (isHelixLeader()) {
      if (!_amIHelixLeader) {
        _amIHelixLeader = true;
        LOGGER.info("Became Helix leader");
      } else {
        LOGGER.info("Already Helix leader. Duplicate notification");
      }
    } else {
      if (_amIHelixLeader) {
        _amIHelixLeader = false;
        LOGGER.info("Lost Helix leadership");
      } else {
        LOGGER.info("Already not Helix leader. Duplicate notification");
      }
    }
  }

  /**
   * Callback on changes in resource config.
   */
  synchronized void onResourceConfigChange() {
    if (_isShuttingDown) {
      return;
    }
    if (isLeadControllerResourceEnabled()) {
      LOGGER.info("Lead controller resource is enabled.");
      _isLeadControllerResourceEnabled = true;
    } else {
      LOGGER.info("Lead controller resource is disabled.");
      _isLeadControllerResourceEnabled = false;
    }
  }
}
