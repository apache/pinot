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
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;


public class LeadControllerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeadControllerManager.class);

  private Set<Integer> _partitionIndexCache;
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private volatile boolean _isLeadControllerResourceEnabled = false;
  private volatile boolean _amIHelixLeader = false;

  public LeadControllerManager() {
    _partitionIndexCache = ConcurrentHashMap.newKeySet();
  }

  /**
   * Registers {@link PinotHelixResourceManager} in LeadControllerManager, which is needed to get the external view of lead controller resource.
   */
  public void registerResourceManager(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  /**
   * Marks the cached indices invalid and unregisters {@link PinotHelixResourceManager}.
   */
  public void stop() {
    _partitionIndexCache.clear();
    _pinotHelixResourceManager = null;
  }

  /**
   * Checks whether the current controller is the leader for the given table. Return true if current controller is the leader for this table.
   * Otherwise check whether the current controller is helix leader if the resource is disabled.
   * @param tableName table name with/without table type.
   */
  public boolean isLeaderForTable(String tableName) {
    if (isLeadControllerResourceEnabled()) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      int partitionIndex = LeadControllerUtils.getPartitionIdForTable(rawTableName, NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
      return _partitionIndexCache.contains(partitionIndex);
    } else {
      // Checks if it's Helix leader if lead controller resource is disabled.
      return isHelixLeader();
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
   * Checks if the current controller host is Helix cluster leader.
   */
  private boolean isHelixLeader() {
    return _amIHelixLeader;
  }

  public boolean isLeadControllerResourceEnabled() {
    return _isLeadControllerResourceEnabled;
  }

  /**
   * Callback on changes in the controller. Should be registered to the controller callback. This callback is not needed when the resource is enabled.
   * However, the resource can be disabled sometime while the cluster is in operation, so we keep it here. Plus, it does not add much overhead.
   * At some point in future when we stop supporting the disabled resource, we will remove this line altogether and the logic that goes with it.
   */
  void onHelixControllerChange() {
    if (_pinotHelixResourceManager.isHelixLeader()) {
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

  void onResourceConfigChange() {
    if (_pinotHelixResourceManager.isLeadControllerResourceEnabled()) {
      LOGGER.info("Lead controller resource is enabled.");
      _isLeadControllerResourceEnabled = true;
    } else {
      LOGGER.info("Lead controller resource is disabled.");
      _isLeadControllerResourceEnabled = false;
    }
  }
}
