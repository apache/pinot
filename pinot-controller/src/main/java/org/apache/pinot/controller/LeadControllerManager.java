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
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.statemodel.LeadControllerChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;
import static org.apache.pinot.common.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;


public class LeadControllerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeadControllerManager.class);

  private final LeadControllerChecker _leadControllerChecker;
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private boolean _amIHelixLeader = false;

  public LeadControllerManager() {
    _leadControllerChecker = new LeadControllerChecker();
  }

  public void registerResourceManager(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  /**
   * Check whether the current controller is the leader for the given table. Return true if current controller is the leader for this table.
   * Otherwise check whether the current controller is helix leader.
   * @param tableName table name with/without table type.
   */
  public boolean isLeaderForTable(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int partitionIndex =
        HelixHelper.getPartitionIdForTable(rawTableName, NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
    if (_leadControllerChecker.isPartitionLeader(partitionIndex)) {
      return true;
    } else {
      return isHelixLeader();
    }
  }

  /**
   * Get lead controller id and partition index for given table. Returns -1 as partition index and null as lead controller id if not found.
   * @param tableName table name with/without table type.
   * @return lead controller id and partition index
   */
  public LeadControllerResponse getLeadControllerAndPartitionIdForTable(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    ExternalView leadControllerResourceExternalView = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceExternalView(_pinotHelixResourceManager.getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
    if (leadControllerResourceExternalView == null) {
      return new LeadControllerResponse();
    }
    Set<String> partitionSet = leadControllerResourceExternalView.getPartitionSet();
    if (partitionSet == null || partitionSet.isEmpty()) {
      return new LeadControllerResponse();
    }

    int numPartitions = partitionSet.size();
    int partitionIndex = HelixHelper.getPartitionIdForTable(rawTableName, numPartitions);
    String leadControllerId = HelixHelper.getLeadControllerForTable(leadControllerResourceExternalView, rawTableName);
    return new LeadControllerResponse(partitionIndex, leadControllerId);
  }

  public synchronized void addPartitionLeader(String partitionName) {
    _leadControllerChecker.addPartitionLeader(partitionName);
  }

  public synchronized void removePartitionLeader(String partitionName) {
    _leadControllerChecker.removePartitionLeader(partitionName);
  }

  /**
   * Checks if the current controller host is Helix leader.
   */
  private boolean isHelixLeader() {
    return _amIHelixLeader;
  }

  /**
   * Response class to return lead controller id and partition index.
   */
  public class LeadControllerResponse {
    private int _partitionIndex;
    private String _leadControllerId;

    public LeadControllerResponse(int partitionIndex, String leadControllerId) {
      _partitionIndex = partitionIndex;
      _leadControllerId = leadControllerId;
    }

    public LeadControllerResponse() {
      _partitionIndex = -1;
      _leadControllerId = null;
    }

    public int getPartitionIndex() {
      return _partitionIndex;
    }

    public String getLeadControllerId() {
      return _leadControllerId;
    }

    @Override
    public String toString() {
      return "Partition index: " + getPartitionIndex() + ", lead controller id: " + getLeadControllerId();
    }
  }

  /**
   * Callback on changes in the controller. Should be registered to the controller callback.
   */
  public void onHelixControllerChange() {
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

  public void onLeadControllerChange() {
  }
}
