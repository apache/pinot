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
        HelixHelper.getHashCodeForTable(rawTableName) % NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;
    if (_leadControllerChecker.isPartitionLeader(partitionIndex)) {
      return true;
    } else {
      return isHelixLeader();
    }
  }

  /**
   * Get lead controller for table.
   * @param tableName table name with/without table type.
   * @return lead controller id
   */
  public String getLeadControllerForTable(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    ExternalView leadControllerResourceExternalView = _pinotHelixResourceManager.getHelixAdmin()
        .getResourceExternalView(_pinotHelixResourceManager.getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
    return HelixHelper.getLeadControllerForTable(leadControllerResourceExternalView, rawTableName);
  }

  public synchronized void addPartitionLeader(String partitionName) {
    _leadControllerChecker.addPartitionLeader(partitionName);
  }

  public synchronized void removePartitionLeader(String partitionName) {
    _leadControllerChecker.removePartitionLeader(partitionName);
  }

  private boolean isHelixLeader() {
    return _pinotHelixResourceManager != null && _pinotHelixResourceManager.isHelixLeader();
  }

  public void onLeadControllerChange() {
  }
}
