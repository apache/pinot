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
package org.apache.pinot.controller.helix.core.statemodel;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.controller.LeadControllerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LeadControllerResourceMasterSlaveStateModelFactory extends StateModelFactory<StateModel> {
  private final LeadControllerManager _leadControllerManager;

  public LeadControllerResourceMasterSlaveStateModelFactory(LeadControllerManager leadControllerManager) {
    _leadControllerManager = leadControllerManager;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new LeadControllerResourceMasterSlaveStateModel(_leadControllerManager, partitionName);
  }

  @StateModelInfo(states = "{'MASTER','SLAVE', 'OFFLINE', 'DROPPED'}", initialState = "OFFLINE")
  public static class LeadControllerResourceMasterSlaveStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeadControllerResourceMasterSlaveStateModel.class);

    private final LeadControllerManager _leadControllerManager;
    private final String _partitionName;

    private LeadControllerResourceMasterSlaveStateModel(LeadControllerManager leadControllerManager,
        String partitionName) {
      _leadControllerManager = leadControllerManager;
      _partitionName = partitionName;
    }

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Got state transition from OFFLINE to SLAVE for partition: {}", _partitionName);
    }

    @Transition(from = "SLAVE", to = "MASTER")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      LOGGER.info("Got state transition from SLAVE to MASTER for partition: {}", _partitionName);
      _leadControllerManager.addPartitionLeader(_partitionName);
    }

    @Transition(from = "MASTER", to = "SLAVE")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      LOGGER.info("Got state transition from MASTER to SLAVE for partition: {}", _partitionName);
      _leadControllerManager.removePartitionLeader(_partitionName);
    }

    @Transition(from = "SLAVE", to = "OFFLINE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      LOGGER.info("Got state transition from SLAVE to OFFLINE for partition: {}", _partitionName);
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Got state transition from OFFLINE to DROPPED for partition: {}", _partitionName);
    }
  }
}
