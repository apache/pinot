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
  private static int TRANSITION_DELAY = 10;
  private final LeadControllerManager _leadControllerManager;
  private final String _instanceName;

  public LeadControllerResourceMasterSlaveStateModelFactory(String instanceName,
      LeadControllerManager leadControllerManager) {
    _instanceName = instanceName;
    _leadControllerManager = leadControllerManager;
  }

  public String getStateModelName() {
    return "LeadControllerResourceMasterSlaveStateModel";
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    LeadControllerResourceMasterSlaveStateModel stateModel = new LeadControllerResourceMasterSlaveStateModel();
    stateModel.setPartitionName(partitionName);
    stateModel.setDelay(TRANSITION_DELAY);
    return stateModel;
  }

  @StateModelInfo(states = "{'MASTER','SLAVE', 'OFFLINE', 'DROPPED'}", initialState = "OFFLINE")
  public class LeadControllerResourceMasterSlaveStateModel extends StateModel {
    private final Logger _logger = LoggerFactory.getLogger(_instanceName + " - " + getClass().getName());
    private String _partitionName;
    private long _transDelay;

    public String getPartitionName() {
      return _partitionName;
    }

    public void setPartitionName(String partitionName) {
      _partitionName = partitionName;
    }

    public void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    private void sleep() {
      try {
        Thread.sleep(_transDelay);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Transition(from = "OFFLINE", to = "SLAVE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      _logger.info("{}: transitioning from {} to {} for {}", getStateModelName(), message.getFromState(),
          message.getToState(), _partitionName);
      sleep();
    }

    @Transition(from = "MASTER", to = "SLAVE")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      _logger.info("{}: transitioning from {} to {} for {}", getStateModelName(), message.getFromState(),
          message.getToState(), _partitionName);
      String partitionName = message.getPartitionName();
      _leadControllerManager.removePartitionLeader(partitionName);
      sleep();
    }

    @Transition(from = "SLAVE", to = "MASTER")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      _logger.info("{}: transitioning from {} to {} for {}", getStateModelName(), message.getFromState(),
          message.getToState(), _partitionName);
      String partitionName = message.getPartitionName();
      _leadControllerManager.addPartitionLeader(partitionName);
      sleep();
    }

    @Transition(from = "SLAVE", to = "OFFLINE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      _logger.info("{}: transitioning from {} to {} for {}", getStateModelName(), message.getFromState(),
          message.getToState(), _partitionName);
      sleep();
    }

    @Transition(from = "OFFLINE", to = "MASTER")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      _logger.info("{}: transitioning from {} to {} for {}", getStateModelName(), message.getFromState(),
          message.getToState(), _partitionName);
      sleep();
    }
  }
}
