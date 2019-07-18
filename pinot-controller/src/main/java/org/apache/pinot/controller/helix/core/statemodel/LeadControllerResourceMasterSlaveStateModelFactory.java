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
import org.apache.helix.examples.MasterSlaveStateModelFactory;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.pinot.controller.LeadControllerManager;


public class LeadControllerResourceMasterSlaveStateModelFactory extends MasterSlaveStateModelFactory {
  private final LeadControllerManager _leadControllerManager;

  public LeadControllerResourceMasterSlaveStateModelFactory(LeadControllerManager leadControllerManager) {
    super();
    _leadControllerManager = leadControllerManager;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    MasterSlaveStateModel stateModel = new LeadControllerResourceMasterSlaveStateModel();
    stateModel.setPartitionName(partitionName);
    return stateModel;
  }

  public class LeadControllerResourceMasterSlaveStateModel extends MasterSlaveStateModel {

    @Override
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      super.onBecomeMasterFromSlave(message, context);
      String partitionName = message.getPartitionName();
      _leadControllerManager.addPartitionLeader(partitionName);
    }

    @Override
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      super.onBecomeSlaveFromMaster(message, context);
      String partitionName = message.getPartitionName();
      _leadControllerManager.removePartitionLeader(partitionName);
    }
  }
}
