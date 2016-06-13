/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EmptyBrokerOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  @Override
  public StateModel createNewStateModel(String partitionName) {
    final EmptyBrokerOnlineOfflineStateModel SegmentOnlineOfflineStateModel = new EmptyBrokerOnlineOfflineStateModel();
    return SegmentOnlineOfflineStateModel;
  }

  public EmptyBrokerOnlineOfflineStateModelFactory() {
  }

  public static String getStateModelDef() {
    return "BrokerResourceOnlineOfflineStateModel";
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public static class EmptyBrokerOnlineOfflineStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmptyBrokerOnlineOfflineStateModel.class);

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.info("EmptyBrokerOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.info("EmptyBrokerOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("EmptyBrokerOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      LOGGER.info("EmptyBrokerOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
    }
  }

}
