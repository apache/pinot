package com.linkedin.pinot.controller.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;


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
    private static final Logger LOGGER = Logger.getLogger(EmptyBrokerOnlineOfflineStateModel.class);

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
