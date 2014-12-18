package com.linkedin.pinot.controller.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;


public class EmptySegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  @Override
  public StateModel createNewStateModel(String partitionName) {
    final EmptySegmentOnlineOfflineStateModel SegmentOnlineOfflineStateModel = new EmptySegmentOnlineOfflineStateModel();
    return SegmentOnlineOfflineStateModel;
  }

  public EmptySegmentOnlineOfflineStateModelFactory() {
  }

  public static String getStateModelDef() {
    return "SegmentOnlineOfflineStateModel";
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public static class EmptySegmentOnlineOfflineStateModel extends StateModel {
    private static final Logger LOGGER = Logger.getLogger(EmptySegmentOnlineOfflineStateModel.class);

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.info("EmptySegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.info("EmptySegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("EmptySegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      LOGGER.info("EmptySegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
    }
  }

}
