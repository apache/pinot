package com.linkedin.pinot.server.starter.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.server.starter.helix.SegmentOnlineOfflineHandlerFactory.SegmentOnlineOfflineTransitionHandler;


public class SegmentOnlineOfflineHandlerFactory extends
    StateTransitionHandlerFactory<SegmentOnlineOfflineTransitionHandler> {
  private static StateModelDefId _stateModelDefId = new StateModelDefId("SegmentOnlineOfflineStateModel");

  public static StateModelDefId getStateModelDefId() {
    return _stateModelDefId;
  }

  @Override
  public SegmentOnlineOfflineTransitionHandler createStateTransitionHandler(PartitionId partitionId) {
    SegmentOnlineOfflineTransitionHandler transitionHandler = new SegmentOnlineOfflineTransitionHandler();
    return transitionHandler;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE'}", initialState = "OFFINE")
  public static class SegmentOnlineOfflineTransitionHandler extends TransitionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOnlineOfflineTransitionHandler.class);

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {

      System.out.println("OnlineOfflineTransitionHandler.onBecomeOnlineFromOffline() : " + message);

      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might start a service, run initialization, etc                            //
      ////////////////////////////////////////////////////////////////////////////////////////////////
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println("OnlineOfflineTransitionHandler.onBecomeOfflineFromOnline() : " + message);

      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might shutdown a service, log this event, or change monitoring settings   //
      ////////////////////////////////////////////////////////////////////////////////////////////////
    }

  }

}
