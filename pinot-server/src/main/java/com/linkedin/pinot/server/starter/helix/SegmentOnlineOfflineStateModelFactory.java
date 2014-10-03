package com.linkedin.pinot.server.starter.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.server.starter.ServerInstance;


/**
 * Data Server layer state model to take over how to operate on:
 * 1. Add a new segment
 * 2. Refresh an existed now servring segment.
 * 3. Delete an existed segment.
 * 
 * @author xiafu
 *
 */
public class SegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  private static ServerInstance SERVER_INSTANCE;
  private static DataManager INSTANCE_DATA_MANAGER;

  public SegmentOnlineOfflineStateModelFactory(ServerInstance serverInstance) {
    // SERVER_INSTANCE = serverInstance;
    // INSTANCE_DATA_MANAGER = serverInstance.getInstanceDataManager();
  }

  public static String getStateModelDef() {
    return "SegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String partitionName) {
    SegmentOnlineOfflineStateModel SegmentOnlineOfflineStateModel = new SegmentOnlineOfflineStateModel();
    return SegmentOnlineOfflineStateModel;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public static class SegmentOnlineOfflineStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOnlineOfflineStateModel.class);

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {

      System.out.println("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      String segmentId = message.getPartitionName();
      String resourceName = message.getResourceName();
      System.out.println(segmentId);
      System.out.println(resourceName);
      LOGGER.info("Trying to load segment : " + segmentId + " for resource : " + resourceName);
      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might start a service, run initialization, etc                            //
      ////////////////////////////////////////////////////////////////////////////////////////////////
      try {
        // INSTANCE_DATA_MANAGER.addSegment(null);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might shutdown a service, log this event, or change monitoring settings   //
      ////////////////////////////////////////////////////////////////////////////////////////////////
      try {
        // INSTANCE_DATA_MANAGER.removeSegment(message.getPartitionName());
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);

      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might shutdown a service, log this event, or change monitoring settings   //
      ////////////////////////////////////////////////////////////////////////////////////////////////
      try {
        // INSTANCE_DATA_MANAGER.removeSegment(message.getPartitionName());
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);

      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might shutdown a service, log this event, or change monitoring settings   //
      ////////////////////////////////////////////////////////////////////////////////////////////////
      try {
        // INSTANCE_DATA_MANAGER.removeSegment(message.getPartitionName());
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

  }

}
