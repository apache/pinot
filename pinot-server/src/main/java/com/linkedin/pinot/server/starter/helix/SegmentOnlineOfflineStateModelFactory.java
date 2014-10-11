package com.linkedin.pinot.server.starter.helix;

import org.apache.helix.AccessOption;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;
import com.linkedin.pinot.common.utils.StringUtil;


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

  private static DataManager INSTANCE_DATA_MANAGER;
  private static SegmentMetadataLoader SEGMENT_METADATA_LOADER;

  public SegmentOnlineOfflineStateModelFactory(DataManager instanceDataManager,
      SegmentMetadataLoader segmentMetadataLoader) {
    INSTANCE_DATA_MANAGER = instanceDataManager;
    SEGMENT_METADATA_LOADER = segmentMetadataLoader;
  }

  public SegmentOnlineOfflineStateModelFactory() {
    // TODO Auto-generated constructor stub
  }

  public static String getStateModelDef() {
    return "SegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String partitionName) {
    final SegmentOnlineOfflineStateModel SegmentOnlineOfflineStateModel = new SegmentOnlineOfflineStateModel();
    return SegmentOnlineOfflineStateModel;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'DROPPED'}", initialState = "OFFLINE")
  public static class SegmentOnlineOfflineStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentOnlineOfflineStateModel.class);

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {


      System.out.println("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      final String segmentId = message.getPartitionName();
      final String resourceName = message.getResourceName();

      final String pathToPropertyStore = "/" + StringUtil.join("/", resourceName, segmentId);

      final ZNRecord record = context.getManager().getHelixPropertyStore().get(pathToPropertyStore, null, AccessOption.PERSISTENT);

      System.out.println(segmentId);
      System.out.println(resourceName);
      LOGGER.info("Trying to load segment : " + segmentId + " for resource : " + resourceName);
      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might start a service, run initialization, etc                            //
      ////////////////////////////////////////////////////////////////////////////////////////////////
      try {
        final String localSegmentDir = downloadSegmentToLocal(message);
        //        SegmentMetadata segmentMetadata =
        //            COLUMNAR_SEGMENT_METADATA_LOADER.loadIndexSegmentMetadataFromDir(localSegmentDir);
        //        INSTANCE_DATA_MANAGER.addSegment(segmentMetadata);
      } catch (final Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    //TODO(xiafu): get segment meta from property store and download the segment and untar'ed to local directory.
    private String downloadSegmentToLocal(Message message) {
      return null;
    }

    // Remove segment from InstanceDataManager.
    // Still keep the data files in local.
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might shutdown a service, log this event, or change monitoring settings   //
      ////////////////////////////////////////////////////////////////////////////////////////////////
      try {
        INSTANCE_DATA_MANAGER.removeSegment(message.getPartitionName());
      } catch (final Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);

      ////////////////////////////////////////////////////////////////////////////////////////////////
      // Application logic to handle transition                                                     //
      // For example, you might shutdown a service, log this event, or change monitoring settings   //
      ////////////////////////////////////////////////////////////////////////////////////////////////
      try {
        // TODO: Remove segment from local directory.
      } catch (final Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      System.out.println("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      try {
        onBecomeOfflineFromOnline(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

  }

}
