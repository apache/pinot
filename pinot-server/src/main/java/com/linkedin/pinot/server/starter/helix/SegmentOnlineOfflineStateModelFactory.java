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
package com.linkedin.pinot.server.starter.helix;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;

/**
 * Data Server layer state model to take over how to operate on:
 * 1. Add a new segment
 * 2. Refresh an existed now serving segment.
 * 3. Delete an existed segment.
 */
public class SegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {

  private DataManager INSTANCE_DATA_MANAGER;
  private final String INSTANCE_ID;
  private static String HELIX_CLUSTER_NAME;
  private ZkHelixPropertyStore<ZNRecord> propertyStore;
  private final SegmentFetcherAndLoader _fetcherAndLoader;

  public SegmentOnlineOfflineStateModelFactory(String helixClusterName, String instanceId,
      DataManager instanceDataManager, ZkHelixPropertyStore<ZNRecord> propertyStore,
      SegmentFetcherAndLoader fetcherAndLoader) {
    _fetcherAndLoader = fetcherAndLoader;
    this.propertyStore = propertyStore;
    HELIX_CLUSTER_NAME = helixClusterName;
    INSTANCE_ID = instanceId;
    INSTANCE_DATA_MANAGER = instanceDataManager;
  }

  public static String getStateModelName() {
    return "SegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String partitionName) {
    final SegmentOnlineOfflineStateModel SegmentOnlineOfflineStateModel =
        new SegmentOnlineOfflineStateModel(HELIX_CLUSTER_NAME, INSTANCE_ID);
    return SegmentOnlineOfflineStateModel;
  }

  // Helix seems to need StateModelInfo annotation for 'initialState'. It does not use the 'states' field.
  // The transitions in the helix messages indicate the from/to states, and helix uses the
  // Transition annotations (but only if StateModelInfo is defined).
  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'CONSUMING', 'DROPPED'}", initialState = "OFFLINE")
  public class SegmentOnlineOfflineStateModel extends StateModel {
    private final Logger LOGGER =
        LoggerFactory.getLogger(INSTANCE_ID + " - " + SegmentOnlineOfflineStateModel.class);
    private final String _helixClusterName;
    private final String _instanceId;

    public SegmentOnlineOfflineStateModel(String helixClusterName, String instanceId) {
      _helixClusterName = helixClusterName;
      _instanceId = instanceId;
    }

    @Transition(from = "OFFLINE", to = "CONSUMING")
    public void onBecomeConsumingFromOnline(Message message, NotificationContext context) {
      Preconditions.checkState(SegmentName.isLowLevelConsumerSegmentName(message.getPartitionName()),
          "Tried to go into CONSUMING state on non-low level segment");
      LOGGER.info("SegmentOnlineOfflineStateModel.onBecomeConsumingFromOffline() : " + message);
      // We do the same processing as usual for going to the consuming state, which adds the segment to the table data
      // manager and starts Kafka consumption
      onBecomeOnlineFromOffline(message, context);
    }

    @Transition(from = "CONSUMING", to = "ONLINE")
    public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
      final TableDataManager tableDataManager =
          ((InstanceDataManager) INSTANCE_DATA_MANAGER).getTableDataManager(message.getResourceName());
      SegmentDataManager acquiredSegment = tableDataManager.acquireSegment(message.getPartitionName());

      try {
        if (acquiredSegment != null && acquiredSegment instanceof LLRealtimeSegmentDataManager) {
          ((LLRealtimeSegmentDataManager) acquiredSegment).goOnlineFromConsuming();
        }
      } finally {
        if (acquiredSegment != null) {
          tableDataManager.releaseSegment(acquiredSegment);
        }
      }
    }

    @Transition(from = "CONSUMING", to = "OFFLINE")
    public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
      LOGGER.info("SegmentOnlineOfflineStateModel.onBecomeOfflineFromConsuming() : " + message);
      final String segmentId = message.getPartitionName();
      try {
        INSTANCE_DATA_MANAGER.removeSegment(segmentId);
      } catch (final Exception e) {
        LOGGER.error("Cannot unload the segment : " + segmentId + "!\n" + e.getMessage(), e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "CONSUMING", to = "DROPPED")
    public void onBecomeDroppedFromConsuming(Message message, NotificationContext context) {
      LOGGER.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromConsuming() : " + message);
      try {
        onBecomeOfflineFromConsuming(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        LOGGER.error("Caught exception on CONSUMING -> DROPPED state transition", e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.info("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      final TableType tableType =
          TableNameBuilder.getTableTypeFromTableName(message.getResourceName());
      try {
        switch (tableType) {
        case OFFLINE:
          onBecomeOnlineFromOfflineForOfflineSegment(message, context);
          break;
        case REALTIME:
          onBecomeOnlineFromOfflineForRealtimeSegment(message, context);
          break;

        default:
          throw new RuntimeException(
              "Not supported table Type for onBecomeOnlineFromOffline message: " + message);
        }
      } catch (Exception e) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Caught exception in state transition for OFFLINE -> ONLINE for partition"
              + message.getPartitionName() + " of table " + message.getResourceName(), e);
        }
        Utils.rethrowException(e);
      }
    }

    private void onBecomeOnlineFromOfflineForRealtimeSegment(Message message,
        NotificationContext context) throws Exception {
      final String segmentId = message.getPartitionName();
      final String tableName = message.getResourceName();

      SegmentZKMetadata realtimeSegmentZKMetadata =
          ZKMetadataProvider.getRealtimeSegmentZKMetadata(propertyStore, tableName, segmentId);
      InstanceZKMetadata instanceZKMetadata =
          ZKMetadataProvider.getInstanceZKMetadata(propertyStore, _instanceId);
      AbstractTableConfig tableConfig =
          ZKMetadataProvider.getRealtimeTableConfig(propertyStore, tableName);
      ((InstanceDataManager) INSTANCE_DATA_MANAGER).addSegment(propertyStore, tableConfig,
          instanceZKMetadata, realtimeSegmentZKMetadata);
    }

    private void onBecomeOnlineFromOfflineForOfflineSegment(Message message, NotificationContext context) {
      final String segmentId = message.getPartitionName();
      final String tableName = message.getResourceName();
      _fetcherAndLoader.addOrReplaceOfflineSegment(tableName, segmentId, /*retryOnFailure=*/true);
    }

    // Remove segment from InstanceDataManager.
    // Still keep the data files in local.
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.info("SegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      final String segmentId = message.getPartitionName();
      try {
        INSTANCE_DATA_MANAGER.removeSegment(segmentId);
      } catch (final Exception e) {
        LOGGER.error("Cannot unload the segment : " + segmentId + "!\n" + e.getMessage(), e);
        Utils.rethrowException(e);
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
      final String segmentId = message.getPartitionName();
      final String tableName = message.getResourceName();
      try {
        final File segmentDir = new File(_fetcherAndLoader.getSegmentLocalDirectory(tableName, segmentId));
        if (segmentDir.exists()) {
          FileUtils.deleteQuietly(segmentDir);
          LOGGER.info("Deleted segment directory {}", segmentDir);
        }
      } catch (final Exception e) {
        LOGGER.error("Cannot delete the segment : " + segmentId + " from local directory!\n"
            + e.getMessage(), e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      LOGGER.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      try {
        onBecomeOfflineFromOnline(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        LOGGER.error("Caught exception on ONLINE -> DROPPED state transition", e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      LOGGER.info("Resetting the state for segment:{} from ERROR to OFFLINE", message.getPartitionName());
    }
  }

}
