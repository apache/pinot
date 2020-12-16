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
package org.apache.pinot.server.starter.helix;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.concurrent.locks.Lock;
import org.apache.commons.io.FileUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.OfflineSegmentFetcherAndLoader;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.SegmentLocks;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data Server layer state model to take over how to operate on:
 * 1. Add a new segment
 * 2. Refresh an existed now serving segment.
 * 3. Delete an existed segment.
 */
public class SegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private final String _instanceId;
  private final InstanceDataManager _instanceDataManager;
  private final OfflineSegmentFetcherAndLoader _fetcherAndLoader;

  public SegmentOnlineOfflineStateModelFactory(String instanceId, InstanceDataManager instanceDataManager,
      OfflineSegmentFetcherAndLoader fetcherAndLoader) {
    _instanceId = instanceId;
    _instanceDataManager = instanceDataManager;
    _fetcherAndLoader = fetcherAndLoader;
  }

  public static String getStateModelName() {
    return "SegmentOnlineOfflineStateModel";
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new SegmentOnlineOfflineStateModel();
  }

  // Helix seems to need StateModelInfo annotation for 'initialState'. It does not use the 'states' field.
  // The transitions in the helix messages indicate the from/to states, and helix uses the
  // Transition annotations (but only if StateModelInfo is defined).
  @SuppressWarnings("unused")
  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'CONSUMING', 'DROPPED'}", initialState = "OFFLINE")
  public class SegmentOnlineOfflineStateModel extends StateModel {
    private final Logger _logger = LoggerFactory.getLogger(_instanceId + " - " + getClass().getName());

    @Transition(from = "OFFLINE", to = "CONSUMING")
    public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
      Preconditions.checkState(SegmentName.isLowLevelConsumerSegmentName(message.getPartitionName()),
          "Tried to go into CONSUMING state on non-low level segment");
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeConsumingFromOffline() : " + message);
      // We do the same processing as usual for going to the consuming state, which adds the segment to the table data
      // manager and starts Kafka consumption
      onBecomeOnlineFromOffline(message, context);
    }

    @Transition(from = "CONSUMING", to = "ONLINE")
    public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
      String realtimeTableName = message.getResourceName();
      String segmentNameStr = message.getPartitionName();
      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);

      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(realtimeTableName);
      Preconditions.checkNotNull(tableDataManager);
      SegmentDataManager acquiredSegment = tableDataManager.acquireSegment(segmentNameStr);
      // For this transition to be correct in helix, we should already have a segment that is consuming
      if (acquiredSegment == null) {
        throw new RuntimeException("Segment " + segmentNameStr + " + not present ");
      }

      try {
        if (!(acquiredSegment instanceof LLRealtimeSegmentDataManager)) {
          // We found a LLC segment that is not consuming right now, must be that we already swapped it with a
          // segment that has been built. Nothing to do for this state transition.
          _logger
              .info("Segment {} not an instance of LLRealtimeSegmentDataManager. Reporting success for the transition",
                  acquiredSegment.getSegmentName());
          return;
        }
        LLRealtimeSegmentDataManager segmentDataManager = (LLRealtimeSegmentDataManager) acquiredSegment;
        RealtimeSegmentZKMetadata metadata = ZKMetadataProvider
            .getRealtimeSegmentZKMetadata(_instanceDataManager.getPropertyStore(), segmentName.getTableName(),
                segmentNameStr);
        segmentDataManager.goOnlineFromConsuming(metadata);
      } catch (InterruptedException e) {
        _logger.warn("State transition interrupted", e);
        throw new RuntimeException(e);
      } finally {
        tableDataManager.releaseSegment(acquiredSegment);
      }
    }

    @Transition(from = "CONSUMING", to = "OFFLINE")
    public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOfflineFromConsuming() : " + message);
      String realtimeTableName = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.removeSegment(realtimeTableName, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition from CONSUMING -> OFFLINE for resource: {}, partition: {}",
            realtimeTableName, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "CONSUMING", to = "DROPPED")
    public void onBecomeDroppedFromConsuming(Message message, NotificationContext context) {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromConsuming() : " + message);
      try {
        onBecomeOfflineFromConsuming(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        _logger.error("Caught exception on CONSUMING -> DROPPED state transition", e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        TableType tableType = TableNameBuilder.getTableTypeFromTableName(message.getResourceName());
        Preconditions.checkNotNull(tableType);
        if (tableType == TableType.OFFLINE) {
          _instanceDataManager.addOrReplaceOfflineSegment(tableNameWithType, segmentName);
        } else {
          _instanceDataManager.addRealtimeSegment(tableNameWithType, segmentName);
        }
      } catch (Exception e) {
        _logger.error("Caught exception in state transition from OFFLINE -> ONLINE for resource: {}, partition: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    // Remove segment from InstanceDataManager.
    // Still keep the data files in local.
    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      try {
        _instanceDataManager.removeSegment(tableNameWithType, segmentName);
      } catch (Exception e) {
        _logger.error("Caught exception in state transition from ONLINE -> OFFLINE for resource: {}, partition: {}",
            tableNameWithType, segmentName, e);
        Utils.rethrowException(e);
      }
    }

    // Delete segment from local directory.
    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : " + message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();

      // This method might modify the file on disk. Use segment lock to prevent race condition
      Lock segmentLock = SegmentLocks.getSegmentLock(tableNameWithType, segmentName);
      try {
        segmentLock.lock();

        final File segmentDir = new File(_fetcherAndLoader.getSegmentLocalDirectory(tableNameWithType, segmentName));
        if (segmentDir.exists()) {
          FileUtils.deleteQuietly(segmentDir);
          _logger.info("Deleted segment directory {}", segmentDir);
        }
      } catch (final Exception e) {
        _logger.error("Cannot delete the segment : " + segmentName + " from local directory!\n" + e.getMessage(), e);
        Utils.rethrowException(e);
      } finally {
        segmentLock.unlock();
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : " + message);
      try {
        onBecomeOfflineFromOnline(message, context);
        onBecomeDroppedFromOffline(message, context);
      } catch (final Exception e) {
        _logger.error("Caught exception on ONLINE -> DROPPED state transition", e);
        Utils.rethrowException(e);
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      _logger.info("Resetting the state for segment:{} from ERROR to OFFLINE", message.getPartitionName());
    }
  }
}
