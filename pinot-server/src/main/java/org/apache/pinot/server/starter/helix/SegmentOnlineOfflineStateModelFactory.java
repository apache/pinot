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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
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
  // NOTE: Helix might process CONSUMING -> DROPPED transition as 2 separate transitions: CONSUMING -> OFFLINE followed
  // by OFFLINE -> DROPPED. Use this cache to track the segments that just went through CONSUMING -> OFFLINE transition
  // to detect CONSUMING -> DROPPED transition.
  // TODO: Check how Helix handle CONSUMING -> DROPPED transition and remove this cache if it's not needed.
  private final Cache<Pair<String, String>, Boolean> _recentlyOffloadedConsumingSegments =
      CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

  private final String _instanceId;
  private final InstanceDataManager _instanceDataManager;

  public SegmentOnlineOfflineStateModelFactory(String instanceId, InstanceDataManager instanceDataManager) {
    _instanceId = instanceId;
    _instanceDataManager = instanceDataManager;
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
    private final Logger _logger = LoggerFactory.getLogger(_instanceId + " - SegmentOnlineOfflineStateModel");

    @Transition(from = "OFFLINE", to = "CONSUMING")
    public void onBecomeConsumingFromOffline(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeConsumingFromOffline() : {}", message);
      _instanceDataManager.addConsumingSegment(message.getResourceName(), message.getPartitionName());
    }

    @Transition(from = "CONSUMING", to = "ONLINE")
    public void onBecomeOnlineFromConsuming(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOnlineFromConsuming() : {}", message);

      try {
        _instanceDataManager.addOnlineSegment(message.getResourceName(), message.getPartitionName());
      } catch (Exception e) {
        _logger.error(
            "Caught exception while processing SegmentOnlineOfflineStateModel.onBecomeOnlineFromConsuming() for "
                + "table: {}, segment: {}",
            message.getResourceName(), message.getPartitionName(), e);
        throw e;
      }
    }

    @Transition(from = "CONSUMING", to = "OFFLINE")
    public void onBecomeOfflineFromConsuming(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOfflineFromConsuming() : {}", message);
      String realtimeTableName = message.getResourceName();
      String segmentName = message.getPartitionName();
      _instanceDataManager.offloadSegment(realtimeTableName, segmentName);
      _recentlyOffloadedConsumingSegments.put(Pair.of(realtimeTableName, segmentName), true);
    }

    @Transition(from = "CONSUMING", to = "DROPPED")
    public void onBecomeDroppedFromConsuming(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromConsuming() : {}", message);
      String realtimeTableName = message.getResourceName();
      String segmentName = message.getPartitionName();
      _instanceDataManager.offloadSegment(realtimeTableName, segmentName);
      _instanceDataManager.deleteSegment(realtimeTableName, segmentName);
      onConsumingToDropped(realtimeTableName, segmentName);
    }

    /**
     * Should be invoked after segment is offloaded and deleted so that it can safely release the resources from table
     * data manager.
     */
    private void onConsumingToDropped(String realtimeTableName, String segmentName) {
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(realtimeTableName);
      if (tableDataManager == null) {
        _logger.warn(
            "Failed to find data manager for table: {}, skip invoking consuming to dropped callback for segment: {}",
            realtimeTableName, segmentName);
        return;
      }
      tableDataManager.onConsumingToDropped(segmentName);
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() : {}", message);

      try {
        _instanceDataManager.addOnlineSegment(message.getResourceName(), message.getPartitionName());
      } catch (Exception e) {
        _logger.error(
            "Caught exception while processing SegmentOnlineOfflineStateModel.onBecomeOnlineFromOffline() for table: "
                + "{}, segment: {}",
            message.getResourceName(), message.getPartitionName(), e);
        throw e;
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOfflineFromOnline() : {}", message);
      _instanceDataManager.offloadSegment(message.getResourceName(), message.getPartitionName());
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOffline() : {}", message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      _instanceDataManager.deleteSegment(tableNameWithType, segmentName);

      // Check if the segment is recently offloaded from CONSUMING to OFFLINE
      if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
        Pair<String, String> tableSegmentPair = Pair.of(tableNameWithType, segmentName);
        if (_recentlyOffloadedConsumingSegments.getIfPresent(tableSegmentPair) != null) {
          _recentlyOffloadedConsumingSegments.invalidate(tableSegmentPair);
          onConsumingToDropped(tableNameWithType, segmentName);
        }
      }
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromOnline() : {}", message);
      String tableNameWithType = message.getResourceName();
      String segmentName = message.getPartitionName();
      _instanceDataManager.offloadSegment(tableNameWithType, segmentName);
      _instanceDataManager.deleteSegment(tableNameWithType, segmentName);
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeOfflineFromError() : {}", message);
    }

    @Transition(from = "ERROR", to = "DROPPED")
    public void onBecomeDroppedFromError(Message message, NotificationContext context)
        throws Exception {
      _logger.info("SegmentOnlineOfflineStateModel.onBecomeDroppedFromError() : {}", message);
      _instanceDataManager.deleteSegment(message.getResourceName(), message.getPartitionName());
    }
  }
}
