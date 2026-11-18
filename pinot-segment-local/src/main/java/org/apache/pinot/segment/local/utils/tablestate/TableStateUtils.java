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
package org.apache.pinot.segment.local.utils.tablestate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableStateUtils {
  private TableStateUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableStateUtils.class);

  /// Returns all segments in a given state for a given table.
  ///
  /// @param helixManager instance of Helix manager
  /// @param tableNameWithType table for which we are obtaining segments in a given state
  /// @param state state of the segments to be returned
  ///
  /// @return List of segment names in a given state.
  public static List<String> getSegmentsInGivenStateForThisInstance(HelixManager helixManager, String tableNameWithType,
      String state) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    IdealState idealState = dataAccessor.getProperty(keyBuilder.idealStates(tableNameWithType));
    List<String> segmentsInGivenState = new ArrayList<>();
    if (idealState == null) {
      LOGGER.warn("Failed to find ideal state for table: {}", tableNameWithType);
      return segmentsInGivenState;
    }

    // Get all segments with state from idealState
    String instanceName = helixManager.getInstanceName();
    Map<String, Map<String, String>> idealStatesMap = idealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : idealStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      String expectedState = instanceStateMap.get(instanceName);
      // Only track state segments assigned to the current instance
      if (!state.equals(expectedState)) {
        continue;
      }
      segmentsInGivenState.add(segmentName);
    }
    return segmentsInGivenState;
  }

  /// Checks the current assignment before a consumer starts. Earlier ONLINE segments of the same partition must be
  /// loaded, as must uploaded segments from that partition. Resolve partitions from names or segment metadata,
  /// matching upsert and dedup loading; unresolved segments are conservatively included. The consumer and its
  /// successors are excluded to avoid waiting on themselves.
  ///
  /// @param isLocallyReady identifies segments whose metadata is already ready locally despite a pending ONLINE
  ///                      transition, such as a predecessor that finished consumption and released its permit
  public static boolean isReadyToConsume(HelixManager helixManager, String tableNameWithType, LLCSegmentName consumer,
      Predicate<String> isLocallyReady) {
    return getReadinessSnapshot(helixManager, tableNameWithType, isLocallyReady)
        .isReadyToConsume(consumer, isLocallyReady, true);
  }

  /// Reads the table assignment and this live instance's current state once for evaluation by multiple consumers.
  /// Only what evaluation reads is kept: this instance's CONSUMING segments, and its ONLINE segments that are not
  /// ONLINE in current state. Partition metadata is read only for custom-named segments among the latter that are not
  /// locally ready. Loaded segments cannot block consumption, so neither they nor their partitions are retained.
  /// A cached snapshot must only reject readiness; callers must perform a fresh check before authorizing consumption
  /// from a cached result.
  ///
  /// @param isLocallyReady a thread-safe predicate used only to skip unnecessary partition lookups. Its results are
  ///                      not retained: evaluation must check local readiness again in case a manager was replaced.
  public static ReadinessSnapshot getReadinessSnapshot(HelixManager helixManager, String tableNameWithType,
      Predicate<String> isLocallyReady) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    IdealState idealState = dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates(tableNameWithType));
    if (idealState == null) {
      LOGGER.warn("Failed to find ideal state for table: {}", tableNameWithType);
      return new ReadinessSnapshot(Set.of(), Map.of(), Map.of());
    }
    String instanceName = helixManager.getInstanceName();
    CurrentState currentState = getCurrentState(helixManager, tableNameWithType);
    Set<String> consumingSegments = new HashSet<>();
    Map<String, String> unloadedSegments = new HashMap<>();
    Map<String, Integer> metadataPartitionIds = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segmentName = entry.getKey();
      String state = entry.getValue().get(instanceName);
      if (SegmentStateModel.CONSUMING.equals(state)) {
        consumingSegments.add(segmentName);
      } else if (SegmentStateModel.ONLINE.equals(state)) {
        String actualState = currentState != null ? currentState.getState(segmentName) : null;
        if (!SegmentStateModel.ONLINE.equals(actualState)) {
          unloadedSegments.put(segmentName, actualState);
          if (SegmentUtils.getPartitionIdFromSegmentName(segmentName) == null && !isLocallyReady.test(segmentName)) {
            Integer partitionId = getPartitionIdFromMetadata(helixManager, tableNameWithType, segmentName);
            if (partitionId != null) {
              metadataPartitionIds.put(segmentName, partitionId);
            }
          }
        }
      }
    }
    return new ReadinessSnapshot(consumingSegments, unloadedSegments, metadataPartitionIds);
  }

  /// An immutable view of the segments readiness evaluation reads, safe to share between consumer threads: this
  /// instance's CONSUMING segments, its ONLINE segments that were not ONLINE in the captured current state, and the
  /// metadata-derived partition ids of the latter. The collections are built for this snapshot and never modified.
  /// Evaluation performs no ZooKeeper reads. It rechecks local readiness for the unloaded segments, then determines
  /// whether any of them is required history. The predicate must itself be safe for its caller's threads. Its results
  /// are never retained here; an omitted or unresolved partition id is treated conservatively if the segment is no
  /// longer locally ready.
  public static final class ReadinessSnapshot {
    private final Set<String> _consumingSegments;
    // ONLINE in the ideal state but not in the captured current state, mapped to that state (null when not reported).
    private final Map<String, String> _unloadedSegments;
    // Only known ids for custom-named segments that could block when captured. Absence means unknown, not ready.
    private final Map<String, Integer> _metadataPartitionIds;

    private ReadinessSnapshot(Set<String> consumingSegments, Map<String, String> unloadedSegments,
        Map<String, Integer> metadataPartitionIds) {
      _consumingSegments = consumingSegments;
      _unloadedSegments = unloadedSegments;
      _metadataPartitionIds = metadataPartitionIds;
    }

    public boolean isReadyToConsume(LLCSegmentName consumer, Predicate<String> isLocallyReady) {
      return isReadyToConsume(consumer, isLocallyReady, false);
    }

    /// Optionally logs the first unavailable history segment. Enable logging for a freshly fetched snapshot, keeping
    /// repeated rejection checks against a cached snapshot quiet.
    public boolean isReadyToConsume(LLCSegmentName consumer, Predicate<String> isLocallyReady,
        boolean logUnavailableSegments) {
      // The consumer must still be assigned to this instance, as CONSUMING or as ONLINE with its transition pending.
      String consumerName = consumer.getSegmentName();
      if (!_consumingSegments.contains(consumerName) && !_unloadedSegments.containsKey(consumerName)) {
        return false;
      }
      for (Map.Entry<String, String> entry : _unloadedSegments.entrySet()) {
        String segmentName = entry.getKey();
        if (!isLocallyReady.test(segmentName) && isRequiredHistory(segmentName, consumer)) {
          if (logUnavailableSegments) {
            String actualState = entry.getValue();
            if (SegmentStateModel.ERROR.equals(actualState)) {
              LOGGER.error("Consumer: {} is waiting for history segment: {} in ERROR state, expected: {}",
                  consumerName, segmentName, SegmentStateModel.ONLINE);
            } else {
              LOGGER.info("Consumer: {} is waiting for history segment: {} in state: {}, expected: {}", consumerName,
                  segmentName, actualState, SegmentStateModel.ONLINE);
            }
          }
          return false;
        }
      }
      return true;
    }

    private boolean isRequiredHistory(String segmentName, LLCSegmentName consumer) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        return llcSegmentName.getPartitionGroupId() == consumer.getPartitionGroupId()
            && llcSegmentName.getSequenceNumber() < consumer.getSequenceNumber();
      }
      Integer partitionId = SegmentUtils.getPartitionIdFromSegmentName(segmentName);
      if (partitionId == null) {
        partitionId = _metadataPartitionIds.get(segmentName);
      }
      return partitionId == null || partitionId == consumer.getPartitionGroupId();
    }
  }

  /// Keep partition interpretation in SegmentUtils, as in upsert/dedup loading. A missing metadata record can race
  /// with assignment changes, so leave its partition unknown instead of failing the consumer's readiness check.
  @Nullable
  private static Integer getPartitionIdFromMetadata(HelixManager helixManager, String tableNameWithType,
      String segmentName) {
    SegmentZKMetadata metadata =
        ZKMetadataProvider.getSegmentZKMetadata(helixManager.getHelixPropertyStore(), tableNameWithType, segmentName);
    return metadata != null ? SegmentUtils.getSegmentPartitionId(metadata, null) : null;
  }

  /// Returns this live instance's current state for the table, or null if it is not available, in which case no
  /// segment counts as loaded.
  @Nullable
  private static CurrentState getCurrentState(HelixManager helixManager, String tableNameWithType) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    String instanceName = helixManager.getInstanceName();
    LiveInstance liveInstance = dataAccessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      LOGGER.warn("Failed to find live instance for instance: {}", instanceName);
      return null;
    }
    String sessionId = liveInstance.getEphemeralOwner();
    CurrentState currentState =
        dataAccessor.getProperty(keyBuilder.currentState(instanceName, sessionId, tableNameWithType));
    if (currentState == null) {
      LOGGER.warn("Failed to find current state for instance: {}, sessionId: {}, table: {}", instanceName, sessionId,
          tableNameWithType);
    }
    return currentState;
  }
}
