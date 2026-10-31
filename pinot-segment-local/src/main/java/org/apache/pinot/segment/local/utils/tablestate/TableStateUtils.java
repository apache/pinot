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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableStateUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStateUtils.class);
  private static final int MAX_NUM_SEGMENTS_TO_LOG = 10;

  private TableStateUtils() {
  }

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

  /// Checks if all segments for the given @param tableNameWithType were successfully loaded
  /// This function will get all segments in IDEALSTATE and CURRENTSTATE for the given table,
  /// and then check if all ONLINE segments in IDEALSTATE match with CURRENTSTATE.
  /// @param helixManager helix manager for the server instance
  /// @param tableNameWithType table name for which segment state is to be checked
  /// @return true if all segments for the given table are successfully loaded. False otherwise
  public static boolean isAllSegmentsLoaded(HelixManager helixManager, String tableNameWithType) {
    List<String> onlineSegments =
        getSegmentsInGivenStateForThisInstance(helixManager, tableNameWithType, SegmentStateModel.ONLINE);
    return areSegmentsLoaded(helixManager, tableNameWithType, onlineSegments);
  }

  /// Checks the current assignment before a consumer starts. Earlier ONLINE segments of the same partition must be
  /// loaded, as must uploaded segments from that partition. Segments whose partition cannot be inferred from their
  /// name are conservatively included. The consumer and its successors are excluded to avoid waiting on themselves.
  ///
  /// @param isLocallyReady identifies segments whose metadata is already ready locally despite a pending ONLINE
  ///                      transition, such as a predecessor that finished consumption and released its permit
  public static boolean isReadyToConsume(HelixManager helixManager, String tableNameWithType, LLCSegmentName consumer,
      Predicate<String> isLocallyReady) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    IdealState idealState = dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates(tableNameWithType));
    if (idealState == null) {
      LOGGER.warn("Failed to find ideal state for table: {}", tableNameWithType);
      return false;
    }
    String instanceName = helixManager.getInstanceName();
    Map<String, String> consumerAssignment = idealState.getInstanceStateMap(consumer.getSegmentName());
    String consumerState = consumerAssignment != null ? consumerAssignment.get(instanceName) : null;
    if (!SegmentStateModel.CONSUMING.equals(consumerState) && !SegmentStateModel.ONLINE.equals(consumerState)) {
      return false;
    }

    List<String> requiredSegments = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      if (!SegmentStateModel.ONLINE.equals(entry.getValue().get(instanceName))) {
        continue;
      }
      String segmentName = entry.getKey();
      if (isRequiredHistory(segmentName, consumer) && !isLocallyReady.test(segmentName)) {
        requiredSegments.add(segmentName);
      }
    }
    return areSegmentsLoaded(helixManager, tableNameWithType, requiredSegments);
  }

  /// Reads the table assignment and this live instance's current state once for evaluation by multiple consumers.
  /// The records are copied so subsequent Helix updates cannot mutate the returned snapshot. A cached snapshot must
  /// only reject readiness; callers must perform a fresh check before authorizing consumption from a cached result.
  public static ReadinessSnapshot getReadinessSnapshot(HelixManager helixManager, String tableNameWithType) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    IdealState idealState = dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates(tableNameWithType));
    if (idealState == null) {
      LOGGER.warn("Failed to find ideal state for table: {}", tableNameWithType);
      return new ReadinessSnapshot(Map.of(), Map.of());
    }
    String instanceName = helixManager.getInstanceName();
    Map<String, String> idealStates = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String state = entry.getValue().get(instanceName);
      if (state != null) {
        idealStates.put(entry.getKey(), state);
      }
    }
    return new ReadinessSnapshot(idealStates, getCurrentStates(helixManager, tableNameWithType));
  }

  /// An immutable view of this instance's assignment and current state, safe to share between consumer threads.
  /// Evaluation performs no ZooKeeper reads and reevaluates the supplied local-readiness predicate for each required
  /// segment. The predicate must itself be safe for its caller's threads; its results are never retained here.
  public static final class ReadinessSnapshot {
    private final Map<String, String> _idealStates;
    private final Map<String, String> _currentStates;

    private ReadinessSnapshot(Map<String, String> idealStates, Map<String, String> currentStates) {
      _idealStates = Collections.unmodifiableMap(new HashMap<>(idealStates));
      _currentStates = Collections.unmodifiableMap(new HashMap<>(currentStates));
    }

    public boolean isReadyToConsume(LLCSegmentName consumer, Predicate<String> isLocallyReady) {
      return isReadyToConsume(consumer, isLocallyReady, false);
    }

    /// Optionally logs the first unavailable history segment. Enable logging for a freshly fetched snapshot, keeping
    /// repeated rejection checks against a cached snapshot quiet.
    public boolean isReadyToConsume(LLCSegmentName consumer, Predicate<String> isLocallyReady,
        boolean logUnavailableSegments) {
      String consumerState = _idealStates.get(consumer.getSegmentName());
      if (!SegmentStateModel.CONSUMING.equals(consumerState) && !SegmentStateModel.ONLINE.equals(consumerState)) {
        return false;
      }
      for (Map.Entry<String, String> entry : _idealStates.entrySet()) {
        String segmentName = entry.getKey();
        if (SegmentStateModel.ONLINE.equals(entry.getValue()) && isRequiredHistory(segmentName, consumer)
            && !isLocallyReady.test(segmentName)) {
          String actualState = _currentStates.get(segmentName);
          if (!SegmentStateModel.ONLINE.equals(actualState)) {
            if (logUnavailableSegments) {
              if (SegmentStateModel.ERROR.equals(actualState)) {
                LOGGER.error("Consumer: {} is waiting for history segment: {} in ERROR state, expected: {}",
                    consumer.getSegmentName(), segmentName, SegmentStateModel.ONLINE);
              } else {
                LOGGER.info("Consumer: {} is waiting for history segment: {} in state: {}, expected: {}",
                    consumer.getSegmentName(), segmentName, actualState, SegmentStateModel.ONLINE);
              }
            }
            return false;
          }
        }
      }
      return true;
    }
  }

  private static boolean isRequiredHistory(String segmentName, LLCSegmentName consumer) {
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    if (llcSegmentName != null) {
      return llcSegmentName.getPartitionGroupId() == consumer.getPartitionGroupId()
          && llcSegmentName.getSequenceNumber() < consumer.getSequenceNumber();
    }
    Integer partitionId = SegmentUtils.getPartitionIdFromSegmentName(segmentName);
    return partitionId == null || partitionId == consumer.getPartitionGroupId();
  }

  private static boolean areSegmentsLoaded(HelixManager helixManager, String tableNameWithType,
      List<String> onlineSegments) {
    if (onlineSegments.isEmpty()) {
      LOGGER.info("No required ONLINE segment found for table: {}", tableNameWithType);
      return true;
    }

    Map<String, String> currentStateMap = getCurrentStates(helixManager, tableNameWithType);
    List<String> unloadedSegments = new ArrayList<>();
    for (String segmentName : onlineSegments) {
      String actualState = currentStateMap.get(segmentName);
      if (!SegmentStateModel.ONLINE.equals(actualState)) {
        if (SegmentStateModel.ERROR.equals(actualState)) {
          LOGGER.error("Found segment: {}, table: {} in ERROR state, expected: {}", segmentName, tableNameWithType,
              SegmentStateModel.ONLINE);
          return false;
        } else {
          unloadedSegments.add(segmentName);
        }
      }
    }
    if (unloadedSegments.isEmpty()) {
      LOGGER.info("All required segments loaded for table: {}", tableNameWithType);
      return true;
    } else {
      int numUnloadedSegments = unloadedSegments.size();
      if (numUnloadedSegments <= MAX_NUM_SEGMENTS_TO_LOG) {
        LOGGER.info("Found {} unloaded segments: {} for table: {}", numUnloadedSegments, unloadedSegments,
            tableNameWithType);
      } else {
        LOGGER.info("Found {} unloaded segments: {}... for table: {}", numUnloadedSegments,
            unloadedSegments.subList(0, MAX_NUM_SEGMENTS_TO_LOG), tableNameWithType);
      }
      return false;
    }
  }

  private static Map<String, String> getCurrentStates(HelixManager helixManager, String tableNameWithType) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    String instanceName = helixManager.getInstanceName();
    LiveInstance liveInstance = dataAccessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      LOGGER.warn("Failed to find live instance for instance: {}", instanceName);
      return Map.of();
    }
    String sessionId = liveInstance.getEphemeralOwner();
    CurrentState currentState =
        dataAccessor.getProperty(keyBuilder.currentState(instanceName, sessionId, tableNameWithType));
    if (currentState == null) {
      LOGGER.warn("Failed to find current state for instance: {}, sessionId: {}, table: {}", instanceName, sessionId,
          tableNameWithType);
      return Map.of();
    }
    return currentState.getPartitionStateMap();
  }
}
