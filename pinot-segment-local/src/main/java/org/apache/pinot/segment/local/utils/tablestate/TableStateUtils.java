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
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableStateUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStateUtils.class);
  private static final int MAX_NUM_SEGMENTS_TO_LOG = 10;

  private TableStateUtils() {
  }

  /**
   * Returns all segments in a given state for a given table.
   *
   * @param helixManager instance of Helix manager
   * @param tableNameWithType table for which we are obtaining segments in a given state
   * @param state state of the segments to be returned
   *
   * @return List of segment names in a given state.
   */
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

  /**
   * Checks if all segments for the given @param tableNameWithType were succesfully loaded
   * This function will get all segments in IDEALSTATE and CURRENTSTATE for the given table,
   * and then check if all ONLINE segments in IDEALSTATE match with CURRENTSTATE.
   * @param helixManager helix manager for the server instance
   * @param tableNameWithType table name for which segment state is to be checked
   * @return true if all segments for the given table are succesfully loaded. False otherwise
   */
  public static boolean isAllSegmentsLoaded(HelixManager helixManager, String tableNameWithType) {
    List<String> onlineSegments =
        getSegmentsInGivenStateForThisInstance(helixManager, tableNameWithType, SegmentStateModel.ONLINE);
    if (onlineSegments.isEmpty()) {
      LOGGER.info("No ONLINE segment found for table: {}", tableNameWithType);
      return true;
    }

    // Check if ideal state and current state matches for all segments assigned to the current instance
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    String instanceName = helixManager.getInstanceName();
    LiveInstance liveInstance = dataAccessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      LOGGER.warn("Failed to find live instance for instance: {}", instanceName);
      return false;
    }
    String sessionId = liveInstance.getEphemeralOwner();
    CurrentState currentState =
        dataAccessor.getProperty(keyBuilder.currentState(instanceName, sessionId, tableNameWithType));
    if (currentState == null) {
      LOGGER.warn("Failed to find current state for instance: {}, sessionId: {}, table: {}", instanceName, sessionId,
          tableNameWithType);
      return false;
    }
    List<String> unloadedSegments = new ArrayList<>();
    Map<String, String> currentStateMap = currentState.getPartitionStateMap();
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
      LOGGER.info("All segments loaded for table: {}", tableNameWithType);
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
}
