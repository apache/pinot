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

import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableStateUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStateUtils.class);

  private TableStateUtils() {
  }

  public static boolean isAllSegmentsLoaded(HelixManager helixManager, String tableNameWithType) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    IdealState idealState = dataAccessor.getProperty(keyBuilder.idealStates(tableNameWithType));
    if (idealState == null) {
      LOGGER.warn("Failed to find ideal state for table: {}", tableNameWithType);
      return false;
    }
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

    // Check if ideal state and current state matches for all segments assigned to the current instance
    Map<String, Map<String, String>> idealStatesMap = idealState.getRecord().getMapFields();
    Map<String, String> currentStateMap = currentState.getPartitionStateMap();
    for (Map.Entry<String, Map<String, String>> entry : idealStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      String expectedState = instanceStateMap.get(instanceName);
      // Only track ONLINE segments assigned to the current instance
      if (!CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(expectedState)) {
        continue;
      }
      String actualState = currentStateMap.get(segmentName);
      if (!CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(actualState)) {
        if (CommonConstants.Helix.StateModel.SegmentStateModel.ERROR.equals(actualState)) {
          LOGGER
              .error("Find ERROR segment: {}, table: {}, expected: {}", segmentName, tableNameWithType, expectedState);
        } else {
          LOGGER.info("Find unloaded segment: {}, table: {}, expected: {}, actual: {}", segmentName, tableNameWithType,
              expectedState, actualState);
        }
        return false;
      }
    }

    LOGGER.info("All segments loaded for table: {}", tableNameWithType);
    return true;
  }
}
