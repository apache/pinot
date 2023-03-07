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
package org.apache.pinot.broker.routing.instanceselector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentStateSnapshot {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStateSnapshot.class);

  private static class SelectionCandidate {
    private Map<String, List<Pair<String, Boolean>>> _instanceMap;
    private Set<String> _unavailableSegments;

    public SelectionCandidate(Map<String, List<Pair<String, Boolean>>> instanceMap,
        @Nullable Set<String> unavailableSegments) {
      _instanceMap = instanceMap;
      _unavailableSegments = unavailableSegments;
    }

    public List<Pair<String, Boolean>> getCandidates(String segment) {
      return _instanceMap.getOrDefault(segment, null);
    }

    Map<String, List<Pair<String, Boolean>>> getAllCandidates() {
      return _instanceMap;
    }

    public Set<String> getUnavailableSegments() {
      return _unavailableSegments;
    }
  }

  private final SelectionCandidate _oldSegmentSelectionCandidate;
  private final SelectionCandidate _newSegmentSelectionCandidate;

  private SegmentStateSnapshot(SelectionCandidate oldSegmentSelectionCandidate,
      SelectionCandidate newSegmentSelectionCandidate) {
    _oldSegmentSelectionCandidate = oldSegmentSelectionCandidate;
    _newSegmentSelectionCandidate = newSegmentSelectionCandidate;
  }

  public static SegmentStateSnapshot createSnapshot(String tableNameWithType,
      Map<String, List<String>> segmentToOnlineInstancesMap,
      Map<String, BaseInstanceSelector.SegmentState> newSegmentState, Set<String> enabledInstance,
      BrokerMetrics brokerMetrics) {
    SelectionCandidate oldSelectionCandidate =
        calculateOldSegmentSelectionCandidate(tableNameWithType, segmentToOnlineInstancesMap, enabledInstance,
            brokerMetrics);
    SelectionCandidate newSelectionCandidate = calculateNewSegmentSelectionCandidate(newSegmentState, enabledInstance);
    SegmentStateSnapshot snapshot = new SegmentStateSnapshot(oldSelectionCandidate, newSelectionCandidate);
    return snapshot;
  }

  private static SelectionCandidate calculateOldSegmentSelectionCandidate(String tableNameWithType,
      Map<String, List<String>> segmentToOnlineInstancesMap, Set<String> enabledInstance, BrokerMetrics brokerMetrics) {
    // Generate a new map from segment to enabled ONLINE/CONSUMING instances and a new set of unavailable segments (no
    // enabled instance or all enabled instances are in ERROR state)
    Map<String, List<Pair<String, Boolean>>> segmentToEnabledInstancesMap = new HashMap<>();
    Set<String> unavailableSegments = new HashSet<>();
    // NOTE: Put null as the value when there is no enabled instances for a segment so that segmentToEnabledInstancesMap
    // always contains all segments. With this, in onInstancesChange() we can directly iterate over
    // segmentToEnabledInstancesMap.entrySet() and modify the value without changing the map entries.
    for (Map.Entry<String, List<String>> entry : segmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      List<String> onlineInstancesForSegment = entry.getValue();
      List<Pair<String, Boolean>> enabledInstancesForSegment = new ArrayList<>();
      List<Boolean> onlineFlagsForSegment = new ArrayList<>();
      for (String onlineInstance : onlineInstancesForSegment) {
        if (enabledInstance.contains(onlineInstance)) {
          enabledInstancesForSegment.add(ImmutablePair.of(onlineInstance, true));
          onlineFlagsForSegment.add(true);
        }
      }
      if (!enabledInstancesForSegment.isEmpty()) {
        segmentToEnabledInstancesMap.put(segment, enabledInstancesForSegment);
        continue;
      }
      LOGGER.warn(
          "Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} counting"
              + " segment as unavailable)", segment, tableNameWithType, onlineInstancesForSegment);
      unavailableSegments.add(segment);
      brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
      // TODO: check whether we need to put null.
      segmentToEnabledInstancesMap.put(segment, null);
    }
    return new SelectionCandidate(segmentToEnabledInstancesMap, unavailableSegments);
  }

  private static SelectionCandidate calculateNewSegmentSelectionCandidate(
      Map<String, BaseInstanceSelector.SegmentState> newSegmentState, Set<String> enabledInstance) {
    Map<String, List<Pair<String, Boolean>>> newSegmentToCandidateInstanceMap = new HashMap<>();
    for (Map.Entry<String, BaseInstanceSelector.SegmentState> entry : newSegmentState.entrySet()) {
      String segment = entry.getKey();
      List<Pair<String, Boolean>> enabledInstancesForSegment = new ArrayList<>();
      BaseInstanceSelector.SegmentState state = entry.getValue();
      HashMap<String, Boolean> candidates = state.getCandidates();
      for (Map.Entry<String, Boolean> instanceEntry : candidates.entrySet()) {
        String instance = instanceEntry.getKey();
        if (enabledInstance.contains(instance)) {
          enabledInstancesForSegment.add(ImmutablePair.of(instance, instanceEntry.getValue()));
        }
      }
      if (!enabledInstancesForSegment.isEmpty()) {
        newSegmentToCandidateInstanceMap.put(segment, enabledInstancesForSegment);
      } else {
        newSegmentToCandidateInstanceMap.put(segment, null);
      }
    }
    return new SelectionCandidate(newSegmentToCandidateInstanceMap, null);
  }

  public List<Pair<String, Boolean>> getCandidates(String segment) {
    List<Pair<String, Boolean>> candidates = _newSegmentSelectionCandidate.getCandidates(segment);
    if (candidates != null) {
      return candidates;
    }
    return _oldSegmentSelectionCandidate.getCandidates(segment);
  }

  public Map<String, List<Pair<String, Boolean>>> getOldSegmentCandidates() {
    return _oldSegmentSelectionCandidate.getAllCandidates();
  }

  public Set<String> getUnavailableSegments() {
    return _oldSegmentSelectionCandidate.getUnavailableSegments();
  }
}
