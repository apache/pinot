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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  This class represents a snapshot state of segments used for routing purpose.
 *  Note that this class is immutable after creation.
 *
 *  For old segments, we return a list of online instances with online flags set to true.
 *  For old segments without any online instances, we report them as unavailable segments.
 *
 *  For new segments, we return a list of candidate instance with online flags to indicate whether the instance is
 *  online or not.
 *  We don't report new segment as unavailable segments because it is valid for new segments to be not online at all.
 */
public class SegmentStateSnapshot {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStateSnapshot.class);

  private static class SelectionCandidate {
    // Mapping from segment to list of servers with online flags.
    private Map<String, List<SegmentInstanceCandidate>> _instanceMap;
    private Set<String> _unavailableSegments;

    public SelectionCandidate(Map<String, List<SegmentInstanceCandidate>> instanceMap,
        Set<String> unavailableSegments) {
      _instanceMap = instanceMap;
      _unavailableSegments = unavailableSegments;
    }

    public List<SegmentInstanceCandidate> getCandidates(String segment) {
      return _instanceMap.get(segment);
    }

    Map<String, List<SegmentInstanceCandidate>> getAllCandidates() {
      return _instanceMap;
    }

    public Set<String> getUnavailableSegments() {
      return _unavailableSegments;
    }
  }

  private final SelectionCandidate _oldSegmentSelectionCandidate;
  // New segment doesn't have unavailable segments reported.
  private final SelectionCandidate _newSegmentSelectionCandidate;

  private SegmentStateSnapshot(SelectionCandidate oldSegmentSelectionCandidate,
      SelectionCandidate newSegmentSelectionCandidate) {
    _oldSegmentSelectionCandidate = oldSegmentSelectionCandidate;
    _newSegmentSelectionCandidate = newSegmentSelectionCandidate;
  }

  // Create a segment state snapshot based on some in-memory states to be used for routing.
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

  // Calculate online instance map for routing and unavailable segments from old segment states.
  private static SelectionCandidate calculateOldSegmentSelectionCandidate(String tableNameWithType,
      Map<String, List<String>> segmentToOnlineInstancesMap, Set<String> enabledInstance, BrokerMetrics brokerMetrics) {
    // Generate a new map from segment to enabled ONLINE/CONSUMING instances and a new set of unavailable segments (no
    // enabled instance or all enabled instances are in ERROR state)
    Map<String, List<SegmentInstanceCandidate>> segmentToEnabledInstancesMap = new HashMap<>();
    Set<String> unavailableSegments = new HashSet<>();
    for (Map.Entry<String, List<String>> entry : segmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      List<String> onlineInstancesForSegment = entry.getValue();
      List<SegmentInstanceCandidate> enabledInstancesForSegment = new ArrayList<>();
      for (String onlineInstance : onlineInstancesForSegment) {
        if (enabledInstance.contains(onlineInstance)) {
          enabledInstancesForSegment.add(SegmentInstanceCandidate.of(onlineInstance, true));
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
    }
    return new SelectionCandidate(segmentToEnabledInstancesMap, unavailableSegments);
  }

  // Calculate candidate instance map for routing from new segment states.
  // Note that we don't report new segment as unavailable.
  private static SelectionCandidate calculateNewSegmentSelectionCandidate(
      Map<String, BaseInstanceSelector.SegmentState> newSegmentState, Set<String> enabledInstance) {
    Map<String, List<SegmentInstanceCandidate>> newSegmentToCandidateInstanceMap = new HashMap<>();
    for (Map.Entry<String, BaseInstanceSelector.SegmentState> entry : newSegmentState.entrySet()) {
      String segment = entry.getKey();
      List<SegmentInstanceCandidate> enabledInstancesForSegment = new ArrayList<>();
      BaseInstanceSelector.SegmentState state = entry.getValue();
      HashMap<String, Boolean> candidates = state.getCandidates();
      for (Map.Entry<String, Boolean> instanceEntry : candidates.entrySet()) {
        String instance = instanceEntry.getKey();
        if (enabledInstance.contains(instance)) {
          enabledInstancesForSegment.add(SegmentInstanceCandidate.of(instance, instanceEntry.getValue()));
        }
      }
      if (!enabledInstancesForSegment.isEmpty()) {
        newSegmentToCandidateInstanceMap.put(segment, enabledInstancesForSegment);
      }
    }
    return new SelectionCandidate(newSegmentToCandidateInstanceMap, Collections.emptySet());
  }

  @Nullable
  public List<SegmentInstanceCandidate> getCandidates(String segment) {
    List<SegmentInstanceCandidate> candidates = _newSegmentSelectionCandidate.getCandidates(segment);
    if (candidates != null) {
      return candidates;
    }
    return _oldSegmentSelectionCandidate.getCandidates(segment);
  }

  @Nullable
  public Map<String, Boolean> getCandidatesAsMap(String segment) {
    List<SegmentInstanceCandidate> candidates = getCandidates(segment);
    if (candidates == null) {
      return null;
    }
    Map<String, Boolean> candidateMap = new HashMap<>();
    for (SegmentInstanceCandidate instanceCandidate : candidates) {
      candidateMap.put(instanceCandidate.getInstance(), instanceCandidate.isOnline());
    }
    return candidateMap;
  }

  public Set<String> getUnavailableSegments() {
    return _oldSegmentSelectionCandidate.getUnavailableSegments();
  }
}
