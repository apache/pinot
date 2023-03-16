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
import java.util.TreeMap;
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

  private Map<String, List<SegmentInstanceCandidate>> _segmentCandidates;
  private Set<String> _unavailableSegments;

  // Create a segment state snapshot based on some in-memory states to be used for routing.
  public static SegmentStateSnapshot createSnapshot(String tableNameWithType, Map<String, SegmentState> oldSegmentState,
      Map<String, SegmentState> newSegmentState, Set<String> enabledInstance, BrokerMetrics brokerMetrics) {
    Map<String, List<SegmentInstanceCandidate>> segmentCandidates = new HashMap<>();
    Set<String> unavailableSegments = new HashSet<>();
    calculateSegmentSelectionCandidate(tableNameWithType, oldSegmentState, enabledInstance, brokerMetrics, true,
        segmentCandidates, unavailableSegments);
    // Note that we don't report new segment as unavailable.
    calculateSegmentSelectionCandidate(tableNameWithType, newSegmentState, enabledInstance, brokerMetrics, false,
        segmentCandidates, unavailableSegments);
    SegmentStateSnapshot snapshot = new SegmentStateSnapshot(segmentCandidates, unavailableSegments);
    return snapshot;
  }

  private SegmentStateSnapshot(Map<String, List<SegmentInstanceCandidate>> segmentCandidates,
      Set<String> unavailableSegments) {
    _segmentCandidates = segmentCandidates;
    _unavailableSegments = unavailableSegments;
  }

  // Calculate online instance map for routing and unavailable segments from old segment states.
  private static void calculateSegmentSelectionCandidate(String tableNameWithType,
      Map<String, SegmentState> oldSegmentState, Set<String> enabledInstance, BrokerMetrics brokerMetrics,
      boolean shouldReportUnavailableSegments, Map<String, List<SegmentInstanceCandidate>> candidates,
      Set<String> unavailableSegments) {
    // Generate a new map from segment to enabled ONLINE/CONSUMING instances and a new set of unavailable segments (no
    // enabled instance or all enabled instances are in ERROR state)
    Map<String, List<SegmentInstanceCandidate>> segmentToEnabledInstancesMap = new HashMap<>();
    for (Map.Entry<String, SegmentState> entry : oldSegmentState.entrySet()) {
      String segment = entry.getKey();
      SegmentState segmentState = entry.getValue();
      List<SegmentInstanceCandidate> enabledInstancesCandidates = new ArrayList<>();
      for (SegmentInstanceCandidate candidate : segmentState.getCandidates()) {
        if (enabledInstance.contains(candidate.getInstance())) {
          enabledInstancesCandidates.add(candidate);
        }
      }
      if (!enabledInstancesCandidates.isEmpty()) {
        candidates.put(segment, enabledInstancesCandidates);
      } else if (shouldReportUnavailableSegments) {
        LOGGER.warn(
            "Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} counting"
                + " segment as unavailable)", segment, tableNameWithType, segmentState);
        unavailableSegments.add(segment);
        brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
      }
    }
  }

  @Nullable
  public List<SegmentInstanceCandidate> getCandidates(String segment) {
    return _segmentCandidates.get(segment);
  }

  @Nullable
  public TreeMap<String, Boolean> getCandidatesAsMap(String segment) {
    List<SegmentInstanceCandidate> candidates = getCandidates(segment);
    if (candidates == null) {
      return null;
    }
    TreeMap<String, Boolean> candidateMap = new TreeMap<>();
    for (SegmentInstanceCandidate instanceCandidate : candidates) {
      candidateMap.put(instanceCandidate.getInstance(), instanceCandidate.isOnline());
    }
    return candidateMap;
  }

  public Set<String> getUnavailableSegments() {
    return _unavailableSegments;
  }
}
