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

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InstanceSelectorUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceSelectorUtils.class);
  public static final long NEW_SEGMENT_EXPIRATION_MILLIS = TimeUnit.MINUTES.toMillis(5);

  private InstanceSelectorUtils() {
  }

  public static boolean isNewSegment(long creationTimeMs, long currentTimeMs) {
    return creationTimeMs > 0 && currentTimeMs - creationTimeMs <= NEW_SEGMENT_EXPIRATION_MILLIS;
  }

  /**
   * Returns whether the instance state is online for routing purpose (ONLINE/CONSUMING).
   */
  public static boolean isOnlineForRouting(@Nullable String state) {
    return CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)
        || CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING.equals(state);
  }

  public static List<SegmentInstanceCandidate> getEnabledCandidatesAndAddToServingInstances(
      List<SegmentInstanceCandidate> candidates, Set<String> enabledInstances, Set<String> servingInstances) {
    List<SegmentInstanceCandidate> enabledCandidates = new ArrayList<>(candidates.size());
    for (SegmentInstanceCandidate candidate : candidates) {
      String instance = candidate.getInstance();
      if (enabledInstances.contains(instance)) {
        enabledCandidates.add(candidate);
        servingInstances.add(instance);
      }
    }
    return enabledCandidates;
  }

  /**
   * Returns the online instances for routing purpose.
   */
  public static TreeSet<String> getOnlineInstances(Map<String, String> idealStateInstanceStateMap,
      Map<String, String> externalViewInstanceStateMap) {
    TreeSet<String> onlineInstances = new TreeSet<>();
    // Only track ONLINE/CONSUMING instances within the ideal state
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      String instance = entry.getKey();
      // NOTE: DO NOT check if EV matches IS because it is a valid state when EV is CONSUMING while IS is ONLINE
      if (InstanceSelectorUtils.isOnlineForRouting(entry.getValue()) && InstanceSelectorUtils.isOnlineForRouting(
          externalViewInstanceStateMap.get(instance))) {
        onlineInstances.add(instance);
      }
    }
    return onlineInstances;
  }

  /**
   * Returns a map from new segment to their creation time based on the ZK metadata.
   */
  public static Map<String, Long> getNewSegmentCreationTimeMapFromZK(String tableNameWithType, IdealState idealState,
      ExternalView externalView, Set<String> onlineSegments, ZkHelixPropertyStore<ZNRecord> propertyStore,
      Clock clock) {
    List<String> potentialNewSegments = new ArrayList<>();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      assert idealStateAssignment.containsKey(segment);
      if (isPotentialNewSegment(idealStateAssignment.get(segment), externalViewAssignment.get(segment))) {
        potentialNewSegments.add(segment);
      }
    }
    Map<String, Long> newSegmentCreationTimeMap = new HashMap<>();
    long currentTimeMs = clock.millis();
    String segmentZKMetadataPathPrefix =
        ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
    List<String> segmentZKMetadataPaths = new ArrayList<>(potentialNewSegments.size());
    for (String segment : potentialNewSegments) {
      segmentZKMetadataPaths.add(segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (ZNRecord record : znRecords) {
      if (record == null) {
        continue;
      }
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(record);
      long creationTimeMs = SegmentUtils.getSegmentCreationTimeMs(segmentZKMetadata);
      if (isNewSegment(creationTimeMs, currentTimeMs)) {
        newSegmentCreationTimeMap.put(segmentZKMetadata.getSegmentName(), creationTimeMs);
      }
    }
    LOGGER.info("Got {} new segments: {} for table: {} by reading ZK metadata, current time: {}",
        newSegmentCreationTimeMap.size(), newSegmentCreationTimeMap, tableNameWithType, currentTimeMs);
    return newSegmentCreationTimeMap;
  }

  /**
   * Returns a map from new segment to their creation time based on the existing in-memory states.
   */
  public static Map<String, Long> getNewSegmentCreationTimeMapFromExistingStates(String tableNameWithType,
      IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, NewSegmentState> newSegmentStateMap,
      Map<String, List<SegmentInstanceCandidate>> oldSegmentCandidatesMap, Clock clock) {
    Map<String, Long> newSegmentCreationTimeMap = new HashMap<>();
    long currentTimeMs = clock.millis();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      NewSegmentState newSegmentState = newSegmentStateMap.get(segment);
      long creationTimeMs = 0;
      if (newSegmentState != null) {
        // It was a new segment before, check the creation time and segment state to see if it is still a new segment
        if (InstanceSelectorUtils.isNewSegment(newSegmentState.getCreationTimeMs(), currentTimeMs)) {
          creationTimeMs = newSegmentState.getCreationTimeMs();
        }
      } else if (!oldSegmentCandidatesMap.containsKey(segment)) {
        // This is the first time we see this segment, use the current time as the creation time
        creationTimeMs = currentTimeMs;
      }
      // For recently created segment, check if it is qualified as new segment
      if (creationTimeMs > 0) {
        assert idealStateAssignment.containsKey(segment);
        if (InstanceSelectorUtils.isPotentialNewSegment(idealStateAssignment.get(segment),
            externalViewAssignment.get(segment))) {
          newSegmentCreationTimeMap.put(segment, creationTimeMs);
        }
      }
    }
    LOGGER.info("Got {} new segments: {} for table: {} by processing existing states, current time: {}",
        newSegmentCreationTimeMap.size(), newSegmentCreationTimeMap, tableNameWithType, currentTimeMs);
    return newSegmentCreationTimeMap;
  }

  /**
   * Returns whether a segment is qualified as a new segment.
   * A segment is count as old when:
   * - Any instance for the segment is in ERROR state
   * - External view for the segment converges with ideal state
   */
  public static boolean isPotentialNewSegment(Map<String, String> idealStateInstanceStateMap,
      @Nullable Map<String, String> externalViewInstanceStateMap) {
    if (externalViewInstanceStateMap == null) {
      return true;
    }
    boolean hasConverged = true;
    // Only track ONLINE/CONSUMING instances within the ideal state
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      if (isOnlineForRouting(entry.getValue())) {
        String externalViewState = externalViewInstanceStateMap.get(entry.getKey());
        if (externalViewState == null || externalViewState.equals(
            CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE)) {
          hasConverged = false;
        } else if (externalViewState.equals(CommonConstants.Helix.StateModel.SegmentStateModel.ERROR)) {
          return false;
        }
      }
    }
    return !hasConverged;
  }

  public static List<SegmentInstanceCandidate> getCandidatesForNewSegment(
      Map<String, String> idealStateInstanceStateMap, Predicate<String> onlineChecker) {
    List<SegmentInstanceCandidate> candidates = new ArrayList<>(idealStateInstanceStateMap.size());
    for (Map.Entry<String, String> entry : convertToSortedMap(idealStateInstanceStateMap).entrySet()) {
      if (isOnlineForRouting(entry.getValue())) {
        String instance = entry.getKey();
        candidates.add(new SegmentInstanceCandidate(instance, onlineChecker.test(instance)));
      }
    }
    return candidates;
  }

  /**
   * Converts the given map into a sorted map if needed.
   */
  private static SortedMap<String, String> convertToSortedMap(Map<String, String> map) {
    if (map instanceof SortedMap) {
      return (SortedMap<String, String>) map;
    } else {
      return new TreeMap<>(map);
    }
  }
}
