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

import com.google.common.base.Preconditions;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;


/**
 * Instance selector for strict replica-group routing strategy.
 *
 * <pre>
 * The strict replica-group routing strategy always routes the query to the instances within the same replica-group.
 * (Note that the replica-group information is derived from the ideal state of the table, where the instances are sorted
 * alphabetically in the instance state map, so the replica-groups in the instance selector might not match the
 * replica-groups in the instance partitions.) The instances in a replica-group should have all the online segments
 * (segments with ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector) available
 * (ONLINE/CONSUMING in the external view) in order to serve queries. If any segment is unavailable in the
 * replica-group, we mark the whole replica-group down and not serve queries with this replica-group.
 *
 * The selection algorithm is the same as {@link ReplicaGroupInstanceSelector}, and will always evenly distribute the
 * traffic to all replica-groups that have all online segments available.
 *
 * The algorithm relies on the mirror segment assignment from replica-group segment assignment strategy. With mirror
 * segment assignment, any server in one replica-group will always have a corresponding server in other replica-groups
 * that have the same segments assigned. For example, if S1 is a server in replica-group 1, and it has mirror server
 * S2 in replica-group 2 and S3 in replica-group 3. All segments assigned to S1 will also be assigned to S2 and S3. In
 * stable scenario (external view matches ideal state), all segments assigned to S1 will have the same enabled instances
 * of [S1, S2, S3] sorted (in alphabetical order). If we always pick the same index of enabled instances for all
 * segments, only one of S1, S2, S3 will be picked, and all the segments are processed by the same server. In
 * transitioning/error scenario (external view does not match ideal state), if a segment is down on S1, we mark all
 * segments with the same assignment ([S1, S2, S3]) down on S1 to ensure that we always route the segments to the same
 * replica-group.
 *
 * Note that New segment won't be used to exclude instance from serving where new segment is unavailable.
 *
 *  </pre>
 */
public class StrictReplicaGroupInstanceSelector extends ReplicaGroupInstanceSelector {
  public StrictReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore,
      Clock clock) {
    super(tableNameWithType, brokerMetrics, adaptiveServerSelector, propertyStore, clock);
  }

  /**
   * {@inheritDoc}
   *
   * <pre>
   *  Instances unavailable in any old segment should not exist in oldNewSegmentStateMap entries or
   *  newSegmentStateMap for segment with same ideal state instance.
   *
   * The maps are calculated in the following steps to meet the strict replica-group guarantee:
   *   1. Check whether segment is new or old based on ideal state and external view and store the online instances for
   *   each segment in new segment state map or old segment state.
   *   2. For old segment, remove it from the newSegmentStateMap.
   *   3. Use old segment state map to compare the instances from the ideal state and the external view and gather the
   *   unavailable instances for each set of instances
   *   4. Exclude the unavailable instances from the online instances map for both old and new segment map.
   *   5. For old segment, add the remaining online instance to the map.
   *   6. For new segment, add the ideal state instance to the map with online flags.
   * </pre>
   */
  @Override
  protected void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, SegmentState> oldSegmentStateMap, Map<String, SegmentState> newSegmentStateMap) {
    int segmentMapCapacity = HashUtil.getHashMapCapacity(onlineSegments.size());
    Map<String, Set<String>> idealStateSegmentToInstancesMap = new HashMap<>(segmentMapCapacity);
    Map<String, Set<String>> oldSegmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
    Map<String, Set<String>> newSegmentTempSegmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    // TODO: Add support for AdaptiveServerSelection.
    // Iterate over the ideal state to
    // 1) Know whether a segment is new or old
    // New segment definition:
    // - external view hasn't converged with ideal state.
    // - the segment hasn't seen error in any instance.
    // 2) For new segment, fill in newSegmentTempSegmentToOnlineInstancesMap.
    // 3) For old segment, remove it from the newSegmentStageMap and fill in oldSegmentToOnlineInstancesMap
    // 4) Track every segment's ideal state in idealStateSegmentToInstancesMap, which we will use to do instance
    // exclusion later.
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstances = idealStateAssignment.get(segment);
      Preconditions.checkState(idealStateInstances != null, "ideal state instances cannot be null");
      Map<String, String> externalViewInstances = externalViewAssignment.getOrDefault(segment, Collections.emptyMap());
      Pair<Set<String>, Boolean> segmentInfo =
          InstanceSelector.getSegmentInfo(externalViewInstances, idealStateInstances);
      Set<String> onlineInstances = segmentInfo.getLeft();
      boolean couldBeNew = segmentInfo.getRight();
      SegmentState state = newSegmentStateMap.get(segment);
      if (couldBeNew && state != null) {
        newSegmentTempSegmentToOnlineInstancesMap.put(segment, onlineInstances);
      } else {
        if (state != null) {
          newSegmentStateMap.remove(segment);
          state.promoteToOld();
        } else {
          state = SegmentState.createDefaultSegmentState();
        }
        oldSegmentStateMap.put(segment, state);
        oldSegmentToOnlineInstancesMap.put(segment, onlineInstances);
      }
      idealStateSegmentToInstancesMap.put(segment, idealStateInstances.keySet());
    }
    // Get unavailable instances from oldSegmentToOnlineInstancesMap.
    // Note that we don't use new segments to set up unavailable instance.
    Map<Set<String>, Set<String>> unavailableInstancesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : oldSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.computeIfAbsent(instancesInIdealState, k -> new HashSet<>());
      Set<String> tempOnlineInstances = entry.getValue();
      for (String instance : instancesInIdealState) {
        if (!tempOnlineInstances.contains(instance)) {
          unavailableInstances.add(instance);
        }
      }
    }

    // Set up 'segmentToOnlineInstancesMap' for old segments with the strict replica-group guarantee
    for (Map.Entry<String, Set<String>> entry : oldSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> candidateInstance = entry.getValue();
      List<SegmentInstanceCandidate> onlineInstanceCandidates = new ArrayList<>();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(instancesInIdealState, Collections.emptySet());
      for (String instance : candidateInstance) {
        // Some instances are unavailable, add the remaining instances as online instance
        if (!unavailableInstances.contains(instance)) {
          onlineInstanceCandidates.add(new SegmentInstanceCandidate(instance, true));
        }
      }
      SegmentState state = oldSegmentStateMap.computeIfAbsent(segment, s -> SegmentState.createDefaultSegmentState());
      state.setCandidates(onlineInstanceCandidates);
    }

    // Set up instances in newSegmentStateMap for new segments.
    for (Map.Entry<String, Set<String>> entry : newSegmentTempSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(instancesInIdealState, Collections.emptySet());
      SegmentState state = newSegmentStateMap.get(segment);
      Set<String> onlineInstances = entry.getValue();
      List<SegmentInstanceCandidate> candidates = new ArrayList<>();
      for (String instance : instancesInIdealState) {
        if (!unavailableInstances.contains(instance)) {
          candidates.add(new SegmentInstanceCandidate(instance, onlineInstances.contains(instance)));
        }
      }
      state.setCandidates(candidates);
    }
  }
}
