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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
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
 * Note that new segments won't be used to exclude instances from serving when the segment is unavailable.
 * </pre>
 */
public class StrictReplicaGroupInstanceSelector extends ReplicaGroupInstanceSelector {

  public StrictReplicaGroupInstanceSelector(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics, @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock) {
    super(tableNameWithType, propertyStore, brokerMetrics, adaptiveServerSelector, clock);
  }

  /**
   * {@inheritDoc}
   *
   * <pre>
   * Instances unavailable for any old segment should not exist in _oldSegmentCandidatesMap or _newSegmentStateMap for
   * segments with the same instances in ideal state.
   *
   * The maps are calculated in the following steps to meet the strict replica-group guarantee:
   *   1. Compute the online instances for both old and new segments
   *   2. Compare online instances for old segments with instances in ideal state and gather the unavailable instances
   *   for each set of instances
   *   3. Exclude the unavailable instances from the online instances map for both old and new segment map
   * </pre>
   */
  @Override
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentPushTimeMap) {
    _oldSegmentCandidatesMap.clear();
    int newSegmentMapCapacity = HashUtil.getHashMapCapacity(newSegmentPushTimeMap.size());
    _newSegmentStateMap = new HashMap<>(newSegmentMapCapacity);

    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();

    // Get the online instances for the segments
    Map<String, Set<String>> oldSegmentToOnlineInstancesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(onlineSegments.size()));
    Map<String, Set<String>> newSegmentToOnlineInstancesMap = new HashMap<>(newSegmentMapCapacity);
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      assert idealStateInstanceStateMap != null;
      Map<String, String> externalViewInstanceStateMap = externalViewAssignment.get(segment);
      Set<String> onlineInstances;
      if (externalViewInstanceStateMap == null) {
        onlineInstances = Collections.emptySet();
      } else {
        onlineInstances = getOnlineInstances(idealStateInstanceStateMap, externalViewInstanceStateMap);
      }
      if (newSegmentPushTimeMap.containsKey(segment)) {
        newSegmentToOnlineInstancesMap.put(segment, onlineInstances);
      } else {
        oldSegmentToOnlineInstancesMap.put(segment, onlineInstances);
      }
    }

    // Calculate the unavailable instances based on the old segments' online instances for each combination of instances
    // in the ideal state
    Map<Set<String>, Set<String>> unavailableInstancesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : oldSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> instancesInIdealState = idealStateAssignment.get(segment).keySet();
      Set<String> unavailableInstances =
          unavailableInstancesMap.computeIfAbsent(instancesInIdealState, k -> new HashSet<>());
      for (String instance : instancesInIdealState) {
        if (!entry.getValue().contains(instance)) {
          unavailableInstances.add(instance);
        }
      }
    }

    // Iterate over the maps and exclude the unavailable instances
    for (Map.Entry<String, Set<String>> entry : oldSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      // NOTE: onlineInstances is either a TreeSet or an EmptySet (sorted)
      Set<String> onlineInstances = entry.getValue();
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(idealStateInstanceStateMap.keySet(), Collections.emptySet());
      List<SegmentInstanceCandidate> candidates = new ArrayList<>(onlineInstances.size());
      for (String instance : onlineInstances) {
        if (!unavailableInstances.contains(instance)) {
          candidates.add(new SegmentInstanceCandidate(instance, true));
        }
      }
      _oldSegmentCandidatesMap.put(segment, candidates);
    }

    for (Map.Entry<String, Set<String>> entry : newSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> onlineInstances = entry.getValue();
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(idealStateInstanceStateMap.keySet(), Collections.emptySet());
      List<SegmentInstanceCandidate> candidates = new ArrayList<>(idealStateInstanceStateMap.size());
      for (Map.Entry<String, String> instanceStateEntry : convertToSortedMap(idealStateInstanceStateMap).entrySet()) {
        String instance = instanceStateEntry.getKey();
        if (!unavailableInstances.contains(instance) && isOnlineForRouting(instanceStateEntry.getValue())) {
          candidates.add(new SegmentInstanceCandidate(instance, onlineInstances.contains(instance)));
        }
      }
      _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentPushTimeMap.get(segment), candidates));
    }
  }
}
