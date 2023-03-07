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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;


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

  private boolean isNewSegment(String segment, Map<String, SegmentState> newSegmentStateMap, long nowMillis) {
    SegmentState state = newSegmentStateMap.getOrDefault(segment, null);
    return state != null && state.isNew(nowMillis);
  }

  public StrictReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    this(tableNameWithType, brokerMetrics, adaptiveServerSelector, propertyStore, Clock.systemUTC());
  }

  // Test only for clock injection.
  public StrictReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore,
      Clock clock) {
    super(tableNameWithType, brokerMetrics, adaptiveServerSelector, propertyStore, clock);
  }

  /**
   * {@inheritDoc}
   *
   * <pre>
   * The maps are calculated in the following steps to meet the strict replica-group guarantee:
   *   1. Create a map from online segment to set of instances hosting the segment based on the ideal state
   *   2. Gather the online and offline instances for each online segment from the external view
   *   3. Compare the instances from the ideal state and the external view and gather the unavailable instances for each
   *      set of instances
   *   4. Exclude the unavailable instances from the online instances map
   *   5. For old segment, add the remaining online instance to the map.
   *   6. For new segment, add the ideal state instance to the map with online flags.
   * </pre>
   */
  @Override
  protected void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, List<String>> segmentToOnlineInstancesMap, Map<String, List<String>> instanceToSegmentsMap,
      Map<String, SegmentState> newSegmentStateMap, long nowMillis) {
    // TODO: Add support for AdaptiveServerSelection.
    // Iterate over the ideal state to fill up 'idealStateSegmentToInstancesMap' which is a map from segment to set of
    // instances hosting the segment in the ideal state
    int segmentMapCapacity = HashUtil.getHashMapCapacity(onlineSegments.size());
    Map<String, Set<String>> idealStateSegmentToInstancesMap = new HashMap<>(segmentMapCapacity);
    Map<String, Set<String>> tempSegmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
    Map<String, Set<String>> newSegmentTempSegmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);

    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    // Iterate over the external view to fill up 'tempSegmentToOnlineInstancesMap' and 'segmentToOfflineInstancesMap'.
    // 'tempSegmentToOnlineInstancesMap' is a temporary map from segment to set of instances that are in the ideal state
    // and also ONLINE/CONSUMING in the external view. This map does not have the strict replica-group guarantee, and
    // will be used to calculate the final 'segmentToOnlineInstancesMap'.
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      // Only track online segments
      if (!onlineSegments.contains(segment)) {
        continue;
      }
      Map<String, String> instanceStateMap = externalViewAssignment.getOrDefault(segment, Collections.emptyMap());
      Map<String, String> idealStateInstanceStateMap = entry.getValue();
      Set<String> tempOnlineInstances = new TreeSet<>();
      for (Map.Entry<String, String> instanceStateEntry : instanceStateMap.entrySet()) {
        String instance = instanceStateEntry.getKey();
        // Only track instance in ideal state.
        if (!idealStateInstanceStateMap.containsKey(instance)) {
          continue;
        }
        String state = instanceStateEntry.getValue();
        if (SegmentStateModel.isOnline(state)) {
          tempOnlineInstances.add(instance);
        } else if (state.equals(SegmentStateModel.OFFLINE)) {
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
        } else {
          // error or dropped state should not be considered as new anymore.
          newSegmentStateMap.remove(segment);
        }
      }
      if (tempOnlineInstances.size() == entry.getValue().size()) {
        // converged state segment is not considered as new
        newSegmentStateMap.remove(segment);
      }
      if (isNewSegment(segment, newSegmentStateMap, nowMillis)) {
        newSegmentTempSegmentToOnlineInstancesMap.put(segment, tempOnlineInstances);
      } else {
        newSegmentStateMap.remove(segment);
        tempSegmentToOnlineInstancesMap.put(segment, tempOnlineInstances);
      }
      idealStateSegmentToInstancesMap.put(segment, new TreeSet<>(idealStateInstanceStateMap.keySet()));
    }

    // Iterate over the 'tempSegmentToOnlineInstancesMap' to gather the unavailable instances for each set of instances
    Map<Set<String>, Set<String>> unavailableInstancesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : tempSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.computeIfAbsent(instancesInIdealState, k -> new TreeSet<>());
      Set<String> tempOnlineInstances = entry.getValue();
      for (String instance : instancesInIdealState) {
        if (!tempOnlineInstances.contains(instance)) {
          unavailableInstances.add(instance);
        }
      }
    }

    // Iterate over the 'tempSegmentToOnlineInstancesMap' again to fill up the 'segmentToOnlineInstancesMap' which has
    // the strict replica-group guarantee
    for (Map.Entry<String, Set<String>> entry : tempSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> candidateInstance = entry.getValue();
      // NOTE: Instances will be sorted here because 'tempOnlineInstances' is a TreeSet. We need the online instances to
      //       be sorted for replica-group routing to work. For multiple segments with the same online instances, if the
      //       list is sorted, the same index in the list will always point to the same instance.
      List<String> onlineInstances = new ArrayList<>(candidateInstance.size());
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(instancesInIdealState, Collections.emptySet());
      for (String instance : candidateInstance) {
        // Some instances are unavailable, add the remaining instances as online instance
        if (!unavailableInstances.contains(instance)) {
          onlineInstances.add(instance);
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
        }
      }
      segmentToOnlineInstancesMap.put(segment, onlineInstances);
    }

    for (Map.Entry<String, Set<String>> entry : newSegmentTempSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(instancesInIdealState, Collections.emptySet());
      SegmentState state = newSegmentStateMap.get(segment);
      state.resetCandidates();
      Set<String> onlineInstances = entry.getValue();
      for (String instance : instancesInIdealState) {
        if (!unavailableInstances.contains(instance)) {
          state.addCandidate(instance, onlineInstances.contains(instance));
        }
      }
    }
  }
}
