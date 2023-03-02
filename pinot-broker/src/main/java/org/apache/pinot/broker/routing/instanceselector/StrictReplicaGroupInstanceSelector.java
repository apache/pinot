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
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.CommonConstants;
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
 * New segment won't be counted as unavailable or used to exclude instance from serving where new segment is
 * unavailable.
 * This is because it is common for new segment to be partially available and we don't want to have hot spot or low
 * query availability problem caused by new segment.
 * We also don't report new segment as unavailable segments.
 *
 * New segment is defined as segment that is created more than 5 minutes ago.
 * - For initialization, we look up segment creation time from zookeeper.
 * - After initialization, we use the system clock when we receive the first update of ideal state for that segment as
 *   approximation of segment creation time.
 *
 * We retire new segment as old when:
 * - The creation time is more than 5 mins ago
 * - We receive error state for new segment
 * - External view for segment converges with ideal state.
 *
 * Note that this implementation means:
 * 1) Inconsistency across requests for new segments (some may be available, some may be not)
 * 2) When there is no assignment/instance change for long time, some of the new segments that expire with the clock
 * won't be used to exclude unavailable instances since no update is triggered.
 * </pre>
 */
public class StrictReplicaGroupInstanceSelector extends ReplicaGroupInstanceSelector {
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private boolean _initialized = false;

  private boolean isNewSegment(String segment, long nowMillis) {
    SegmentState state = _newSegmentStates.getOrDefault(segment, null);
    return state != null && state.isNew(nowMillis);
  }

  @Override
  protected boolean isValidUnavailable(@Nullable Long creationMillis, String segment, long nowMillis) {
    return creationMillis != null && CommonConstants.Helix.StateModel.isNewSegment(creationMillis, nowMillis);
  }

  @Override
  protected SegmentState calculateCandidatesForNewSegment(String segment, @Nullable SegmentState state) {
    if (state == null) {
      return null;
    }
    SegmentState newCandidateInstances = new SegmentState(state.getCreationMillis());
    List<String> candidates = state.getCandidates();
    for (int i = 0; i < candidates.size(); i++) {
      String instance = candidates.get(i);
      if (_enabledInstances.contains(instance)) {
        newCandidateInstances.addCandidate(instance, state.isInstanceOnline(i));
      }
    }
    return newCandidateInstances;
  }

  public StrictReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    this(tableNameWithType, brokerMetrics, adaptiveServerSelector, propertyStore, Clock.systemUTC());
  }

  public StrictReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore,
      Clock clock) {
    super(tableNameWithType, brokerMetrics, adaptiveServerSelector, clock);
    _newSegmentStates = new HashMap<>();
    _propertyStore = propertyStore;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    _initialized = false;
    _enabledInstances = enabledInstances;
    int segmentMapCapacity = HashUtil.getHashMapCapacity(onlineSegments.size());
    _segmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
    _segmentToOfflineInstancesMap = new HashMap<>(segmentMapCapacity);
    _instanceToSegmentsMap = new HashMap<>();
    _newSegmentStates = new HashMap<>();
    long nowMillis = _clock.millis();
    for (String onlineSegment : onlineSegments) {
      SegmentZKMetadata metadata =
          ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, _tableNameWithType, onlineSegment);
      if (metadata == null) {
        continue;
      }
      long creationTimeMillis = metadata.getCreationTime();
      if (CommonConstants.Helix.StateModel.isNewSegment(creationTimeMillis, nowMillis)) {
        _newSegmentStates.put(onlineSegment, new SegmentState(creationTimeMillis));
      }
    }
    onAssignmentChange(idealState, externalView, onlineSegments);
    _initialized = true;
  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    if (_initialized) {
      // Update new segments with assignment change when the init has been called.
      long nowMillis = _clock.millis();
      for (String segment : onlineSegments) {
        if (!_segmentToOnlineInstancesMap.containsKey(segment) && !_newSegmentStates.containsKey(segment)) {
          _newSegmentStates.put(segment, new SegmentState(nowMillis));
        }
      }
    }
    super.onAssignmentChange(idealState, externalView, onlineSegments);
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
   * </pre>
   */
  @Override
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, List<String>> segmentToOnlineInstancesMap, Map<String, List<String>> segmentToOfflineInstancesMap,
      Map<String, List<String>> instanceToSegmentsMap, long nowMillis) {
    // TODO: Add support for AdaptiveServerSelection.
    // Iterate over the ideal state to fill up 'idealStateSegmentToInstancesMap' which is a map from segment to set of
    // instances hosting the segment in the ideal state
    int segmentMapCapacity = HashUtil.getHashMapCapacity(onlineSegments.size());
    Map<String, Set<String>> idealStateSegmentToInstancesMap = new HashMap<>(segmentMapCapacity);
    Map<String, Set<String>> tempSegmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);

    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      // Only track online segments
      if (!onlineSegments.contains(segment)) {
        continue;
      }
      SegmentState state = _newSegmentStates.getOrDefault(segment, null);
      if (state != null && state.isNew(nowMillis)) {
        state.resetCandidates();
        for (String instance : entry.getValue().keySet()) {
          state.addCandidate(instance, false);
        }
      } else {
        _newSegmentStates.remove(segment);
      }
      idealStateSegmentToInstancesMap.put(segment, entry.getValue().keySet());
      // Initialize state maps in ideal state to cover the segments with missing external view.
      tempSegmentToOnlineInstancesMap.put(segment, Collections.emptySet());
      segmentToOfflineInstancesMap.put(segment, Collections.emptyList());
    }

    // Iterate over the external view to fill up 'tempSegmentToOnlineInstancesMap' and 'segmentToOfflineInstancesMap'.
    // 'tempSegmentToOnlineInstancesMap' is a temporary map from segment to set of instances that are in the ideal state
    // and also ONLINE/CONSUMING in the external view. This map does not have the strict replica-group guarantee, and
    // will be used to calculate the final 'segmentToOnlineInstancesMap'.
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      // Only track online segments
      if (instancesInIdealState == null) {
        continue;
      }
      Map<String, String> instanceStateMap = entry.getValue();
      Set<String> tempOnlineInstances = new TreeSet<>();
      List<String> offlineInstances = new ArrayList<>();
      tempSegmentToOnlineInstancesMap.put(segment, tempOnlineInstances);
      segmentToOfflineInstancesMap.put(segment, offlineInstances);
      for (Map.Entry<String, String> instanceStateEntry : instanceStateMap.entrySet()) {
        String instance = instanceStateEntry.getKey();
        // Only track instances within the ideal state
        if (!instancesInIdealState.contains(instance)) {
          continue;
        }
        String state = instanceStateEntry.getValue();
        if (state.equals(SegmentStateModel.ONLINE) || state.equals(SegmentStateModel.CONSUMING)) {
          tempOnlineInstances.add(instance);
        } else if (state.equals(SegmentStateModel.OFFLINE)) {
          offlineInstances.add(instance);
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
        } else {
          // error or dropped state should not be considered as new any more.
          _newSegmentStates.remove(segment);
        }
      }
    }
    // Iterate over the 'tempSegmentToOnlineInstancesMap' to gather the unavailable instances for each set of instances
    Map<Set<String>, Set<String>> unavailableInstancesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : tempSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> tempOnlineInstances = entry.getValue();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      if (tempOnlineInstances.size() == instancesInIdealState.size()) {
        // converged state segment is not considered as new
        _newSegmentStates.remove(segment);
        continue;
      }
      // Exclude new segment from marking unavailable instances as well since we don't want to bring down instances
      // when segment is new and valid.
      if (isNewSegment(segment, nowMillis)) {
        continue;
      }
      _newSegmentStates.remove(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.computeIfAbsent(instancesInIdealState, k -> new TreeSet<>());
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
      Set<String> tempOnlineInstances = entry.getValue();
      // NOTE: Instances will be sorted here because 'tempOnlineInstances' is a TreeSet. We need the online instances to
      //       be sorted for replica-group routing to work. For multiple segments with the same online instances, if the
      //       list is sorted, the same index in the list will always point to the same instance.
      List<String> onlineInstances = new ArrayList<>(tempOnlineInstances.size());
      // NOTE: When a segment doesn't have any online instance, we fill in an empty instance list.
      // In calculateEnabledInstancesForSegment, we will check whether the segment is new or not.
      // We won't report error when segment is new.
      segmentToOnlineInstancesMap.put(segment, onlineInstances);
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      Set<String> unavailableInstances =
          unavailableInstancesMap.getOrDefault(instancesInIdealState, Collections.emptySet());
      SegmentState newSegmentState = _newSegmentStates.getOrDefault(segment, null);
      for (String instance : tempOnlineInstances) {
        // Some instances are unavailable, add the remaining instances as online instance
        if (!unavailableInstances.contains(instance)) {
          onlineInstances.add(instance);
          if (newSegmentState != null) {
            newSegmentState.setOnline(instance);
          }
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
        }
      }
    }
  }
}
