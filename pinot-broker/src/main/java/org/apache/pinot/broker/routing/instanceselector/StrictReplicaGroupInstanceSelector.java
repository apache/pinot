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
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMeter;
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
 * Note that this implementation means:
 * 1) Inconsistency across requests for new segments (some may be available, some may be not)
 * 2) When there is no assignment/instance change for long time, some of the new segments that expire with the clock
 * won't be counted as unavailable since no update is triggered.
 * </pre>
 */
public class StrictReplicaGroupInstanceSelector extends ReplicaGroupInstanceSelector {
  private final Clock _clock;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HashMap<String, Long> _newSegmentCreationTimeMillis;
  private boolean _initialized = false;

  private void updateNewSegmentsCreationTime(Set<String> onlineSegments) {
    long nowMillis = _clock.millis();
    for (String segment : onlineSegments) {
      if (!_segmentToOnlineInstancesMap.containsKey(segment)) {
        _newSegmentCreationTimeMillis.put(segment, nowMillis);
      }
    }
  }

  private boolean isNewSegment(String segment, long nowMillis) {
    Long segmentCreationMillis = _newSegmentCreationTimeMillis.getOrDefault(segment, null);
    if (segmentCreationMillis == null) {
      return false;
    }
    return CommonConstants.Helix.StateModel.isNewSegment(segmentCreationMillis, nowMillis);
  }

  @Override
  protected List<String> calculateEnabledInstancesForSegment(String segment, List<String> onlineInstancesForSegment,
      Set<String> unavailableSegments) {
    List<String> enabledInstancesForSegment = new ArrayList<>(onlineInstancesForSegment.size());
    for (String onlineInstance : onlineInstancesForSegment) {
      if (_enabledInstances.contains(onlineInstance)) {
        enabledInstancesForSegment.add(onlineInstance);
      }
    }
    if (!enabledInstancesForSegment.isEmpty()) {
      return enabledInstancesForSegment;
    } else {
      // NOTE: When there are enabled instances in OFFLINE state, we don't count the segment as unavailable because it
      //       is a valid state when the segment is new added.
      List<String> offlineInstancesForSegment = _segmentToOfflineInstancesMap.get(segment);
      for (String offlineInstance : offlineInstancesForSegment) {
        if (_enabledInstances.contains(offlineInstance)) {
          LOGGER.info(
              "Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} are "
                  + "disabled, but find enabled OFFLINE instance: {} from OFFLINE instances: {}, not counting the "
                  + "segment as unavailable)", segment, _tableNameWithType, onlineInstancesForSegment, offlineInstance,
              offlineInstancesForSegment);
          return null;
        }
      }
      // NOTE: When the segment is new, we don't count the segment as unavailable.
      if (isNewSegment(segment, _clock.millis())) {
        LOGGER.info("Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} are "
            + "disabled) and segments are new.", segment, _tableNameWithType, onlineInstancesForSegment);
        return null;
      }
      LOGGER.warn(
          "Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} and OFFLINE "
              + "instances: {} are disabled, counting segment as unavailable)", segment, _tableNameWithType,
          onlineInstancesForSegment, offlineInstancesForSegment);
      unavailableSegments.add(segment);
      _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
      return null;
    }
  }

  public StrictReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    this(tableNameWithType, brokerMetrics, adaptiveServerSelector, propertyStore, Clock.systemUTC());
  }

  public StrictReplicaGroupInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore,
      Clock clock) {
    super(tableNameWithType, brokerMetrics, adaptiveServerSelector);
    _newSegmentCreationTimeMillis = new HashMap<>();
    _propertyStore = propertyStore;
    _clock = clock;
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
    _newSegmentCreationTimeMillis = new HashMap<>();
    long nowMillis = _clock.millis();
    for (String onlineSegment : onlineSegments) {
      SegmentZKMetadata metadata =
          ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, _tableNameWithType, onlineSegment);
      if (metadata == null) {
        continue;
      }
      long creationTimeMillis = metadata.getCreationTime();
      if (CommonConstants.Helix.StateModel.isNewSegment(creationTimeMillis, nowMillis)) {
        _newSegmentCreationTimeMillis.put(onlineSegment, creationTimeMillis);
      }
    }
    onAssignmentChange(idealState, externalView, onlineSegments);
    _initialized = true;
  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    if (_initialized) {
      updateNewSegmentsCreationTime(onlineSegments);
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
      Map<String, List<String>> instanceToSegmentsMap) {
    // TODO: Add support for AdaptiveServerSelection.
    // Iterate over the ideal state to fill up 'idealStateSegmentToInstancesMap' which is a map from segment to set of
    // instances hosting the segment in the ideal state
    int segmentMapCapacity = HashUtil.getHashMapCapacity(onlineSegments.size());
    Map<String, Set<String>> idealStateSegmentToInstancesMap = new HashMap<>(segmentMapCapacity);
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      // Only track online segments
      if (!onlineSegments.contains(segment)) {
        continue;
      }
      idealStateSegmentToInstancesMap.put(segment, entry.getValue().keySet());
    }

    // Iterate over the external view to fill up 'tempSegmentToOnlineInstancesMap' and 'segmentToOfflineInstancesMap'.
    // 'tempSegmentToOnlineInstancesMap' is a temporary map from segment to set of instances that are in the ideal state
    // and also ONLINE/CONSUMING in the external view. This map does not have the strict replica-group guarantee, and
    // will be used to calculate the final 'segmentToOnlineInstancesMap'.
    Map<String, Set<String>> tempSegmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
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
        }
      }
    }
    long nowMillis = _clock.millis();
    // Iterate over the 'tempSegmentToOnlineInstancesMap' to gather the unavailable instances for each set of instances
    Map<Set<String>, Set<String>> unavailableInstancesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : tempSegmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      Set<String> tempOnlineInstances = entry.getValue();
      Set<String> instancesInIdealState = idealStateSegmentToInstancesMap.get(segment);
      if (tempOnlineInstances.size() == instancesInIdealState.size()) {
        _newSegmentCreationTimeMillis.remove(segment);
        continue;
      }
      // NOTE: When a segment is unavailable on all the instances, do not count all the instances as unavailable because
      //       this segment is unavailable and won't be included in the routing table, thus not breaking the requirement
      //       of routing to the same replica-group. This is normal for new added segments, and we don't want to mark
      //       all instances down on all segments with the same assignment.
      if (tempOnlineInstances.isEmpty()) {
        continue;
      }
      // Exclude new segment from unavailable instances as well since we don't want to hotspot the instance where new
      // segment is online.
      if (isNewSegment(segment, nowMillis)) {
        continue;
      }
      _newSegmentCreationTimeMillis.remove(segment);
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
      Set<String> unavailableInstances = unavailableInstancesMap.get(instancesInIdealState);
      if (unavailableInstances == null) {
        // No unavailable instance, add all instances as online instance
        for (String instance : tempOnlineInstances) {
          onlineInstances.add(instance);
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
        }
      } else {
        // Some instances are unavailable, add the remaining instances as online instance
        for (String instance : tempOnlineInstances) {
          if (!unavailableInstances.contains(instance)) {
            onlineInstances.add(instance);
            instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
          }
        }
      }
    }
  }
}
