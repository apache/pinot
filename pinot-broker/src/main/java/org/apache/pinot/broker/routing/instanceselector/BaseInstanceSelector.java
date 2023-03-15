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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;


/**
 * Base implementation of instance selector. Selector maintains a map from segment to enabled ONLINE/CONSUMING server
 * instances that serves the segment and a set of unavailable segments (no enabled instance or all enabled instances are
 * in ERROR state).
 * <p>
 * Special handling of new segment: It is common for new segment to be partially available or not available at all in
 * all instances.
 * 1) We don't report new segment as unavailable segments.
 * 2) To increase query availability, unavailable
 * instance for new segment won't be excluded for instance selection. When it is selected, we don't serve the new
 * segment.
 * <p>
 * Definition of new segment:
 * 1) Segment created more than 5 minutes ago.
 * - If we first see a segment via initialization, we look up segment creation time from zookeeper.
 * - If we first see a segment via onAssignmentChange initialization, we use the calling time of onAssignmentChange
 * as approximation.
 * 2) We retire new segment as old when:
 * - The creation time is more than 5 minutes ago
 * - Any instance for new segment is in error state
 * - External view for segment converges with ideal state.
 *
 * Note that this implementation means:
 * 1) Inconsistent selection of new segments across queries. (some queries will serve new segments and others won't)
 * 2) When there is no state update from helix, new segments won't be retired because of the time passing.
 * (those with creation time more than 5 minutes ago)
 */
abstract class BaseInstanceSelector implements InstanceSelector {
  // Class used to represent the instance state for new segment.
  protected static class SegmentState {
    // List of SegmentInstanceCandidate: which contains instance name and online flags.
    // The candidates have to be in instance sorted order.
    private TreeSet<SegmentInstanceCandidate> _candidates;

    private static class CompareByInstanceName implements Comparator<SegmentInstanceCandidate> {
      public int compare(SegmentInstanceCandidate candidate1, SegmentInstanceCandidate candidate2) {
        return candidate1.getInstance().compareTo(candidate2.getInstance());
      }
    }

    // Segment creation time. This could be
    // 1) From ZK if we first see this segment via init call.
    // 2) Use wall time, if first see this segment from onAssignmentChange call.
    private long _creationMillis;

    private SegmentState(long creationMillis) {
      _creationMillis = creationMillis;
      _candidates = new TreeSet<>(new CompareByInstanceName());
    }

    public boolean isNew(long nowMillis) {
      return InstanceSelector.isNewSegment(_creationMillis, nowMillis);
    }

    public void resetCandidates() {
      _candidates.clear();
    }

    public void addCandidate(String candidate, boolean online) {
      _candidates.add(SegmentInstanceCandidate.of(candidate, online));
    }

    public TreeSet<SegmentInstanceCandidate> getCandidates() {
      return _candidates;
    }
  }

  // To prevent int overflow, reset the request id once it reaches this value
  private static final long MAX_REQUEST_ID = 1_000_000_000;

  private final BrokerMetrics _brokerMetrics;
  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected final AdaptiveServerSelector _adaptiveServerSelector;

  // These 3 variables are the cached states to help accelerate the change processing
  private Set<String> _enabledInstances;
  // Tracking instance state old for segments.
  // Instances have to be in sorted order
  private Map<String, TreeSet<String>> _segmentToOnlineInstancesMap;
  // Tracking instance state for new segments.
  private Map<String, SegmentState> _newSegmentStates;

  // _segmentStateSnapshot is needed for instance selection (multi-threaded), so it is made volatile.
  private volatile SegmentStateSnapshot _segmentStateSnapshot;
  private Clock _clock;

  BaseInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore,
      Clock clock) {
    _tableNameWithType = tableNameWithType;
    _brokerMetrics = brokerMetrics;
    _adaptiveServerSelector = adaptiveServerSelector;
    _propertyStore = propertyStore;
    _segmentToOnlineInstancesMap = new HashMap<>();
    _clock = clock;
  }

  // Get potential new segments
  // - external view hasn't converged with ideal state
  // - external view could be either missing or partial
  // - external view doesn't have error instance.
  private static List<String> getPotentialNewSegments(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    List<String> potentialNewSegments = new ArrayList<>();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      // Only track online segments
      if (!onlineSegments.contains(segment)) {
        continue;
      }
      Map<String, String> idealStateInstanceStateMap = entry.getValue();
      // Segments with missing external view are considered as new.
      Map<String, String> externalViewInstanceStateMap =
          externalViewAssignment.getOrDefault(segment, Collections.emptyMap());
      List<String> onlineInstance = new ArrayList<>();
      boolean hasErrorInstance = false;
      for (Map.Entry<String, String> instanceStateEntry : externalViewInstanceStateMap.entrySet()) {
        String instance = instanceStateEntry.getKey();
        // Only track instance in ideal state.
        if (!idealStateInstanceStateMap.containsKey(instance)) {
          continue;
        }
        String externalViewState = instanceStateEntry.getValue();
        if (InstanceSelector.isOnlineForServing(externalViewState)) {
          onlineInstance.add(instance);
        } else if (externalViewState.equals(SegmentStateModel.ERROR)) {
          hasErrorInstance = true;
          break;
        }
      }
      if (onlineInstance.size() != idealStateInstanceStateMap.size() && !hasErrorInstance) {
        potentialNewSegments.add(segment);
      }
    }
    return potentialNewSegments;
  }

  // Batch read zk for potential new segments to get segment creation time to decide whether a segment is new or not.
  private static Map<String, SegmentState> getNewSegmentFromZKWithCreationTime(String tableNameWithType,
      List<String> potentialNewSegments, ZkHelixPropertyStore<ZNRecord> propertyStore, long nowMillis) {
    Map<String, SegmentState> newSegmentState = new HashMap<>();
    List<String> segmentZKMetadataPaths = new ArrayList<>();
    for (String segment : potentialNewSegments) {
      segmentZKMetadataPaths.add(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segment));
    }
    List<ZNRecord> znRecords = propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (ZNRecord record : znRecords) {
      if (record == null) {
        continue;
      }
      SegmentZKMetadata metadata = new SegmentZKMetadata(record);
      long creationTimeMillis = metadata.getCreationTime();
      String segmentName = metadata.getSegmentName();
      if (InstanceSelector.isNewSegment(creationTimeMillis, nowMillis)) {
        newSegmentState.put(segmentName, new SegmentState(creationTimeMillis));
      }
    }
    return newSegmentState;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    _enabledInstances = enabledInstances;
    _newSegmentStates = getNewSegmentWithCreationTime(idealState, externalView, onlineSegments, true);
    updateSegmentMaps(idealState, externalView, onlineSegments, _segmentToOnlineInstancesMap, _newSegmentStates);
    _segmentStateSnapshot =
        SegmentStateSnapshot.createSnapshot(_tableNameWithType, _segmentToOnlineInstancesMap, _newSegmentStates,
            _enabledInstances, _brokerMetrics);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Updates the cached enabled instances and re-calculates {@code segmentToEnabledInstancesMap} and
   * {@code unavailableSegments} based on the cached states.
   */
  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    _enabledInstances = enabledInstances;
    // Note that the whole _segmentStateSnapshot has to be updated together to avoid partial state update.
    // Update the map from segment to enabled ONLINE/CONSUMING instances and set of unavailable segments (no enabled
    // instance or all enabled instances are in ERROR state)
    _segmentStateSnapshot =
        SegmentStateSnapshot.createSnapshot(_tableNameWithType, _segmentToOnlineInstancesMap, _newSegmentStates,
            _enabledInstances, _brokerMetrics);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Updates the cached maps ({@code segmentToOnlineInstancesMap}, {@code segmentToOfflineInstancesMap} and
   * {@code instanceToSegmentsMap}) and re-calculates {@code segmentToEnabledInstancesMap} and
   * {@code unavailableSegments} based on the cached states.
   */
  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    _newSegmentStates = getNewSegmentWithCreationTime(idealState, externalView, onlineSegments, false);
    updateSegmentMaps(idealState, externalView, onlineSegments, _segmentToOnlineInstancesMap, _newSegmentStates);
    _segmentStateSnapshot =
        SegmentStateSnapshot.createSnapshot(_tableNameWithType, _segmentToOnlineInstancesMap, _newSegmentStates,
            _enabledInstances, _brokerMetrics);
  }

  // Return a map of potential new segments name to creation time.
  // 1) When readFromZK is set to true, we read creation time from ZK
  // 2) Otherwise, we use system time as an approximation.
  private Map<String, SegmentState> getNewSegmentWithCreationTime(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, boolean readFromZK) {
    long nowMillis = _clock.millis();
    if (!readFromZK) {
      // If this call is not from init, we use existing state to check whether a segment is new.
      // And we use system clock time as an approximation of segment creation time.
      Map<String, SegmentState> newSegmentState = new HashMap<>();
      for (String segment : onlineSegments) {
        SegmentState state = _newSegmentStates.getOrDefault(segment, null);
        if (state != null) {
          // We see this segment before and it is still new.
          if (state.isNew(nowMillis)) {
            newSegmentState.put(segment, state);
          }
        } else if (!_segmentToOnlineInstancesMap.containsKey(segment)) {
          // This is the first time we see this segment.
          newSegmentState.put(segment, new SegmentState(nowMillis));
        }
      }
      return newSegmentState;
    }
    List<String> potentialNewSegments = getPotentialNewSegments(idealState, externalView, onlineSegments);
    return getNewSegmentFromZKWithCreationTime(_tableNameWithType, potentialNewSegments, _propertyStore, nowMillis);
  }

  /**
   * Updates the segment maps based on the given ideal state, external view and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and pre-selected by the {@link SegmentPreSelector}).
   * Prerequisite for this call
   * 1) newSegmentStateMap contains all the potentially new segments based on segment creation time.
   * Invariants for in memory state after this update:
   * 1) New segments should retire from newSegmentStateMap based on external view or ideal state change.
   * 2) Segment should only exist in newSegmentStateMap or segmentToOnlineInstancesMap depending on whether it is old or
   * new.
   * 3) Old segment's online instance should be tracked in segmentToOnlineInstancesMap
   * 4) New segment's instances in ideal state should be tracked in newSegmentStateMap with online flags
   */
  protected void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, TreeSet<String>> segmentToOnlineInstancesMap, Map<String, SegmentState> newSegmentStateMap) {
    // Iterate over the ideal state instead of the external view since segments with missing external view are
    // considered as new.
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      // Only track online segments
      if (!onlineSegments.contains(segment)) {
        continue;
      }
      Map<String, String> idealStateInstanceStateMap = entry.getValue();
      Map<String, String> externalViewInstanceStateMap =
          externalViewAssignment.getOrDefault(segment, Collections.emptyMap());
      // Sort the online instances for replica-group routing to work. For multiple segments with the same online
      // instances, if the list is sorted, the same index in the list will always point to the same instance.
      TreeSet<String> onlineInstances = new TreeSet<>();
      boolean hasErrorInstance = false;
      for (Map.Entry<String, String> instanceStateEntry : externalViewInstanceStateMap.entrySet()) {
        String instance = instanceStateEntry.getKey();
        // Only track instances within the ideal state
        // NOTE: When an instance is not in the ideal state, the instance will drop the segment soon, and it is not safe
        // to query this instance for the segment. This could happen when a segment is moved from one instance to
        // another instance.
        if (!idealStateInstanceStateMap.containsKey(instance)) {
          continue;
        }
        String externalViewState = instanceStateEntry.getValue();
        // Do not track instances in ERROR state
        if (InstanceSelector.isOnlineForServing(externalViewState)) {
          onlineInstances.add(instance);
        } else if (externalViewState.equals(SegmentStateModel.ERROR)) {
          hasErrorInstance = true;
        }
      }
      SegmentState state = newSegmentStateMap.getOrDefault(segment, null);
      if (state != null && onlineInstances.size() != idealStateInstanceStateMap.size() && !hasErrorInstance) {
        state.resetCandidates();
        // New segment should have all ideal state as candidates to avoid hotspot.
        for (String instance : idealStateInstanceStateMap.keySet()) {
          state.addCandidate(instance, onlineInstances.contains(instance));
        }
      } else {
        newSegmentStateMap.remove(segment);
        segmentToOnlineInstancesMap.put(segment, onlineInstances);
      }
    }
  }

  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments, long requestId) {
    Map<String, String> queryOptions =
        (brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().getQueryOptions() != null)
            ? brokerRequest.getPinotQuery().getQueryOptions() : Collections.emptyMap();
    int requestIdInt = (int) (requestId % MAX_REQUEST_ID);
    // Copy the snapshot reference so that segmentToInstanceMap and unavailableSegments can have a consistent view of
    // the state.
    SegmentStateSnapshot snapshot = _segmentStateSnapshot;
    Map<String, String> segmentToInstanceMap = select(segments, requestIdInt, snapshot, queryOptions);
    Set<String> unavailableSegments = snapshot.getUnavailableSegments();
    if (unavailableSegments.isEmpty()) {
      return new SelectionResult(segmentToInstanceMap, Collections.emptyList());
    } else {
      List<String> unavailableSegmentsForRequest = new ArrayList<>();
      for (String segment : segments) {
        if (unavailableSegments.contains(segment)) {
          unavailableSegmentsForRequest.add(segment);
        }
      }
      return new SelectionResult(segmentToInstanceMap, unavailableSegmentsForRequest);
    }
  }

  /**
   * Selects the server instances for the given segments based on the request id and SegmentStateSnapshot. Returns a map
   * from segment to selected server instance hosting the segment.
   */
  protected abstract Map<String, String> select(List<String> segments, int requestId, SegmentStateSnapshot snapshot,
      Map<String, String> queryOptions);
}
