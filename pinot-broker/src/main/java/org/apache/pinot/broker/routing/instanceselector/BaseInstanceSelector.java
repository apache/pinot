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
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;


/**
 * Base implementation of instance selector which maintains a map from segment to enabled ONLINE/CONSUMING server
 * instances that serves the segment and a set of unavailable segments (no enabled instance or all enabled instances are
 * in ERROR state).
 *
 * New segment won't be counted as unavailable.
 * This is because it is common for new segment to be partially available, and we don't want to have hot spot or low
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
 * are still considered as old.
 */
abstract class BaseInstanceSelector implements InstanceSelector {
  public static class SegmentState {
    // List of instance for this segment in ideal state.
    // Mapping from candidate to index in _candidateInstance.
    private HashMap<String, Boolean> _candidates;

    private long _creationMillis;

    public SegmentState(long creationMillis) {
      _creationMillis = creationMillis;
      _candidates = new HashMap<>();
    }

    public boolean isNew(long nowMillis) {
      return CommonConstants.Helix.StateModel.isNewSegment(_creationMillis, nowMillis);
    }

    public void resetCandidates() {
      _candidates.clear();
    }

    public void addCandidate(String candidate, boolean online) {
      _candidates.put(candidate, online);
    }

    public HashMap<String, Boolean> getCandidates() {
      return _candidates;
    }
  }

  // To prevent int overflow, reset the request id once it reaches this value
  private static final long MAX_REQUEST_ID = 1_000_000_000;

  private final BrokerMetrics _brokerMetrics;
  private final String _tableNameWithType;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected final AdaptiveServerSelector _adaptiveServerSelector;

  // These 4 variables are the cached states to help accelerate the change processing
  private Set<String> _enabledInstances;
  private Map<String, List<String>> _segmentToOnlineInstancesMap;
  private Map<String, List<String>> _instanceToSegmentsMap;
  private Map<String, SegmentState> _newSegmentStates;

  // _segmentStateSnapshot is needed for instance selection (multi-threaded), so make them volatile
  private volatile SegmentStateSnapshot _segmentStateSnapshot;
  private Clock _clock;

  BaseInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    this(tableNameWithType, brokerMetrics, adaptiveServerSelector, propertyStore, Clock.systemUTC());
  }

  // Test only for clock injection.
  BaseInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, ZkHelixPropertyStore<ZNRecord> propertyStore,
      Clock clock) {
    _tableNameWithType = tableNameWithType;
    _brokerMetrics = brokerMetrics;
    _adaptiveServerSelector = adaptiveServerSelector;
    _newSegmentStates = new HashMap<>();
    _propertyStore = propertyStore;
    _clock = clock;
  }

  // Get the segment where the ideal state hasn't converged with external view and which doesn't have error instance.
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
      boolean couldBeNewSegment = true;
      for (Map.Entry<String, String> instanceStateEntry : externalViewInstanceStateMap.entrySet()) {
        String instance = instanceStateEntry.getKey();
        // Only track instance in ideal state.
        if (!idealStateInstanceStateMap.containsKey(instance)) {
          continue;
        }
        String externalViewState = instanceStateEntry.getValue();
        // Do not track instances in ERROR state
        if (externalViewState.equals(SegmentStateModel.ERROR)) {
          couldBeNewSegment = false;
          break;
        }
        if (SegmentStateModel.isOnline(externalViewState)) {
          onlineInstance.add(instance);
        }
      }
      if (couldBeNewSegment && onlineInstance.size() != idealStateInstanceStateMap.size()) {
        potentialNewSegments.add(segment);
      }
    }
    return potentialNewSegments;
  }

  // Use segment creation time to decide whether a segment is new.
  private static Map<String, SegmentState> getNewSegmentsFromZK(String tableNameWithType,
      List<String> potentialNewSegments, ZkHelixPropertyStore<ZNRecord> propertyStore, long nowMillis) {
    Map<String, SegmentState> newSegmentState = new HashMap<>();
    List<String> segmentZKMetadataPaths = new ArrayList<>();
    for (String segment : potentialNewSegments) {
      segmentZKMetadataPaths.add(ZKMetadataProvider.constructPropertyStorePathForSegment(tableNameWithType, segment));
    }
    List<ZNRecord> znRecords = propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (ZNRecord record: znRecords) {
      if (record == null) {
        continue;
      }
      SegmentZKMetadata metadata = new SegmentZKMetadata(record);
      long creationTimeMillis = metadata.getCreationTime();
      String segmentName = metadata.getSegmentName();
      if (CommonConstants.Helix.StateModel.isNewSegment(creationTimeMillis, nowMillis)) {
        newSegmentState.put(segmentName, new SegmentState(creationTimeMillis));
      }
    }
    return newSegmentState;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    _enabledInstances = enabledInstances;
    List<String> potentialNewSegments = getPotentialNewSegments(idealState, externalView, onlineSegments);
    long nowMillis = _clock.millis();
    _newSegmentStates = getNewSegmentsFromZK(_tableNameWithType, potentialNewSegments, _propertyStore, nowMillis);
    int segmentMapCapacity = HashUtil.getHashMapCapacity(onlineSegments.size());
    _segmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
    _instanceToSegmentsMap = new HashMap<>();
    onAssignmentChange(idealState, externalView, onlineSegments, nowMillis, false);
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

    // Update all segments served by the changed instances
    Set<String> segmentsToUpdate = new HashSet<>();
    for (String instance : changedInstances) {
      List<String> segments = _instanceToSegmentsMap.get(instance);
      if (segments != null) {
        segmentsToUpdate.addAll(segments);
      }
    }

    // Directly return if no segment needs to be updated
    if (segmentsToUpdate.isEmpty()) {
      return;
    }

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
    long nowMillis = _clock.millis();
    onAssignmentChange(idealState, externalView, onlineSegments, nowMillis, true);
  }

  private void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      long nowMillis, boolean refreshNewSegments) {
    if (refreshNewSegments) {
      // If this call is not from init, we use existing state to check whether a segment is new.
      // And we use system clock time as an approximation of segment creation time.
      for (String segment : onlineSegments) {
        if (!_segmentToOnlineInstancesMap.containsKey(segment) && !_newSegmentStates.containsKey(segment)) {
          _newSegmentStates.put(segment, new SegmentState(nowMillis));
        }
      }
    }
    _segmentToOnlineInstancesMap.clear();
    _instanceToSegmentsMap.clear();

    // Update the cached maps
    updateSegmentMaps(idealState, externalView, onlineSegments, _segmentToOnlineInstancesMap, _instanceToSegmentsMap,
        _newSegmentStates, nowMillis);

    _segmentStateSnapshot =
        SegmentStateSnapshot.createSnapshot(_tableNameWithType, _segmentToOnlineInstancesMap, _newSegmentStates,
            _enabledInstances, _brokerMetrics);
  }

  /**
   * Updates the segment maps based on the given ideal state, external view and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and pre-selected by the {@link SegmentPreSelector}).
   */
  protected void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, List<String>> segmentToOnlineInstancesMap, Map<String, List<String>> instanceToSegmentsMap,
      Map<String, SegmentState> newSegmentStateMap, long nowMillis) {
    // NOTE: Segments with missing external view are considered as new.
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    // Iterate over the ideal state instead of the external view since this will cover segment with missing external
    // view.
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
      Set<String> onlineInstances = new TreeSet<>();
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
        if (!externalViewState.equals(SegmentStateModel.ERROR)) {
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
          if (SegmentStateModel.isOnline(externalViewState)) {
            onlineInstances.add(instance);
          }
        } else {
          // Segment with error state instance should be considered old.
          newSegmentStateMap.remove(segment);
        }
      }
      SegmentState state = newSegmentStateMap.getOrDefault(segment, null);
      boolean isNewSegment = false;
      if (state != null && state.isNew(nowMillis)) {
        if (onlineInstances.size() == idealStateInstanceStateMap.size()) {
          // Segment with converged state is not considered as new anymore.
          newSegmentStateMap.remove(segment);
        } else {
          isNewSegment = true;
        }
      }
      if (isNewSegment) {
        state.resetCandidates();
        // New segment should have all ideal state as candidates to avoid hotspot.
        for (String instance : new TreeSet<>(idealStateInstanceStateMap.keySet())) {
          if (onlineInstances.contains(instance)) {
            state.addCandidate(instance, true);
          } else {
            state.addCandidate(instance, false);
          }
        }
      } else {
        List<String> instances = new ArrayList<>();
        for (String instance : onlineInstances) {
          instances.add(instance);
        }
        segmentToOnlineInstancesMap.put(segment, instances);
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
   * Selects the server instances for the given segments based on the request id and segment to enabled ONLINE/CONSUMING
   * instances map, returns a map from segment to selected server instance hosting the segment.
   * <p>NOTE: {@code segmentToEnabledInstancesMap} might contain {@code null} values (segment with no enabled
   * ONLINE/CONSUMING instances). If enabled instances are not {@code null}, they are sorted in alphabetical order.
   */
  protected abstract Map<String, String> select(List<String> segments, int requestId, SegmentStateSnapshot snapshot,
      Map<String, String> queryOptions);
}
