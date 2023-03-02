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
import java.util.SortedMap;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of instance selector which maintains a map from segment to enabled ONLINE/CONSUMING server
 * instances that serves the segment and a set of unavailable segments (no enabled instance or all enabled instances are
 * in ERROR state).
 */
abstract class BaseInstanceSelector implements InstanceSelector {
  public class SegmentState {
    // List of instance for this segment in ideal state.
    private List<String> _candidateInstance;
    // Mapping from candidate to index in _candidateInstance.
    private HashMap<String, Integer> _candidateIdx;
    // Mark offline instance in _candidateInstance using idx.
    private RoaringBitmap _offlineFlags;
    // Segment creation time.
    // It is zk record's creation time when instance selector is init.
    // After init, we use system clock time when we receive first ideal state for this segment as approximation.
    private long _creationMillis;

    public SegmentState(long creationMillis) {
      _creationMillis = creationMillis;
      _candidateIdx = new HashMap<>();
      _candidateInstance = new ArrayList<>();
      _offlineFlags = new RoaringBitmap();
    }

    public boolean isNew(long nowMillis) {
      return CommonConstants.Helix.StateModel.isNewSegment(_creationMillis, nowMillis);
    }

    // Reset the candidate state upon.
    public void resetCandidates() {
      _candidateIdx.clear();
      _candidateInstance.clear();
      _offlineFlags.clear();
    }

    public void addCandidate(String candidate, boolean online) {
      int idx = _candidateInstance.size();
      _candidateInstance.add(candidate);
      if (!online) {
        _offlineFlags.add(idx);
      }
      _candidateIdx.put(candidate, idx);
    }

    public void setOnline(String candidate) {
      _offlineFlags.remove(_candidateIdx.get(candidate));
    }

    public boolean isAllOffline() {
      return _offlineFlags != null && _offlineFlags.getCardinality() == _candidateIdx.size();
    }

    public boolean isInstanceOnline(int instanceIdx) {
      return _offlineFlags == null || !_offlineFlags.contains(instanceIdx);
    }

    public List<String> getCandidates() {
      return _candidateInstance;
    }

    public RoaringBitmap getOfflineFlags() {
      return _offlineFlags;
    }

    public long getCreationMillis() {
      return _creationMillis;
    }
  }

  // To prevent int overflow, reset the request id once it reaches this value
  private static final long MAX_REQUEST_ID = 1_000_000_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseInstanceSelector.class);

  protected final BrokerMetrics _brokerMetrics;
  protected final AdaptiveServerSelector _adaptiveServerSelector;

  protected final String _tableNameWithType;

  // These 4 variables are the cached states to help accelerate the change processing
  protected Set<String> _enabledInstances;
  protected Map<String, List<String>> _segmentToOnlineInstancesMap;
  protected Map<String, List<String>> _segmentToOfflineInstancesMap;
  protected Map<String, List<String>> _instanceToSegmentsMap;
  protected Map<String, SegmentState> _newSegmentStates;

  // These 2 variables are needed for instance selection (multi-threaded), so make them volatile
  protected volatile Map<String, List<String>> _segmentToEnabledInstancesMap;
  protected volatile Set<String> _unavailableSegments;
  protected volatile Map<String, SegmentState> _newSegmentToCandidateInstanceMap;

  protected Clock _clock;

  BaseInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector) {
    this(tableNameWithType, brokerMetrics, adaptiveServerSelector, Clock.systemUTC());
  }

  BaseInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics,
      @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock) {
    _tableNameWithType = tableNameWithType;
    _brokerMetrics = brokerMetrics;
    _adaptiveServerSelector = adaptiveServerSelector;
    _newSegmentStates = Collections.emptyMap();
    _newSegmentToCandidateInstanceMap = Collections.emptyMap();
    _clock = clock;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    _enabledInstances = enabledInstances;
    int segmentMapCapacity = HashUtil.getHashMapCapacity(onlineSegments.size());
    _segmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
    _segmentToOfflineInstancesMap = new HashMap<>(segmentMapCapacity);
    _instanceToSegmentsMap = new HashMap<>();
    onAssignmentChange(idealState, externalView, onlineSegments);
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
    // NOTE: We can directly modify the map because we will only update the values without changing the map entries.
    // Because the map is marked as volatile, the running queries (already accessed the map) might use the enabled
    // instances either before or after the change, which is okay; the following queries (not yet accessed the map) will
    // get the updated value.
    Map<String, List<String>> segmentToEnabledInstancesMap = _segmentToEnabledInstancesMap;
    Set<String> currentUnavailableSegments = _unavailableSegments;
    Set<String> newUnavailableSegments = new HashSet<>();
    long nowMillis = _clock.millis();
    Map<String, SegmentState> newSegmentToCandidateInstanceMap = _newSegmentToCandidateInstanceMap;
    for (Map.Entry<String, SegmentState> entry : newSegmentToCandidateInstanceMap.entrySet()) {
      String segment = entry.getKey();
      if (segmentsToUpdate.contains(segment)) {
        SegmentState candidates =
            calculateCandidatesForNewSegment(segment, _newSegmentToCandidateInstanceMap.get(segment));
        entry.setValue(candidates);
      }
    }
    for (Map.Entry<String, List<String>> entry : segmentToEnabledInstancesMap.entrySet()) {
      String segment = entry.getKey();
      if (segmentsToUpdate.contains(segment)) {
        SegmentState candidateInstances = newSegmentToCandidateInstanceMap.get(segment);
        Long creationMillis = candidateInstances == null ? null : candidateInstances.getCreationMillis();
        List<String> enabledInstancesForSegment =
            calculateEnabledInstancesForSegment(segment, _segmentToOnlineInstancesMap.get(segment), creationMillis,
                nowMillis, newUnavailableSegments);
        entry.setValue(enabledInstancesForSegment);
      } else {
        if (currentUnavailableSegments.contains(segment)) {
          newUnavailableSegments.add(segment);
        }
      }
    }
    _newSegmentToCandidateInstanceMap = newSegmentToCandidateInstanceMap;
    _segmentToEnabledInstancesMap = segmentToEnabledInstancesMap;
    _unavailableSegments = newUnavailableSegments;
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
    _segmentToOnlineInstancesMap.clear();
    _segmentToOfflineInstancesMap.clear();
    _instanceToSegmentsMap.clear();

    long nowMillis = _clock.millis();
    // Update the cached maps
    updateSegmentMaps(idealState, externalView, onlineSegments, _segmentToOnlineInstancesMap,
        _segmentToOfflineInstancesMap, _instanceToSegmentsMap, nowMillis);

    // Generate a new map from segment to enabled ONLINE/CONSUMING instances and a new set of unavailable segments (no
    // enabled instance or all enabled instances are in ERROR state)
    Map<String, List<String>> segmentToEnabledInstancesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(_segmentToOnlineInstancesMap.size()));
    Map<String, SegmentState> newSegmentToCandidateInstanceMap = new HashMap<>(
        HashUtil.getHashMapCapacity(HashUtil.getHashMapCapacity(_newSegmentToCandidateInstanceMap.size())));
    for (Map.Entry<String, SegmentState> entry : _newSegmentStates.entrySet()) {
      String segment = entry.getKey();
      SegmentState candidateInstances = calculateCandidatesForNewSegment(segment, entry.getValue());
      newSegmentToCandidateInstanceMap.put(segment, candidateInstances);
    }

    Set<String> unavailableSegments = new HashSet<>();
    // NOTE: Put null as the value when there is no enabled instances for a segment so that segmentToEnabledInstancesMap
    // always contains all segments. With this, in onInstancesChange() we can directly iterate over
    // segmentToEnabledInstancesMap.entrySet() and modify the value without changing the map entries.
    for (Map.Entry<String, List<String>> entry : _segmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      SegmentState candidateInstances = newSegmentToCandidateInstanceMap.get(segment);
      Long creationMillis = candidateInstances == null ? null : candidateInstances.getCreationMillis();
      List<String> enabledInstancesForSegment =
          calculateEnabledInstancesForSegment(segment, entry.getValue(), creationMillis, nowMillis,
              unavailableSegments);
      segmentToEnabledInstancesMap.put(segment, enabledInstancesForSegment);
    }

    _newSegmentToCandidateInstanceMap = newSegmentToCandidateInstanceMap;
    _segmentToEnabledInstancesMap = segmentToEnabledInstancesMap;
    _unavailableSegments = unavailableSegments;
  }

  /**
   * Updates the segment maps based on the given ideal state, external view and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and pre-selected by the {@link SegmentPreSelector}).
   */
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, List<String>> segmentToOnlineInstancesMap, Map<String, List<String>> segmentToOfflineInstancesMap,
      Map<String, List<String>> instanceToSegmentsMap, long nowMillis) {
    // Iterate over the external view instead of the online segments so that the map lookups are performed on the
    // HashSet instead of the TreeSet for performance
    // NOTE: Do not track segments not in the external view because it is a valid state when the segment is new added
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();

      // Only track online segments
      if (!onlineSegments.contains(segment)) {
        continue;
      }

      Map<String, String> externalViewInstanceStateMap = entry.getValue();
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      List<String> onlineInstances = new ArrayList<>(externalViewInstanceStateMap.size());
      List<String> offlineInstances = new ArrayList<>();
      segmentToOnlineInstancesMap.put(segment, onlineInstances);
      segmentToOfflineInstancesMap.put(segment, offlineInstances);
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
          if (externalViewState.equals(SegmentStateModel.OFFLINE)) {
            offlineInstances.add(instance);
          } else {
            onlineInstances.add(instance);
          }
        }
      }

      // Sort the online instances for replica-group routing to work. For multiple segments with the same online
      // instances, if the list is sorted, the same index in the list will always point to the same instance.
      if (!(externalViewInstanceStateMap instanceof SortedMap)) {
        onlineInstances.sort(null);
        offlineInstances.sort(null);
      }
    }
  }

  /**
   * Calculates the routing information for new segments. Only calculated in StrictReplicaGroup.
   */
  protected SegmentState calculateCandidatesForNewSegment(String segment, SegmentState state) {
    return null;
  }

  /**
   * Calculates the enabled ONLINE/CONSUMING instances for the given segment, and updates the unavailable segments (no
   * enabled instance or all enabled instances are in ERROR state).
   */
  @Nullable
  private List<String> calculateEnabledInstancesForSegment(String segment, List<String> onlineInstancesForSegment,
      Long creationMillis, long nowMillis, Set<String> unavailableSegments) {
    List<String> enabledInstancesForSegment = new ArrayList<>(onlineInstancesForSegment.size());
    for (String onlineInstance : onlineInstancesForSegment) {
      if (_enabledInstances.contains(onlineInstance)) {
        enabledInstancesForSegment.add(onlineInstance);
      }
    }
    if (!enabledInstancesForSegment.isEmpty()) {
      return enabledInstancesForSegment;
    }
    if (isValidUnavailable(creationMillis, segment, nowMillis)) {
      LOGGER.info("Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} are "
              + "disabled not counting the segment as unavailable)", segment, _tableNameWithType,
          onlineInstancesForSegment);
    } else {
      LOGGER.warn(
          "Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} counting"
              + " segment as unavailable)", segment, _tableNameWithType, onlineInstancesForSegment);
      unavailableSegments.add(segment);
      _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
    }
    return null;
  }

  protected boolean isValidUnavailable(Long segmentCreationTime, String segment, long nowMillis) {
    // NOTE: When there are enabled instances in OFFLINE state, we don't count the segment as unavailable because it
    //       is a valid state when the segment is new added.
    List<String> offlineInstancesForSegment = _segmentToOfflineInstancesMap.get(segment);
    for (String offlineInstance : offlineInstancesForSegment) {
      if (_enabledInstances.contains(offlineInstance)) {
        return true;
      }
    }
    return false;
  }

  protected Set<String> getUnavailableSegments(long nowMillis) {
    return _unavailableSegments;
  }

  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments, long requestId) {
    Map<String, String> queryOptions =
        (brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().getQueryOptions() != null)
            ? brokerRequest.getPinotQuery().getQueryOptions() : Collections.emptyMap();
    int requestIdInt = (int) (requestId % MAX_REQUEST_ID);
    long nowMillis = _clock.millis();
    Map<String, String> segmentToInstanceMap =
        select(segments, requestIdInt, _segmentToEnabledInstancesMap, _newSegmentToCandidateInstanceMap, queryOptions,
            nowMillis);
    Set<String> unavailableSegments = getUnavailableSegments(nowMillis);
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
  abstract Map<String, String> select(List<String> segments, int requestId,
      Map<String, List<String>> segmentToEnabledInstancesMap,
      Map<String, SegmentState> newSegmentToCandidateInstanceMap, Map<String, String> queryOptions, long nowMillis);
}
