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
import java.util.TreeMap;
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
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of instance selector. Selector maintains a map from segment to enabled ONLINE/CONSUMING server
 * instances that serves the segment and a set of unavailable segments (no enabled instance or all enabled instances are
 * in OFFLINE/ERROR state).
 * <p>
 * Special handling of new segment: It is common for new segment to be partially available or not available at all in
 * all instances.
 * 1) We don't report new segment as unavailable segments.
 * 2) To avoid creating hotspot instances, unavailable instances for new segment won't be excluded for instance
 * selection. When it is selected, we don't serve the new segment.
 * <p>
 * Definition of new segment:
 * 1) Segment pushed more than 5 minutes ago.
 * - If we first see a segment via initialization, we look up segment push time from zookeeper.
 * - If we first see a segment via onAssignmentChange initialization, we use the calling time of onAssignmentChange
 * as approximation.
 * 2) We retire new segment as old when:
 * - The push time is more than 5 minutes ago
 * - Any instance for new segment is in ERROR state
 * - External view for segment converges with ideal state
 *
 * Note that this implementation means:
 * 1) Inconsistent selection of new segments across queries (some queries will serve new segments and others won't).
 * 2) When there is no state update from helix, new segments won't be retired because of the time passing (those with
 * push time more than 5 minutes ago).
 * TODO: refresh new/old segment state where there is no update from helix for long time.
 */
abstract class BaseInstanceSelector implements InstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseInstanceSelector.class);
  // To prevent int overflow, reset the request id once it reaches this value
  private static final long MAX_REQUEST_ID = 1_000_000_000;

  final String _tableNameWithType;
  final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  final BrokerMetrics _brokerMetrics;
  final AdaptiveServerSelector _adaptiveServerSelector;
  final Clock _clock;

  // These 3 variables are the cached states to help accelerate the change processing
  Set<String> _enabledInstances;
  // For old segments, all candidates are online
  // Reduce this map to reduce garbage
  final Map<String, List<SegmentInstanceCandidate>> _oldSegmentCandidatesMap = new HashMap<>();
  Map<String, NewSegmentState> _newSegmentStateMap;

  // _segmentStates is needed for instance selection (multi-threaded), so it is made volatile.
  private volatile SegmentStates _segmentStates;

  BaseInstanceSelector(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics, @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock) {
    _tableNameWithType = tableNameWithType;
    _propertyStore = propertyStore;
    _brokerMetrics = brokerMetrics;
    _adaptiveServerSelector = adaptiveServerSelector;
    _clock = clock;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    _enabledInstances = enabledInstances;
    Map<String, Long> newSegmentPushTimeMap = getNewSegmentPushTimeMapFromZK(idealState, externalView, onlineSegments);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentPushTimeMap);
    refreshSegmentStates();
  }

  /**
   * Returns whether the instance state is online for routing purpose (ONLINE/CONSUMING).
   */
  static boolean isOnlineForRouting(@Nullable String state) {
    return SegmentStateModel.ONLINE.equals(state) || SegmentStateModel.CONSUMING.equals(state);
  }

  /**
   * Returns a map from new segment to their push time based on the ZK metadata.
   */
  Map<String, Long> getNewSegmentPushTimeMapFromZK(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    List<String> potentialNewSegments = new ArrayList<>();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      assert idealStateAssignment.containsKey(segment);
      if (isPotentialNewSegment(idealStateAssignment.get(segment), externalViewAssignment.get(segment))) {
        potentialNewSegments.add(segment);
      }
    }

    // Use push time in ZK metadata to determine whether the potential new segment is newly pushed
    Map<String, Long> newSegmentPushTimeMap = new HashMap<>();
    long nowMillis = _clock.millis();
    String segmentZKMetadataPathPrefix =
        ZKMetadataProvider.constructPropertyStorePathForResource(_tableNameWithType) + "/";
    List<String> segmentZKMetadataPaths = new ArrayList<>(potentialNewSegments.size());
    for (String segment : potentialNewSegments) {
      segmentZKMetadataPaths.add(segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT, false);
    for (ZNRecord record : znRecords) {
      if (record == null) {
        continue;
      }
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(record);
      long pushTimeMillis = segmentZKMetadata.getPushTime();
      if (InstanceSelector.isNewSegment(pushTimeMillis, nowMillis)) {
        newSegmentPushTimeMap.put(segmentZKMetadata.getSegmentName(), pushTimeMillis);
      }
    }
    LOGGER.info("Got {} new segments: {} for table: {} by reading ZK metadata, current time: {}",
        newSegmentPushTimeMap.size(), newSegmentPushTimeMap, _tableNameWithType, nowMillis);
    return newSegmentPushTimeMap;
  }

  /**
   * Returns whether a segment is qualified as a new segment.
   * A segment is count as old when:
   * - Any instance for the segment is in ERROR state
   * - External view for the segment converges with ideal state
   */
  static boolean isPotentialNewSegment(Map<String, String> idealStateInstanceStateMap,
      @Nullable Map<String, String> externalViewInstanceStateMap) {
    if (externalViewInstanceStateMap == null) {
      return true;
    }
    boolean hasConverged = true;
    // Only track ONLINE/CONSUMING instances within the ideal state
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      if (isOnlineForRouting(entry.getValue())) {
        String externalViewState = externalViewInstanceStateMap.get(entry.getKey());
        if (externalViewState == null || externalViewState.equals(SegmentStateModel.OFFLINE)) {
          hasConverged = false;
        } else if (externalViewState.equals(SegmentStateModel.ERROR)) {
          return false;
        }
      }
    }
    return !hasConverged;
  }

  /**
   * Returns the online instances for routing purpose.
   */
  static TreeSet<String> getOnlineInstances(Map<String, String> idealStateInstanceStateMap,
      Map<String, String> externalViewInstanceStateMap) {
    TreeSet<String> onlineInstances = new TreeSet<>();
    // Only track ONLINE/CONSUMING instances within the ideal state
    for (Map.Entry<String, String> entry : idealStateInstanceStateMap.entrySet()) {
      String instance = entry.getKey();
      // NOTE: DO NOT check if EV matches IS because it is a valid state when EV is CONSUMING while IS is ONLINE
      if (isOnlineForRouting(entry.getValue()) && isOnlineForRouting(externalViewInstanceStateMap.get(instance))) {
        onlineInstances.add(instance);
      }
    }
    return onlineInstances;
  }

  /**
   * Converts the given map into a sorted map if needed.
   */
  static SortedMap<String, String> convertToSortedMap(Map<String, String> map) {
    if (map instanceof SortedMap) {
      return (SortedMap<String, String>) map;
    } else {
      return new TreeMap<>(map);
    }
  }

  /**
   * Updates the segment maps based on the given ideal state, external view, online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and pre-selected by the {@link SegmentPreSelector}) and new segments.
   * After this update:
   * - Old segments' online instances should be tracked in _oldSegmentCandidatesMap
   * - New segments' state (push time and candidate instances) should be tracked in _newSegmentStateMap
   */
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentPushTimeMap) {
    _oldSegmentCandidatesMap.clear();
    _newSegmentStateMap = new HashMap<>(HashUtil.getHashMapCapacity(newSegmentPushTimeMap.size()));

    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      Long newSegmentPushTimeMillis = newSegmentPushTimeMap.get(segment);
      Map<String, String> externalViewInstanceStateMap = externalViewAssignment.get(segment);
      if (externalViewInstanceStateMap == null) {
        if (newSegmentPushTimeMillis != null) {
          // New segment
          List<SegmentInstanceCandidate> candidates = new ArrayList<>(idealStateInstanceStateMap.size());
          for (Map.Entry<String, String> entry : convertToSortedMap(idealStateInstanceStateMap).entrySet()) {
            if (isOnlineForRouting(entry.getValue())) {
              candidates.add(new SegmentInstanceCandidate(entry.getKey(), false));
            }
          }
          _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentPushTimeMillis, candidates));
        } else {
          // Old segment
          _oldSegmentCandidatesMap.put(segment, Collections.emptyList());
        }
      } else {
        TreeSet<String> onlineInstances = getOnlineInstances(idealStateInstanceStateMap, externalViewInstanceStateMap);
        if (newSegmentPushTimeMillis != null) {
          // New segment
          List<SegmentInstanceCandidate> candidates = new ArrayList<>(idealStateInstanceStateMap.size());
          for (Map.Entry<String, String> entry : convertToSortedMap(idealStateInstanceStateMap).entrySet()) {
            if (isOnlineForRouting(entry.getValue())) {
              String instance = entry.getKey();
              candidates.add(new SegmentInstanceCandidate(instance, onlineInstances.contains(instance)));
            }
          }
          _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentPushTimeMillis, candidates));
        } else {
          // Old segment
          List<SegmentInstanceCandidate> candidates = new ArrayList<>(onlineInstances.size());
          for (String instance : onlineInstances) {
            candidates.add(new SegmentInstanceCandidate(instance, true));
          }
          _oldSegmentCandidatesMap.put(segment, candidates);
        }
      }
    }
  }

  /**
   * Refreshes the _segmentStates based on the in-memory states.
   * Note that the whole _segmentStates has to be updated together to avoid partial state update.
   **/
  void refreshSegmentStates() {
    Map<String, List<SegmentInstanceCandidate>> instanceCandidatesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(_oldSegmentCandidatesMap.size() + _newSegmentStateMap.size()));
    Set<String> unavailableSegments = new HashSet<>();

    for (Map.Entry<String, List<SegmentInstanceCandidate>> entry : _oldSegmentCandidatesMap.entrySet()) {
      String segment = entry.getKey();
      List<SegmentInstanceCandidate> candidates = entry.getValue();
      List<SegmentInstanceCandidate> enabledCandidates = new ArrayList<>(candidates.size());
      for (SegmentInstanceCandidate candidate : candidates) {
        if (_enabledInstances.contains(candidate.getInstance())) {
          enabledCandidates.add(candidate);
        }
      }
      if (!enabledCandidates.isEmpty()) {
        instanceCandidatesMap.put(segment, enabledCandidates);
      } else {
        List<String> candidateInstances = new ArrayList<>(candidates.size());
        for (SegmentInstanceCandidate candidate : candidates) {
          candidateInstances.add(candidate.getInstance());
        }
        LOGGER.warn("Failed to find servers hosting old segment: {} for table: {} "
                + "(all candidate instances: {} are disabled, counting segment as unavailable)", segment,
            _tableNameWithType, candidateInstances);
        unavailableSegments.add(segment);
        _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
      }
    }

    for (Map.Entry<String, NewSegmentState> entry : _newSegmentStateMap.entrySet()) {
      String segment = entry.getKey();
      NewSegmentState newSegmentState = entry.getValue();
      List<SegmentInstanceCandidate> candidates = newSegmentState.getCandidates();
      List<SegmentInstanceCandidate> enabledCandidates = new ArrayList<>(candidates.size());
      for (SegmentInstanceCandidate candidate : candidates) {
        if (_enabledInstances.contains(candidate.getInstance())) {
          enabledCandidates.add(candidate);
        }
      }
      if (!enabledCandidates.isEmpty()) {
        instanceCandidatesMap.put(segment, enabledCandidates);
      } else {
        // Do not count new segment as unavailable
        List<String> candidateInstances = new ArrayList<>(candidates.size());
        for (SegmentInstanceCandidate candidate : candidates) {
          candidateInstances.add(candidate.getInstance());
        }
        LOGGER.info("Failed to find servers hosting new segment: {} for table: {} "
                + "(all candidate instances: {} are disabled, but not counting new segment as unavailable)", segment,
            _tableNameWithType, candidateInstances);
      }
    }

    _segmentStates = new SegmentStates(instanceCandidatesMap, unavailableSegments);
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
    refreshSegmentStates();
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
    Map<String, Long> newSegmentPushTimeMap =
        getNewSegmentPushTimeMapFromExistingStates(idealState, externalView, onlineSegments);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentPushTimeMap);
    refreshSegmentStates();
  }

  /**
   * Returns a map from new segment to their push time based on the existing in-memory states.
   */
  Map<String, Long> getNewSegmentPushTimeMapFromExistingStates(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    Map<String, Long> newSegmentPushTimeMap = new HashMap<>();
    long nowMillis = _clock.millis();
    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      NewSegmentState newSegmentState = _newSegmentStateMap.get(segment);
      long pushTimeMillis = 0;
      if (newSegmentState != null) {
        // It was a new segment before, check the push time and segment state to see if it is still a new segment
        if (InstanceSelector.isNewSegment(newSegmentState.getPushTimeMillis(), nowMillis)) {
          pushTimeMillis = newSegmentState.getPushTimeMillis();
        }
      } else if (!_oldSegmentCandidatesMap.containsKey(segment)) {
        // This is the first time we see this segment, use the current time as the push time
        pushTimeMillis = nowMillis;
      }
      // For recently pushed segment, check if it is qualified as new segment
      if (pushTimeMillis > 0) {
        assert idealStateAssignment.containsKey(segment);
        if (isPotentialNewSegment(idealStateAssignment.get(segment), externalViewAssignment.get(segment))) {
          newSegmentPushTimeMap.put(segment, pushTimeMillis);
        }
      }
    }
    LOGGER.info("Got {} new segments: {} for table: {} by processing existing states, current time: {}",
        newSegmentPushTimeMap.size(), newSegmentPushTimeMap, _tableNameWithType, nowMillis);
    return newSegmentPushTimeMap;
  }

  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments, long requestId) {
    Map<String, String> queryOptions =
        (brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().getQueryOptions() != null)
            ? brokerRequest.getPinotQuery().getQueryOptions() : Collections.emptyMap();
    int requestIdInt = (int) (requestId % MAX_REQUEST_ID);
    // Copy the volatile reference so that segmentToInstanceMap and unavailableSegments can have a consistent view of
    // the state.
    SegmentStates segmentStates = _segmentStates;
    Map<String, String> segmentToInstanceMap = select(segments, requestIdInt, segmentStates, queryOptions);
    Set<String> unavailableSegments = segmentStates.getUnavailableSegments();
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
   * Selects the server instances for the given segments based on the request id and segment states. Returns a map
   * from segment to selected server instance hosting the segment.
   */
  abstract Map<String, String> select(List<String> segments, int requestId, SegmentStates segmentStates,
      Map<String, String> queryOptions);
}
