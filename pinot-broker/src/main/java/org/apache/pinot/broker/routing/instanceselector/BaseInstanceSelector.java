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
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.HashUtil;
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
 * 1) Segment created more than 5 minutes ago.
 * - If we first see a segment via initialization, we look up segment creation time from zookeeper.
 * - If we first see a segment via onAssignmentChange initialization, we use the calling time of onAssignmentChange
 * as approximation.
 * 2) We retire new segment as old when:
 * - The creation time is more than 5 minutes ago
 * - Any instance for new segment is in ERROR state
 * - External view for segment converges with ideal state
 *
 * Note that this implementation means:
 * 1) Inconsistent selection of new segments across queries (some queries will serve new segments and others won't).
 * 2) When there is no state update from helix, new segments won't be retired because of the time passing (those with
 * creation time more than 5 minutes ago).
 * TODO: refresh new/old segment state where there is no update from helix for long time.
 */
abstract class BaseInstanceSelector implements InstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseInstanceSelector.class);

  final String _tableNameWithType;
  final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  final AdaptiveServerSelector _adaptiveServerSelector;
  private final BrokerMetrics _brokerMetrics;
  private final Clock _clock;

  // These 3 variables are the cached states to help accelerate the change processing
  private Set<String> _enabledInstances;
  // For old segments, all candidates are online. Reduce this map to reduce garbage
  private final Map<String, List<SegmentInstanceCandidate>> _oldSegmentCandidatesMap = new HashMap<>();
  private Map<String, NewSegmentState> _newSegmentStateMap;

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
    Map<String, Long> newSegmentCreationTimeMap =
        InstanceSelectorUtils.getNewSegmentCreationTimeMapFromZK(_tableNameWithType, idealState, externalView,
            onlineSegments, _propertyStore, _clock);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    refreshSegmentStates();
  }

  /**
   * Updates the segment maps based on the given ideal state, external view, online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and pre-selected by the {@link SegmentPreSelector}) and new segments.
   * After this update:
   * - Old segments' online instances should be tracked in _oldSegmentCandidatesMap
   * - New segments' state (creation time and candidate instances) should be tracked in _newSegmentStateMap
   */
  void updateSegmentMaps(IdealState idealState, ExternalView externalView, Set<String> onlineSegments,
      Map<String, Long> newSegmentCreationTimeMap) {
    _oldSegmentCandidatesMap.clear();
    _newSegmentStateMap = new HashMap<>(HashUtil.getHashMapCapacity(newSegmentCreationTimeMap.size()));

    Map<String, Map<String, String>> idealStateAssignment = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> externalViewAssignment = externalView.getRecord().getMapFields();
    for (String segment : onlineSegments) {
      Map<String, String> idealStateInstanceStateMap = idealStateAssignment.get(segment);
      assert idealStateInstanceStateMap != null;
      Map<String, String> externalViewInstanceStateMap = externalViewAssignment.get(segment);
      if (externalViewInstanceStateMap == null) {
        if (newSegmentCreationTimeMap.containsKey(segment)) {
          // New segment
          List<SegmentInstanceCandidate> candidates =
              InstanceSelectorUtils.getCandidatesForNewSegment(idealStateInstanceStateMap, instance -> false);
          _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentCreationTimeMap.get(segment), candidates));
        } else {
          // Old segment
          _oldSegmentCandidatesMap.put(segment, Collections.emptyList());
        }
      } else {
        TreeSet<String> onlineInstances =
            InstanceSelectorUtils.getOnlineInstances(idealStateInstanceStateMap, externalViewInstanceStateMap);
        if (newSegmentCreationTimeMap.containsKey(segment)) {
          // New segment
          List<SegmentInstanceCandidate> candidates =
              InstanceSelectorUtils.getCandidatesForNewSegment(idealStateInstanceStateMap, onlineInstances::contains);
          _newSegmentStateMap.put(segment, new NewSegmentState(newSegmentCreationTimeMap.get(segment), candidates));
        } else {
          // Old segment
          List<SegmentInstanceCandidate> candidates =
              onlineInstances.stream().map(instance -> new SegmentInstanceCandidate(instance, true))
                  .collect(Collectors.toList());
          _oldSegmentCandidatesMap.put(segment, candidates);
        }
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Got _newSegmentStateMap: {}, _oldSegmentCandidatesMap: {}", _newSegmentStateMap.keySet(),
          _oldSegmentCandidatesMap.keySet());
    }
  }

  /**
   * Refreshes the _segmentStates based on the in-memory states.
   * Note that the whole _segmentStates has to be updated together to avoid partial state update.
   **/
  void refreshSegmentStates() {
    Map<String, List<SegmentInstanceCandidate>> instanceCandidatesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(_oldSegmentCandidatesMap.size() + _newSegmentStateMap.size()));
    Set<String> servingInstances = new HashSet<>();
    Set<String> unavailableSegments = new HashSet<>();

    for (Map.Entry<String, List<SegmentInstanceCandidate>> entry : _oldSegmentCandidatesMap.entrySet()) {
      String segment = entry.getKey();
      List<SegmentInstanceCandidate> candidates = entry.getValue();
      List<SegmentInstanceCandidate> enabledCandidates =
          InstanceSelectorUtils.getEnabledCandidatesAndAddToServingInstances(candidates, _enabledInstances,
              servingInstances);
      if (!enabledCandidates.isEmpty()) {
        instanceCandidatesMap.put(segment, enabledCandidates);
      } else {
        List<String> candidateInstances =
            candidates.stream().map(SegmentInstanceCandidate::getInstance).collect(Collectors.toList());
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
      List<SegmentInstanceCandidate> enabledCandidates =
          InstanceSelectorUtils.getEnabledCandidatesAndAddToServingInstances(candidates, _enabledInstances,
              servingInstances);
      if (!enabledCandidates.isEmpty()) {
        instanceCandidatesMap.put(segment, enabledCandidates);
      } else {
        // Do not count new segment as unavailable
        List<String> candidateInstances =
            candidates.stream().map(SegmentInstanceCandidate::getInstance).collect(Collectors.toList());
        LOGGER.info("Failed to find servers hosting new segment: {} for table: {} "
                + "(all candidate instances: {} are disabled, but not counting new segment as unavailable)", segment,
            _tableNameWithType, candidateInstances);
      }
    }

    _segmentStates = new SegmentStates(instanceCandidatesMap, servingInstances, unavailableSegments);
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
    Map<String, Long> newSegmentCreationTimeMap =
        InstanceSelectorUtils.getNewSegmentCreationTimeMapFromExistingStates(_tableNameWithType, idealState,
            externalView, onlineSegments, _newSegmentStateMap, _oldSegmentCandidatesMap, _clock);
    updateSegmentMaps(idealState, externalView, onlineSegments, newSegmentCreationTimeMap);
    refreshSegmentStates();
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
    Pair<Map<String, String>, Map<String, String>> segmentToInstanceMap =
        select(segments, requestIdInt, segmentStates, queryOptions);
    Set<String> unavailableSegments = segmentStates.getUnavailableSegments();
    if (unavailableSegments.isEmpty()) {
      return new SelectionResult(segmentToInstanceMap, Collections.emptyList(), 0);
    } else {
      List<String> unavailableSegmentsForRequest = new ArrayList<>();
      for (String segment : segments) {
        if (unavailableSegments.contains(segment)) {
          unavailableSegmentsForRequest.add(segment);
        }
      }
      return new SelectionResult(segmentToInstanceMap, unavailableSegmentsForRequest, 0);
    }
  }

  @Override
  public Set<String> getServingInstances() {
    return _segmentStates.getServingInstances();
  }

  /**
   * Selects the server instances for the given segments based on the request id and segment states. Returns two maps
   * from segment to selected server instance hosting the segment. The 2nd map is for optional segments. The optional
   * segments are used to get the new segments that is not online yet. Instead of simply skipping them by broker at
   * routing time, we can send them to servers and let servers decide how to handle them.
   */
  abstract Pair<Map<String, String>, Map<String, String>/*optional segments*/> select(List<String> segments,
      int requestId, SegmentStates segmentStates, Map<String, String> queryOptions);
}
