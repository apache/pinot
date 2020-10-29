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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.common.utils.HashUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of instance selector which maintains a map from segment to enabled ONLINE/CONSUMING server
 * instances that serves the segment and a set of unavailable segments (no enabled instance or all enabled instances are
 * in ERROR state).
 */
abstract class BaseInstanceSelector implements InstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseInstanceSelector.class);

  // To prevent int overflow, reset the request id once it reaches this value
  private static final int MAX_REQUEST_ID = 1_000_000_000;

  private final AtomicLong _requestId = new AtomicLong();
  private final String _tableNameWithType;
  private final BrokerMetrics _brokerMetrics;

  // These 4 variables are the cached states to help accelerate the change processing
  private Set<String> _enabledInstances;
  private Map<String, List<String>> _segmentToOnlineInstancesMap;
  private Map<String, List<String>> _segmentToOfflineInstancesMap;
  private Map<String, List<String>> _instanceToSegmentsMap;

  // These 2 variables are needed for instance selection (multi-threaded), so make them volatile
  private volatile Map<String, List<String>> _segmentToEnabledInstancesMap;
  private volatile Set<String> _unavailableSegments;

  BaseInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics) {
    _tableNameWithType = tableNameWithType;
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void init(Set<String> enabledInstances, ExternalView externalView, IdealState idealState,
      Set<String> onlineSegments) {
    _enabledInstances = enabledInstances;
    onExternalViewChange(externalView, idealState, onlineSegments);
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
    for (Map.Entry<String, List<String>> entry : segmentToEnabledInstancesMap.entrySet()) {
      String segment = entry.getKey();
      if (segmentsToUpdate.contains(segment)) {
        List<String> enabledInstancesForSegment =
            calculateEnabledInstancesForSegment(segment, _segmentToOnlineInstancesMap.get(segment),
                newUnavailableSegments);
        entry.setValue(enabledInstancesForSegment);
      } else {
        if (currentUnavailableSegments.contains(segment)) {
          newUnavailableSegments.add(segment);
        }
      }
    }
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
  public void onExternalViewChange(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    int numSegments = onlineSegments.size();
    int segmentMapCapacity = HashUtil.getHashMapCapacity(numSegments);
    _segmentToOnlineInstancesMap = new HashMap<>(segmentMapCapacity);
    _segmentToOfflineInstancesMap = new HashMap<>(segmentMapCapacity);
    if (_instanceToSegmentsMap != null) {
      _instanceToSegmentsMap = new HashMap<>(HashUtil.getHashMapCapacity(_instanceToSegmentsMap.size()));
    } else {
      _instanceToSegmentsMap = new HashMap<>();
    }

    // Update the cached maps
    updateSegmentMaps(externalView, idealState, onlineSegments, _segmentToOnlineInstancesMap,
        _segmentToOfflineInstancesMap, _instanceToSegmentsMap);

    // Generate a new map from segment to enabled ONLINE/CONSUMING instances and a new set of unavailable segments (no
    // enabled instance or all enabled instances are in ERROR state)
    Map<String, List<String>> segmentToEnabledInstancesMap = new HashMap<>(segmentMapCapacity);
    Set<String> unavailableSegments = new HashSet<>();
    // NOTE: Put null as the value when there is no enabled instances for a segment so that segmentToEnabledInstancesMap
    // always contains all segments. With this, in onInstancesChange() we can directly iterate over
    // segmentToEnabledInstancesMap.entrySet() and modify the value without changing the map entries.
    for (Map.Entry<String, List<String>> entry : _segmentToOnlineInstancesMap.entrySet()) {
      String segment = entry.getKey();
      List<String> enabledInstancesForSegment =
          calculateEnabledInstancesForSegment(segment, entry.getValue(), unavailableSegments);
      segmentToEnabledInstancesMap.put(segment, enabledInstancesForSegment);
    }

    _segmentToEnabledInstancesMap = segmentToEnabledInstancesMap;
    _unavailableSegments = unavailableSegments;
  }

  /**
   * Updates the segment maps based on the given external view, ideal state and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector).
   */
  void updateSegmentMaps(ExternalView externalView, IdealState idealState, Set<String> onlineSegments,
      Map<String, List<String>> segmentToOnlineInstancesMap, Map<String, List<String>> segmentToOfflineInstancesMap,
      Map<String, List<String>> instanceToSegmentsMap) {
    // Iterate over the external view instead of the online segments so that the map lookups are performed on the
    // HashSet instead of the TreeSet for performance
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      String segment = entry.getKey();
      if (!onlineSegments.contains(segment)) {
        continue;
      }
      // NOTE: Instances will be sorted here because 'instanceStateMap' is a TreeMap.
      Map<String, String> instanceStateMap = entry.getValue();
      List<String> onlineInstances = new ArrayList<>(instanceStateMap.size());
      List<String> offlineInstances = new ArrayList<>();
      segmentToOnlineInstancesMap.put(segment, onlineInstances);
      segmentToOfflineInstancesMap.put(segment, offlineInstances);
      for (Map.Entry<String, String> instanceStateEntry : instanceStateMap.entrySet()) {
        String instance = instanceStateEntry.getKey();
        String state = instanceStateEntry.getValue();
        // Do not track instances in ERROR state
        if (!state.equals(SegmentStateModel.ERROR)) {
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
          if (state.equals(SegmentStateModel.OFFLINE)) {
            offlineInstances.add(instance);
          } else {
            onlineInstances.add(instance);
          }
        }
      }
    }
  }

  /**
   * Calculates the enabled ONLINE/CONSUMING instances for the given segment, and updates the unavailable segments (no
   * enabled instance or all enabled instances are in ERROR state).
   */
  @Nullable
  private List<String> calculateEnabledInstancesForSegment(String segment, List<String> onlineInstancesForSegment,
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
              "Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} are disabled, but find enabled OFFLINE instance: {} from OFFLINE instances: {}, not counting the segment as unavailable)",
              segment, _tableNameWithType, onlineInstancesForSegment, offlineInstance, offlineInstancesForSegment);
          return null;
        }
      }
      LOGGER.warn(
          "Failed to find servers hosting segment: {} for table: {} (all ONLINE/CONSUMING instances: {} and OFFLINE instances: {} are disabled, counting segment as unavailable)",
          segment, _tableNameWithType, onlineInstancesForSegment, offlineInstancesForSegment);
      unavailableSegments.add(segment);
      _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
      return null;
    }
  }

  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments) {
    int requestId = (int) (_requestId.getAndIncrement() % MAX_REQUEST_ID);
    Map<String, String> segmentToInstanceMap = select(segments, requestId, _segmentToEnabledInstancesMap);
    Set<String> unavailableSegments = _unavailableSegments;
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
      Map<String, List<String>> segmentToEnabledInstancesMap);
}
