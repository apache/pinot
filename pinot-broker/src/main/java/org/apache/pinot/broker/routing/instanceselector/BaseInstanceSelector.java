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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.HashUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of instance selector which maintains a map from segment to enabled server instances that serves
 * the segment.
 */
abstract class BaseInstanceSelector implements InstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseInstanceSelector.class);

  // To prevent int overflow, reset the request id once it reaches this value
  private static final int MAX_REQUEST_ID = 1_000_000_000;

  private final AtomicLong _requestId = new AtomicLong();
  private final String _tableNameWithType;
  private final BrokerMetrics _brokerMetrics;

  private volatile Set<String> _enabledInstances;
  private volatile Map<String, List<String>> _segmentToInstancesMap;
  private volatile Map<String, List<String>> _instanceToSegmentsMap;
  private volatile Map<String, List<String>> _segmentToEnabledInstancesMap;

  BaseInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics) {
    _tableNameWithType = tableNameWithType;
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void init(Set<String> enabledInstances, ExternalView externalView, Set<String> onlineSegments) {
    _enabledInstances = enabledInstances;
    onExternalViewChange(externalView, onlineSegments);
  }

  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    _enabledInstances = enabledInstances;

    // Update all segments served by the changed instances
    Set<String> segmentsToUpdate = new HashSet<>();
    Map<String, List<String>> instanceToSegmentsMap = _instanceToSegmentsMap;
    for (String instance : changedInstances) {
      List<String> segments = instanceToSegmentsMap.get(instance);
      if (segments != null) {
        segmentsToUpdate.addAll(segments);
      }
    }

    // Directly return if no segment needs to be updated
    if (segmentsToUpdate.isEmpty()) {
      return;
    }

    // Update the map from segment to enabled instances
    // NOTE: We can directly modify the map because we will only update the values without changing the map entries.
    // Because the map is marked as volatile, the running queries (already accessed the map) might use the enabled
    // instances either before or after the change, which is okay; the following queries (not yet accessed the map) will
    // get the updated value.
    Map<String, List<String>> segmentToInstancesMap = _segmentToInstancesMap;
    Map<String, List<String>> segmentToEnabledInstancesMap = _segmentToEnabledInstancesMap;
    for (Map.Entry<String, List<String>> entry : segmentToEnabledInstancesMap.entrySet()) {
      String segment = entry.getKey();
      if (segmentsToUpdate.contains(segment)) {
        entry.setValue(
            calculateEnabledInstancesForSegment(segment, segmentToInstancesMap.get(segment), enabledInstances));
      }
    }
  }

  @Override
  public void onExternalViewChange(ExternalView externalView, Set<String> onlineSegments) {
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Map<String, List<String>> segmentToInstancesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(segmentAssignment.size()));
    Map<String, List<String>> instanceToSegmentsMap = new HashMap<>();

    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segment = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      // NOTE: 'instances' will be sorted here because 'instanceStateMap' is a TreeMap
      List<String> instances = new ArrayList<>(instanceStateMap.size());
      segmentToInstancesMap.put(segment, instances);
      for (Map.Entry<String, String> instanceStateEntry : instanceStateMap.entrySet()) {
        String instance = instanceStateEntry.getKey();
        String state = instanceStateEntry.getValue();
        if (state.equals(RealtimeSegmentOnlineOfflineStateModel.ONLINE) || state
            .equals(RealtimeSegmentOnlineOfflineStateModel.CONSUMING)) {
          instances.add(instance);
          instanceToSegmentsMap.computeIfAbsent(instance, k -> new ArrayList<>()).add(segment);
        }
      }
    }

    // Generate a new map from segment to enabled instances
    Set<String> enabledInstances = _enabledInstances;
    Map<String, List<String>> segmentToEnabledInstancesMap =
        new HashMap<>(HashUtil.getHashMapCapacity(segmentToInstancesMap.size()));
    // NOTE: Put null as the value when there is no enabled instances for a segment so that segmentToEnabledInstancesMap
    // always contains all segments. With this, in onInstancesChange() we can directly iterate over
    // segmentToEnabledInstancesMap.entrySet() and modify the value without changing the map entries.
    for (Map.Entry<String, List<String>> entry : segmentToInstancesMap.entrySet()) {
      String segment = entry.getKey();
      segmentToEnabledInstancesMap
          .put(segment, calculateEnabledInstancesForSegment(segment, entry.getValue(), enabledInstances));
    }

    _segmentToInstancesMap = segmentToInstancesMap;
    _instanceToSegmentsMap = instanceToSegmentsMap;
    _segmentToEnabledInstancesMap = segmentToEnabledInstancesMap;
  }

  @Nullable
  private List<String> calculateEnabledInstancesForSegment(String segment, List<String> instancesForSegment,
      Set<String> enabledInstances) {
    List<String> enabledInstancesForSegment = new ArrayList<>(instancesForSegment.size());
    for (String instance : instancesForSegment) {
      if (enabledInstances.contains(instance)) {
        enabledInstancesForSegment.add(instance);
      }
    }
    if (!enabledInstancesForSegment.isEmpty()) {
      return enabledInstancesForSegment;
    } else {
      LOGGER.warn("Failed to find servers hosting segment: {} for table: {} (all online instances: {} are disabled)",
          segment, _tableNameWithType, instancesForSegment);
      _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.NO_SERVING_HOST_FOR_SEGMENT, 1);
      return null;
    }
  }

  @Override
  public Map<String, String> select(BrokerRequest brokerRequest, List<String> segments) {
    int requestId = (int) (_requestId.getAndIncrement() % MAX_REQUEST_ID);
    return select(segments, requestId, _segmentToEnabledInstancesMap);
  }

  /**
   * Selects the server instances for the given segments based on the request id and segment to enabled instances map,
   * returns a map from segment to selected server instance hosting the segment.
   * <p>NOTE: {@code segmentToEnabledInstancesMap} might contain {@code null} values (segment with {@code null} enabled
   * instances). If enabled instances are not {@code null}, they are sorted (in alphabetical order).
   */
  abstract Map<String, String> select(List<String> segments, int requestId,
      Map<String, List<String>> segmentToEnabledInstancesMap);
}
