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
package org.apache.pinot.broker.routing.instanceselector.zoneaware;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.broker.routing.instanceselector.BalancedInstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.SegmentInstanceCandidate;
import org.apache.pinot.broker.routing.instanceselector.SegmentStates;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ZoneAwareInstanceSelector is designed to prioritize selecting server instances that are in the same
 * availability zone as the broker, while still ensuring availability and load balancing.
 * <p>It accepts configuration parameters to control how aggressively it biases toward same-zone servers:
 * <ul>
 *   <li>sameZonePreference - A value between 0.0 and 1.0 indicating the preference for same-zone servers
 *       (0.0 means no preference, 1.0 means use only same-zone servers if available)</li>
 *   <li>strictZoneMatch - If true, queries will fail if the sameZonePreference cannot be satisfied</li>
 * </ul>
 */
public class ZoneAwareInstanceSelector extends BalancedInstanceSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZoneAwareInstanceSelector.class);

  // Config constants
  public static final String ZONE_AWARE_INSTANCE_SELECTOR_TYPE = "zoneAware";
  public static final String CONFIG_OF_SAME_ZONE_PREFERENCE = "pinot.broker.routing.sameZonePreference";
  public static final double DEFAULT_SAME_ZONE_PREFERENCE = 0.5; // Default: try to route 50% of queries to same zone
  public static final String CONFIG_OF_STRICT_ZONE_MATCH = "pinot.broker.routing.strictZoneMatch";
  public static final boolean DEFAULT_STRICT_ZONE_MATCH = false; // Default: don't fail queries if same zone preference can't be met
  public static final String CONFIG_OF_BROKER_ZONE = "pinot.broker.zone"; // Manual zone config

  // Metrics names
  public static final String METRIC_SAME_ZONE_SERVER_PERCENTAGE = "sameZoneServerPercentage";
  public static final String METRIC_STRICT_ZONE_MATCH_FAILURE = "strictZoneMatchFailure";

  private final double _sameZonePreference;
  private final boolean _strictZoneMatch;
  private final String _brokerZone;
  private final Map<String, String> _instanceToZoneMap = new HashMap<>();
  
  // The Helix manager for accessing InstanceConfig information
  private HelixManager _helixManager;

  /**
   * Constructor for ZoneAwareInstanceSelector.
   * @param tableNameWithType Table name with type suffix
   * @param propertyStore Helix property store
   * @param brokerMetrics Broker metrics
   * @param adaptiveServerSelector Adaptive server selector (optional)
   * @param clock Clock for timing operations
   * @param useFixedReplica Whether to use fixed replica
   * @param newSegmentExpirationTimeInSeconds Expiration time for new segments
   * @param sameZonePreference Preference for same-zone servers (0.0-1.0)
   * @param strictZoneMatch Whether to fail queries if zone preference can't be met
   * @param brokerZone The availability zone of the broker
   */
  public ZoneAwareInstanceSelector(String tableNameWithType, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics, @Nullable AdaptiveServerSelector adaptiveServerSelector, Clock clock,
      boolean useFixedReplica, long newSegmentExpirationTimeInSeconds, double sameZonePreference,
      boolean strictZoneMatch, String brokerZone) {
    super(tableNameWithType, propertyStore, brokerMetrics, adaptiveServerSelector, clock, useFixedReplica,
        newSegmentExpirationTimeInSeconds);
    
    // Validate and set parameters
    _sameZonePreference = Math.max(0.0, Math.min(1.0, sameZonePreference)); // Clamp between 0.0 and 1.0
    _strictZoneMatch = strictZoneMatch;
    _brokerZone = brokerZone;
    
    LOGGER.info("Initialized ZoneAwareInstanceSelector for table: {} with broker zone: {}, sameZonePreference: {}, "
        + "strictZoneMatch: {}", tableNameWithType, _brokerZone, _sameZonePreference, _strictZoneMatch);
  }
  
  /**
   * Sets the Helix manager used for accessing instance configuration.
   * @param helixManager The Helix manager
   */
  public void setHelixManager(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  @Override
  public void init(Set<String> enabledInstances, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments) {
    super.init(enabledInstances, idealState, externalView, onlineSegments);
    
    // Cache zone information for all enabled instances
    updateInstanceZones(enabledInstances);
  }

  @Override
  public void onInstancesChange(Set<String> enabledInstances, List<String> changedInstances) {
    super.onInstancesChange(enabledInstances, changedInstances);
    
    // Update cached zone information for all instances
    updateInstanceZones(enabledInstances);
  }

  /**
   * Updates the cached mapping from instance to zone for all enabled instances.
   */
  private void updateInstanceZones(Set<String> enabledInstances) {
    _instanceToZoneMap.clear();
    
    for (String instance : enabledInstances) {
      InstanceConfig instanceConfig = _helixManager.getHelixDataAccessor().getProperty(
          _helixManager.getHelixDataAccessor().keyBuilder().instanceConfig(instance));
      
      if (instanceConfig != null) {
        Map<String, String> environmentMap = instanceConfig.getRecord().getMapField(CommonConstants.ENVIRONMENT_IDENTIFIER);
        if (environmentMap != null && environmentMap.containsKey(CommonConstants.INSTANCE_FAILURE_DOMAIN)) {
          String zone = environmentMap.get(CommonConstants.INSTANCE_FAILURE_DOMAIN);
          _instanceToZoneMap.put(instance, zone);
        }
      }
    }
    
    LOGGER.info("Updated zone information for {} instances for table: {}", _instanceToZoneMap.size(), _tableNameWithType);
  }

  @Override
  Pair<Map<String, String>, Map<String, String>> select(List<String> segments, int requestId,
      SegmentStates segmentStates, Map<String, String> queryOptions) {
    
    // If zone awareness is disabled or broker zone is unknown, use the default selection
    if (_sameZonePreference <= 0.0 || _brokerZone == null || _brokerZone.isEmpty()) {
      return super.select(segments, requestId, segmentStates, queryOptions);
    }
    
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>();
    Map<String, String> optionalSegmentToInstanceMap = new HashMap<>();
    
    int totalSegments = segments.size();
    int selectedSameZoneSegments = 0;
    
    // For each segment, try to select an instance in the same zone
    for (String segment : segments) {
      List<SegmentInstanceCandidate> candidates = segmentStates.getCandidates(segment);
      if (CollectionUtils.isEmpty(candidates)) {
        continue;
      }
      
      // Separate candidates into same-zone and different-zone lists
      List<SegmentInstanceCandidate> sameZoneCandidates = new ArrayList<>();
      List<SegmentInstanceCandidate> differentZoneCandidates = new ArrayList<>();
      
      for (SegmentInstanceCandidate candidate : candidates) {
        String instance = candidate.getInstance();
        String zone = _instanceToZoneMap.get(instance);
        
        if (_brokerZone.equals(zone)) {
          sameZoneCandidates.add(candidate);
        } else {
          differentZoneCandidates.add(candidate);
        }
      }
      
      // Determine if we should select from same-zone candidates
      boolean useSameZone = false;
      
      // If we have same-zone candidates, use them based on the preference
      if (!sameZoneCandidates.isEmpty()) {
        // Use same zone if preference is 1.0 (always)
        if (_sameZonePreference >= 1.0) {
          useSameZone = true;
        } 
        // Otherwise, calculate based on current percentage of same-zone segments
        else if (selectedSameZoneSegments < _sameZonePreference * segmentToSelectedInstanceMap.size()) {
          useSameZone = true;
        }
      }
      
      SegmentInstanceCandidate selectedCandidate;
      
      // Select the appropriate candidate
      if (useSameZone) {
        selectedCandidate = selectCandidate(sameZoneCandidates, requestId, queryOptions);
        selectedSameZoneSegments++;
      } else if (!differentZoneCandidates.isEmpty()) {
        selectedCandidate = selectCandidate(differentZoneCandidates, requestId, queryOptions);
      } else if (_strictZoneMatch && segmentToSelectedInstanceMap.size() > 0) {
        // If strict zone matching is enabled and we can't meet the preference, fail the query
        // Only fail if we've already selected some segments (to avoid failing queries for single-segment tables)
        if (_brokerMetrics != null) {
          _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.ZONE_AWARE_ROUTING_FAILURES, 1);
        }
        return Pair.of(new HashMap<>(), new HashMap<>());
      } else {
        // Fall back to any available candidate
        selectedCandidate = selectCandidate(candidates, requestId, queryOptions);
      }
      
      // Add selected candidate to the appropriate map
      if (selectedCandidate.isOnline()) {
        segmentToSelectedInstanceMap.put(segment, selectedCandidate.getInstance());
      } else {
        optionalSegmentToInstanceMap.put(segment, selectedCandidate.getInstance());
      }
    }
    
    // Calculate and emit metrics
    if (!segmentToSelectedInstanceMap.isEmpty() && _brokerMetrics != null) {
      double sameZonePercentage = (double) selectedSameZoneSegments / segmentToSelectedInstanceMap.size();
      _brokerMetrics.addValueToTableGauge(_tableNameWithType, METRIC_SAME_ZONE_SERVER_PERCENTAGE, sameZonePercentage * 100);
    }
    
    return Pair.of(segmentToSelectedInstanceMap, optionalSegmentToInstanceMap);
  }
  
  /**
   * Helper method to select a candidate from a list based on the request ID and options.
   */
  private SegmentInstanceCandidate selectCandidate(List<SegmentInstanceCandidate> candidates, int requestId,
      Map<String, String> queryOptions) {
    if (_adaptiveServerSelector != null) {
      List<String> candidateInstances = new ArrayList<>(candidates.size());
      for (SegmentInstanceCandidate candidate : candidates) {
        candidateInstances.add(candidate.getInstance());
      }
      String selectedInstance = _adaptiveServerSelector.select(candidateInstances);
      return candidates.get(candidateInstances.indexOf(selectedInstance));
    } else {
      int selectedIdx;
      if (isUseFixedReplica(queryOptions)) {
        selectedIdx = _tableNameHashForFixedReplicaRouting % candidates.size();
      } else {
        selectedIdx = requestId % candidates.size();
      }
      return candidates.get(selectedIdx);
    }
  }
  
  @Override
  public SelectionResult select(BrokerRequest brokerRequest, List<String> segments, long requestId) {
    int requestIdHash = (int) (requestId % MAX_REQUEST_ID);
    Map<String, String> queryOptions = QueryOptionsUtils.extractOptionsFromQuery(brokerRequest);
    Pair<Map<String, String>, Map<String, String>> segmentToInstanceMap =
        select(segments, requestIdHash, _segmentStates, queryOptions);
    
    // Handle strict zone matching failure
    if (_strictZoneMatch && segmentToInstanceMap.getLeft().isEmpty() && !segments.isEmpty()) {
      if (_brokerMetrics != null) {
        _brokerMetrics.addMeteredTableValue(_tableNameWithType, BrokerMeter.ZONE_AWARE_ROUTING_FAILURES, 1);
      }
      
      List<String> unavailableSegments = new ArrayList<>(segments);
      return new SelectionResult(Pair.of(new HashMap<>(), new HashMap<>()), unavailableSegments, 0);
    }
    
    return super.select(brokerRequest, segments, requestId);
  }
}