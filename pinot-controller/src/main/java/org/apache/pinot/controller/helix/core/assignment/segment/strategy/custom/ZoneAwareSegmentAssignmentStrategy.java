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
package org.apache.pinot.controller.helix.core.assignment.segment.strategy.custom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Zone-aware segment assignment strategy that ensures segment replicas are distributed across different availability zones
 * to achieve better fault tolerance.
 * <p>For a table with N replicas, the strategy attempts to place replicas across N different availability zones if possible.
 * <p>If there are fewer zones than replicas, multiple replicas will be placed in the same zone, but as evenly as possible.
 */
public class ZoneAwareSegmentAssignmentStrategy implements SegmentAssignmentStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZoneAwareSegmentAssignmentStrategy.class);
  
  private HelixManager _helixManager;
  private TableConfig _tableConfig;
  private int _replication;
  private String _tableNameWithType;

  @Override
  public void init(HelixManager helixManager, TableConfig tableConfig) {
    _helixManager = helixManager;
    _tableConfig = tableConfig;
    _tableNameWithType = tableConfig.getTableName();
    _replication = tableConfig.getReplication();
    LOGGER.info("Initialized ZoneAwareSegmentAssignmentStrategy for table: {} with replication: {}", 
        _tableNameWithType, _replication);
  }

  @Override
  public List<String> assignSegment(String segmentName, Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType) {
    
    LOGGER.info("Assigning segment: {} with instance partitions: {}, type: {}", 
        segmentName, instancePartitions.getInstancePartitionsName(), instancePartitionsType);
    
    // Get all instances from instance partitions
    List<String> instances = SegmentAssignmentUtils.getInstancesForNonReplicaGroupBasedAssignment(
        instancePartitions, _replication);
    
    // Get instance configs for all instances
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (String instance : instances) {
      InstanceConfig instanceConfig = _helixManager.getHelixDataAccessor().getProperty(
          _helixManager.getHelixDataAccessor().keyBuilder().instanceConfig(instance));
      if (instanceConfig != null) {
        instanceConfigMap.put(instance, instanceConfig);
      }
    }
    
    // Group instances by availability zone
    Map<String, List<InstanceInfo>> instancesByZone = groupInstancesByZone(instances, instanceConfigMap, currentAssignment);
    
    // Get sorted list of zones based on number of available instances
    List<String> sortedZones = new ArrayList<>(instancesByZone.keySet());
    Collections.sort(sortedZones, Comparator.comparingInt(zone -> instancesByZone.get(zone).size()));
    
    // Create a copy of the zone map with instances sorted by load within each zone
    Map<String, List<InstanceInfo>> sortedInstancesByZone = new HashMap<>();
    for (String zone : sortedZones) {
      List<InstanceInfo> zoneInstances = instancesByZone.get(zone);
      zoneInstances.sort(Comparator.comparingInt(InstanceInfo::getSegmentCount));
      sortedInstancesByZone.put(zone, zoneInstances);
    }
    
    int availableZones = sortedZones.size();
    
    LOGGER.info("Assigning segment {} with {} replicas across {} availability zones", 
        segmentName, _replication, availableZones);
    
    // Assign replicas to zones, prioritizing distribution across zones
    List<String> selectedInstances = new ArrayList<>(_replication);
    
    // Case: We have at least as many zones as replicas (ideal scenario)
    if (availableZones >= _replication) {
      // Assign one replica per zone, starting with zones that have fewer instances (for better balancing)
      for (int i = 0; i < _replication; i++) {
        String zone = sortedZones.get(i);
        List<InstanceInfo> zoneInstances = sortedInstancesByZone.get(zone);
        
        if (!zoneInstances.isEmpty()) {
          // Pick the instance with the lowest load in this zone
          InstanceInfo selectedInstance = zoneInstances.get(0);
          selectedInstances.add(selectedInstance.getInstanceName());
          
          // Update the instance's segment count and re-sort the zone's instances
          selectedInstance.incrementSegmentCount();
          Collections.sort(zoneInstances, Comparator.comparingInt(InstanceInfo::getSegmentCount));
        }
      }
    } else {
      // Case: We have fewer zones than replicas
      // First, distribute one replica per zone
      for (int i = 0; i < availableZones; i++) {
        String zone = sortedZones.get(i);
        List<InstanceInfo> zoneInstances = sortedInstancesByZone.get(zone);
        
        if (!zoneInstances.isEmpty()) {
          InstanceInfo selectedInstance = zoneInstances.get(0);
          selectedInstances.add(selectedInstance.getInstanceName());
          
          selectedInstance.incrementSegmentCount();
          Collections.sort(zoneInstances, Comparator.comparingInt(InstanceInfo::getSegmentCount));
        }
      }
      
      // Then distribute remaining replicas across zones, starting with zones that have more instances
      List<String> reverseSortedZones = new ArrayList<>(sortedZones);
      Collections.reverse(reverseSortedZones);
      
      int remainingReplicas = _replication - availableZones;
      int zoneIndex = 0;
      
      while (remainingReplicas > 0) {
        String zone = reverseSortedZones.get(zoneIndex % availableZones);
        List<InstanceInfo> zoneInstances = sortedInstancesByZone.get(zone);
        
        if (!zoneInstances.isEmpty()) {
          InstanceInfo selectedInstance = zoneInstances.get(0);
          selectedInstances.add(selectedInstance.getInstanceName());
          
          selectedInstance.incrementSegmentCount();
          Collections.sort(zoneInstances, Comparator.comparingInt(InstanceInfo::getSegmentCount));
        }
        
        zoneIndex++;
        remainingReplicas--;
      }
    }
    
    LOGGER.info("Assigned segment {} to instances: {}", segmentName, selectedInstances);
    return selectedInstances;
  }

  @Override
  public Map<String, Map<String, String>> reassignSegments(Map<String, Map<String, String>> currentAssignment,
      InstancePartitions instancePartitions, InstancePartitionsType instancePartitionsType) {
    
    LOGGER.info("Rebalancing segments for {} instance partitions: {}", 
        _tableNameWithType, instancePartitions.getInstancePartitionsName());
    
    // Create new assignment map
    Map<String, Map<String, String>> newAssignment = new HashMap<>();
    
    // For each segment in the current assignment
    for (Map.Entry<String, Map<String, String>> entry : currentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      
      // Assign segment using our zone-aware strategy
      List<String> newInstances = assignSegment(segmentName, currentAssignment, instancePartitions, instancePartitionsType);
      
      // Create instance state map with same states as current assignment
      Map<String, String> newInstanceStateMap = new HashMap<>(newInstances.size());
      for (String instance : newInstances) {
        // Copy state from the old assignment or use default of ONLINE
        String state = instanceStateMap.getOrDefault(instance, "ONLINE");
        newInstanceStateMap.put(instance, state);
      }
      
      newAssignment.put(segmentName, newInstanceStateMap);
    }
    
    return newAssignment;
  }

  /**
   * Groups instances by their availability zone and calculates current segment count.
   */
  private Map<String, List<InstanceInfo>> groupInstancesByZone(List<String> instances, 
      Map<String, InstanceConfig> instanceConfigMap, Map<String, Map<String, String>> instanceStates) {
    
    Map<String, List<InstanceInfo>> instancesByZone = new TreeMap<>();
    
    for (String instanceName : instances) {
      InstanceConfig instanceConfig = instanceConfigMap.get(instanceName);
      if (instanceConfig == null) {
        LOGGER.warn("Skip instance {} with null InstanceConfig", instanceName);
        continue;
      }
      
      // Get the failure domain (availability zone) for this instance
      String zone = CommonConstants.DEFAULT_FAILURE_DOMAIN;
      Map<String, String> environmentMap = instanceConfig.getRecord().getMapField(CommonConstants.ENVIRONMENT_IDENTIFIER);
      
      if (environmentMap != null && environmentMap.containsKey(CommonConstants.INSTANCE_FAILURE_DOMAIN)) {
        zone = environmentMap.get(CommonConstants.INSTANCE_FAILURE_DOMAIN);
      }
      
      // Count current segments assigned to this instance
      int segmentCount = 0;
      for (Map<String, String> segmentAssignment : instanceStates.values()) {
        if (segmentAssignment.containsKey(instanceName)) {
          segmentCount++;
        }
      }
      
      // Add instance to zone group
      instancesByZone.computeIfAbsent(zone, k -> new ArrayList<>())
          .add(new InstanceInfo(instanceName, segmentCount));
    }
    
    return instancesByZone;
  }

  /**
   * Helper class to track instance information and current load.
   */
  private static class InstanceInfo {
    private final String _instanceName;
    private int _segmentCount;

    InstanceInfo(String instanceName, int segmentCount) {
      _instanceName = instanceName;
      _segmentCount = segmentCount;
    }

    String getInstanceName() {
      return _instanceName;
    }

    int getSegmentCount() {
      return _segmentCount;
    }

    void incrementSegmentCount() {
      _segmentCount++;
    }
  }
}