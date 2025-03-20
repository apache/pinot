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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZoneAwareSegmentAssignmentStrategyTest {

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final String SEGMENT_NAME = "testSegment";

  @Mock
  private HelixManager _helixManager;
  
  @Mock
  private HelixDataAccessor _helixDataAccessor;
  
  @Mock
  private PropertyKey.Builder _keyBuilder;

  private SegmentAssignmentStrategy _strategy;
  private TableConfig _tableConfig;
  private InstancePartitions _instancePartitions;
  private Map<String, InstanceConfig> _instanceConfigMap;
  private Map<String, PropertyKey> _instanceConfigKeys;

  @BeforeMethod
  public void setUp() {
    // Initialize mocks
    _helixManager = Mockito.mock(HelixManager.class);
    _helixDataAccessor = Mockito.mock(HelixDataAccessor.class);
    _keyBuilder = Mockito.mock(PropertyKey.Builder.class);
    
    // Setup table config
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(3).build();
    
    // Setup instance configs and mocks
    setupInstanceConfigs();
    
    // Setup mocks
    Mockito.when(_helixManager.getHelixDataAccessor()).thenReturn(_helixDataAccessor);
    Mockito.when(_helixDataAccessor.keyBuilder()).thenReturn(_keyBuilder);
    
    // Setup instance config keys and mock getProperty
    for (Map.Entry<String, InstanceConfig> entry : _instanceConfigMap.entrySet()) {
      PropertyKey instanceConfigKey = Mockito.mock(PropertyKey.class);
      _instanceConfigKeys.put(entry.getKey(), instanceConfigKey);
      Mockito.when(_keyBuilder.instanceConfig(entry.getKey())).thenReturn(instanceConfigKey);
      Mockito.when(_helixDataAccessor.getProperty(instanceConfigKey)).thenReturn(entry.getValue());
    }
    
    // Setup strategy
    _strategy = new ZoneAwareSegmentAssignmentStrategy();
    _strategy.init(_helixManager, _tableConfig);
  }
  
  private void setupInstanceConfigs() {
    _instanceConfigMap = new HashMap<>();
    _instanceConfigKeys = new HashMap<>();
    
    // Setup 6 servers across 3 zones (2 servers per zone)
    setupInstanceWithZone(_instanceConfigMap, "server1", "us-west-2a");
    setupInstanceWithZone(_instanceConfigMap, "server2", "us-west-2a");
    setupInstanceWithZone(_instanceConfigMap, "server3", "us-west-2b");
    setupInstanceWithZone(_instanceConfigMap, "server4", "us-west-2b");
    setupInstanceWithZone(_instanceConfigMap, "server5", "us-west-2c");
    setupInstanceWithZone(_instanceConfigMap, "server6", "us-west-2c");
    
    // Create instance partitions with all 6 servers
    _instancePartitions = new InstancePartitions(TABLE_NAME_WITH_TYPE);
    _instancePartitions.setInstances(0, 0, Arrays.asList(
        "server1", "server2", "server3", "server4", "server5", "server6"));
  }

  @Test
  public void testAssignmentWithMultipleZones() {
    // No existing segments
    Map<String, Map<String, String>> currentAssignment = new HashMap<>();
    
    // Test assignment
    List<String> selectedInstances = _strategy.assignSegment(
        SEGMENT_NAME, currentAssignment, _instancePartitions, InstancePartitionsType.OFFLINE);
    
    // Verify we have 3 instances selected
    Assert.assertEquals(selectedInstances.size(), 3);
    
    // Verify instances are from different zones
    Map<String, Integer> zoneCount = countInstancesByZone(selectedInstances, _instanceConfigMap);
    Assert.assertEquals(zoneCount.size(), 3);
    for (int count : zoneCount.values()) {
      Assert.assertEquals(count, 1);
    }
  }
  
  @Test
  public void testAssignmentWithFewerZonesThanReplicas() {
    // Create a new instance partitions with only 4 servers from 2 zones
    InstancePartitions limitedPartitions = new InstancePartitions(TABLE_NAME_WITH_TYPE);
    limitedPartitions.setInstances(0, 0, Arrays.asList("server1", "server2", "server3", "server4"));
    
    // No existing segments
    Map<String, Map<String, String>> currentAssignment = new HashMap<>();
    
    // Test assignment
    List<String> selectedInstances = _strategy.assignSegment(
        SEGMENT_NAME, currentAssignment, limitedPartitions, InstancePartitionsType.OFFLINE);
    
    // Verify we have 3 instances selected
    Assert.assertEquals(selectedInstances.size(), 3);
    
    // Verify instances are distributed across zones (at least one per zone)
    Map<String, Integer> zoneCount = countInstancesByZone(selectedInstances, _instanceConfigMap);
    Assert.assertEquals(zoneCount.size(), 2);
    for (int count : zoneCount.values()) {
      Assert.assertTrue(count >= 1);
    }
    
    // Total count should still be 3
    int totalCount = 0;
    for (int count : zoneCount.values()) {
      totalCount += count;
    }
    Assert.assertEquals(totalCount, 3);
  }
  
  @Test
  public void testLoadBalancing() {
    // Create some existing segment assignments to test load balancing
    Map<String, Map<String, String>> currentAssignment = new HashMap<>();
    
    // Add existing segments to server1, server3, server5
    Map<String, String> segment1Assignment = new HashMap<>();
    segment1Assignment.put("server1", "ONLINE");
    segment1Assignment.put("server3", "ONLINE");
    segment1Assignment.put("server5", "ONLINE");
    currentAssignment.put("segment1", segment1Assignment);
    
    Map<String, String> segment2Assignment = new HashMap<>();
    segment2Assignment.put("server1", "ONLINE");
    segment2Assignment.put("server3", "ONLINE");
    segment2Assignment.put("server5", "ONLINE");
    currentAssignment.put("segment2", segment2Assignment);
    
    // Test assignment
    List<String> selectedInstances = _strategy.assignSegment(
        SEGMENT_NAME, currentAssignment, _instancePartitions, InstancePartitionsType.OFFLINE);
    
    // Verify we have 3 instances selected
    Assert.assertEquals(selectedInstances.size(), 3);
    
    // Verify selection prioritizes less loaded servers in each zone
    Assert.assertTrue(selectedInstances.contains("server2")); // Should prefer less loaded server in zone A
    Assert.assertTrue(selectedInstances.contains("server4")); // Should prefer less loaded server in zone B
    Assert.assertTrue(selectedInstances.contains("server6")); // Should prefer less loaded server in zone C
  }
  
  @Test
  public void testReassignSegments() {
    // Create initial assignment with 2 segments
    Map<String, Map<String, String>> currentAssignment = new HashMap<>();
    
    Map<String, String> segment1Assignment = new HashMap<>();
    segment1Assignment.put("server1", "ONLINE");
    segment1Assignment.put("server3", "ONLINE");
    segment1Assignment.put("server5", "ONLINE");
    currentAssignment.put("segment1", segment1Assignment);
    
    Map<String, String> segment2Assignment = new HashMap<>();
    segment2Assignment.put("server1", "ONLINE");
    segment2Assignment.put("server3", "ONLINE");
    segment2Assignment.put("server6", "ONLINE");
    currentAssignment.put("segment2", segment2Assignment);
    
    // Test reassignment
    Map<String, Map<String, String>> newAssignment = _strategy.reassignSegments(
        currentAssignment, _instancePartitions, InstancePartitionsType.OFFLINE);
    
    // Verify we have the same number of segments
    Assert.assertEquals(newAssignment.size(), currentAssignment.size());
    
    // Verify each segment has the correct number of replicas
    for (Map<String, String> instanceStateMap : newAssignment.values()) {
      Assert.assertEquals(instanceStateMap.size(), 3);
    }
    
    // Verify state is preserved
    for (Map.Entry<String, Map<String, String>> entry : newAssignment.entrySet()) {
      for (Map.Entry<String, String> instanceState : entry.getValue().entrySet()) {
        Assert.assertEquals(instanceState.getValue(), "ONLINE");
      }
    }
  }
  
  private void setupInstanceWithZone(Map<String, InstanceConfig> instanceConfigMap, String instanceName, String zone) {
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    ZNRecord record = instanceConfig.getRecord();
    Map<String, String> environmentMap = Collections.singletonMap(CommonConstants.INSTANCE_FAILURE_DOMAIN, zone);
    record.setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, environmentMap);
    instanceConfigMap.put(instanceName, instanceConfig);
  }
  
  private Map<String, Integer> countInstancesByZone(List<String> instances, Map<String, InstanceConfig> instanceConfigMap) {
    Map<String, Integer> zoneCount = new TreeMap<>();
    
    for (String instance : instances) {
      InstanceConfig instanceConfig = instanceConfigMap.get(instance);
      Map<String, String> environmentMap = instanceConfig.getRecord().getMapField(CommonConstants.ENVIRONMENT_IDENTIFIER);
      String zone = environmentMap.get(CommonConstants.INSTANCE_FAILURE_DOMAIN);
      
      zoneCount.put(zone, zoneCount.getOrDefault(zone, 0) + 1);
    }
    
    return zoneCount;
  }
}