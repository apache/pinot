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
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.controller.helix.core.assignment.segment.strategy.SegmentAssignmentStrategy;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZoneAwareSegmentAssignmentStrategyTest {

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  @Mock
  private HelixManager _helixManager;

  private SegmentAssignmentStrategy _strategy;
  private TableConfig _tableConfig;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    _strategy = new ZoneAwareSegmentAssignmentStrategy();
    _strategy.init(_helixManager, _tableConfig);
  }

  @Test
  public void testAssignmentWithMultipleZones() {
    // Create instances in different zones
    List<String> instances = Arrays.asList("server1", "server2", "server3", "server4", "server5", "server6");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    
    // Setup 6 servers across 3 zones (2 servers per zone)
    setupInstanceWithZone(instanceConfigMap, "server1", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server2", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server3", "us-west-2b");
    setupInstanceWithZone(instanceConfigMap, "server4", "us-west-2b");
    setupInstanceWithZone(instanceConfigMap, "server5", "us-west-2c");
    setupInstanceWithZone(instanceConfigMap, "server6", "us-west-2c");
    
    // No existing segments
    Map<String, Map<String, String>> instanceStates = new HashMap<>();
    for (String instance : instances) {
      instanceStates.put(instance, new HashMap<>());
    }
    
    // Test with 3 replicas (should assign one replica to each zone)
    List<String> selectedInstances = _strategy.assignSegment(SEGMENT_NAME, instances, instanceStates, instanceConfigMap, 3);
    
    // Verify we have 3 instances selected
    Assert.assertEquals(selectedInstances.size(), 3);
    
    // Verify instances are from different zones
    Map<String, Integer> zoneCount = countInstancesByZone(selectedInstances, instanceConfigMap);
    Assert.assertEquals(zoneCount.size(), 3);
    for (int count : zoneCount.values()) {
      Assert.assertEquals(count, 1);
    }
  }
  
  @Test
  public void testAssignmentWithFewerZonesThanReplicas() {
    // Create instances in different zones
    List<String> instances = Arrays.asList("server1", "server2", "server3", "server4");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    
    // Setup 4 servers across 2 zones (2 servers per zone)
    setupInstanceWithZone(instanceConfigMap, "server1", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server2", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server3", "us-west-2b");
    setupInstanceWithZone(instanceConfigMap, "server4", "us-west-2b");
    
    // No existing segments
    Map<String, Map<String, String>> instanceStates = new HashMap<>();
    for (String instance : instances) {
      instanceStates.put(instance, new HashMap<>());
    }
    
    // Test with 3 replicas (should assign at least one replica to each zone)
    List<String> selectedInstances = _strategy.assignSegment(SEGMENT_NAME, instances, instanceStates, instanceConfigMap, 3);
    
    // Verify we have 3 instances selected
    Assert.assertEquals(selectedInstances.size(), 3);
    
    // Verify instances are distributed across zones (at least one per zone)
    Map<String, Integer> zoneCount = countInstancesByZone(selectedInstances, instanceConfigMap);
    Assert.assertEquals(zoneCount.size(), 2);
    for (int count : zoneCount.values()) {
      Assert.assertTrue(count >= 1);
    }
    
    // Third replica must be in one of the zones
    int totalCount = 0;
    for (int count : zoneCount.values()) {
      totalCount += count;
    }
    Assert.assertEquals(totalCount, 3);
  }
  
  @Test
  public void testLoadBalancing() {
    // Create instances in different zones
    List<String> instances = Arrays.asList("server1", "server2", "server3", "server4", "server5", "server6");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    
    // Setup 6 servers across 3 zones (2 servers per zone)
    setupInstanceWithZone(instanceConfigMap, "server1", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server2", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server3", "us-west-2b");
    setupInstanceWithZone(instanceConfigMap, "server4", "us-west-2b");
    setupInstanceWithZone(instanceConfigMap, "server5", "us-west-2c");
    setupInstanceWithZone(instanceConfigMap, "server6", "us-west-2c");
    
    // Create some existing segment assignments to test load balancing
    Map<String, Map<String, String>> instanceStates = new HashMap<>();
    for (String instance : instances) {
      instanceStates.put(instance, new HashMap<>());
    }
    
    // Add existing segments to server1, server3, server5
    Map<String, String> server1Segments = instanceStates.get("server1");
    server1Segments.put("segment1", "ONLINE");
    server1Segments.put("segment2", "ONLINE");
    
    Map<String, String> server3Segments = instanceStates.get("server3");
    server3Segments.put("segment3", "ONLINE");
    
    Map<String, String> server5Segments = instanceStates.get("server5");
    server5Segments.put("segment4", "ONLINE");
    server5Segments.put("segment5", "ONLINE");
    server5Segments.put("segment6", "ONLINE");
    
    // Test with 3 replicas (should distribute based on load within zones)
    List<String> selectedInstances = _strategy.assignSegment(SEGMENT_NAME, instances, instanceStates, instanceConfigMap, 3);
    
    // Verify we have 3 instances selected
    Assert.assertEquals(selectedInstances.size(), 3);
    
    // Verify selection prioritizes less loaded servers in each zone
    Assert.assertTrue(selectedInstances.contains("server2")); // Should prefer less loaded server in zone A
    Assert.assertTrue(selectedInstances.contains("server4")); // Should prefer less loaded server in zone B
    Assert.assertTrue(selectedInstances.contains("server6")); // Should prefer less loaded server in zone C
  }
  
  @Test
  public void testSameZoneAllocation() {
    // Create instances in the same zone
    List<String> instances = Arrays.asList("server1", "server2", "server3");
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    
    // Setup 3 servers all in the same zone
    setupInstanceWithZone(instanceConfigMap, "server1", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server2", "us-west-2a");
    setupInstanceWithZone(instanceConfigMap, "server3", "us-west-2a");
    
    // No existing segments
    Map<String, Map<String, String>> instanceStates = new HashMap<>();
    for (String instance : instances) {
      instanceStates.put(instance, new HashMap<>());
    }
    
    // Test with 3 replicas (all must be in the same zone)
    List<String> selectedInstances = _strategy.assignSegment(SEGMENT_NAME, instances, instanceStates, instanceConfigMap, 3);
    
    // Verify we have 3 instances selected
    Assert.assertEquals(selectedInstances.size(), 3);
    
    // All instances should be from the same zone
    Map<String, Integer> zoneCount = countInstancesByZone(selectedInstances, instanceConfigMap);
    Assert.assertEquals(zoneCount.size(), 1);
    Assert.assertEquals(zoneCount.get("us-west-2a").intValue(), 3);
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