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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class ZoneAwareInstanceSelectorTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INSTANCE_1 = "instance_1";
  private static final String INSTANCE_2 = "instance_2";
  private static final String INSTANCE_3 = "instance_3";
  private static final String ZONE_A = "zone-a";
  private static final String ZONE_B = "zone-b";
  private static final String ZONE_C = "zone-c";

  @Mock
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Mock
  private BrokerMetrics _brokerMetrics;

  @Mock
  private org.apache.helix.HelixManager _helixManager;

  @Mock
  private org.apache.helix.HelixDataAccessor _helixDataAccessor;

  @Mock
  private org.apache.helix.PropertyKey.Builder _keyBuilder;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    
    // Setup for HelixManager mock
    when(_helixManager.getHelixDataAccessor()).thenReturn(_helixDataAccessor);
    when(_helixDataAccessor.keyBuilder()).thenReturn(_keyBuilder);
  }

  @Test
  public void testSameZoneSelection() {
    // Set up 3 instances in 2 zones
    InstanceConfig instance1Config = new InstanceConfig(INSTANCE_1);
    Map<String, String> instance1Env = new HashMap<>();
    instance1Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_A);
    instance1Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance1Env);
    
    InstanceConfig instance2Config = new InstanceConfig(INSTANCE_2);
    Map<String, String> instance2Env = new HashMap<>();
    instance2Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_A);
    instance2Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance2Env);
    
    InstanceConfig instance3Config = new InstanceConfig(INSTANCE_3);
    Map<String, String> instance3Env = new HashMap<>();
    instance3Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_B);
    instance3Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance3Env);
    
    // Mock HelixManager behavior for getting instance configs
    when(_keyBuilder.instanceConfig(INSTANCE_1)).thenReturn(null);
    when(_keyBuilder.instanceConfig(INSTANCE_2)).thenReturn(null);
    when(_keyBuilder.instanceConfig(INSTANCE_3)).thenReturn(null);
    
    when(_helixDataAccessor.getProperty(null)).thenReturn(instance1Config, instance2Config, instance3Config);

    // Create a zone-aware instance selector with broker in zone-a and 100% same-zone preference
    ZoneAwareInstanceSelector instanceSelector = new ZoneAwareInstanceSelector(
        OFFLINE_TABLE_NAME, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), 
        false, 5, 1.0, false, ZONE_A);
    
    // Setup the Helix manager in the instance selector through reflection
    try {
      java.lang.reflect.Field helixManagerField = instanceSelector.getClass().getSuperclass().getDeclaredField("_helixManager");
      helixManagerField.setAccessible(true);
      helixManagerField.set(instanceSelector, _helixManager);
    } catch (Exception e) {
      fail("Failed to set _helixManager field", e);
    }

    // Create ideal state with all instances
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    Map<String, String> instanceStateMap = new HashMap<>();
    instanceStateMap.put(INSTANCE_1, "ONLINE");
    instanceStateMap.put(INSTANCE_2, "ONLINE");
    instanceStateMap.put(INSTANCE_3, "ONLINE");
    idealState.setInstanceStateMap(SEGMENT_NAME, instanceStateMap);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    // Create external view with the same states as ideal state
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState(SEGMENT_NAME, INSTANCE_1, "ONLINE");
    externalView.setState(SEGMENT_NAME, INSTANCE_2, "ONLINE");
    externalView.setState(SEGMENT_NAME, INSTANCE_3, "ONLINE");

    // Initialize the instance selector
    Set<String> onlineSegments = new HashSet<>(Collections.singletonList(SEGMENT_NAME));
    Set<String> enabledInstances = new HashSet<>(Arrays.asList(INSTANCE_1, INSTANCE_2, INSTANCE_3));
    instanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // Select instances for routing - should prefer instances in the same zone (ZONE_A)
    List<String> segments = Collections.singletonList(SEGMENT_NAME);
    Pair<Map<String, String>, Map<String, String>> selectionResult = 
        instanceSelector.select(segments, 0, instanceSelector._segmentStates, new HashMap<>());
    
    String selectedInstance = selectionResult.getLeft().get(SEGMENT_NAME);
    
    // The selected instance should be in ZONE_A (i.e., it should be either INSTANCE_1 or INSTANCE_2)
    assertTrue(selectedInstance.equals(INSTANCE_1) || selectedInstance.equals(INSTANCE_2), 
        "Selected instance should be in the same zone as the broker");
    
    // Verify metrics are updated
    verify(_brokerMetrics, times(1)).addValueToTableGauge(eq(OFFLINE_TABLE_NAME), 
        eq(ZoneAwareInstanceSelector.METRIC_SAME_ZONE_SERVER_PERCENTAGE), eq(100.0));
  }

  @Test
  public void testStrictZoneMatching() {
    // Set up instance configs for three instances in three different zones
    InstanceConfig instance1Config = new InstanceConfig(INSTANCE_1);
    Map<String, String> instance1Env = new HashMap<>();
    instance1Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_A);
    instance1Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance1Env);
    
    InstanceConfig instance2Config = new InstanceConfig(INSTANCE_2);
    Map<String, String> instance2Env = new HashMap<>();
    instance2Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_B);
    instance2Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance2Env);
    
    InstanceConfig instance3Config = new InstanceConfig(INSTANCE_3);
    Map<String, String> instance3Env = new HashMap<>();
    instance3Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_C);
    instance3Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance3Env);
    
    // Mock HelixManager behavior for getting instance configs
    when(_keyBuilder.instanceConfig(INSTANCE_1)).thenReturn(null);
    when(_keyBuilder.instanceConfig(INSTANCE_2)).thenReturn(null);
    when(_keyBuilder.instanceConfig(INSTANCE_3)).thenReturn(null);
    
    when(_helixDataAccessor.getProperty(null)).thenReturn(instance1Config, instance2Config, instance3Config);

    // Create a zone-aware instance selector with broker in ZONE_A, 100% same-zone preference and strict matching
    ZoneAwareInstanceSelector instanceSelector = new ZoneAwareInstanceSelector(
        OFFLINE_TABLE_NAME, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), 
        false, 5, 1.0, true, ZONE_A);
    
    // Setup the Helix manager in the instance selector through reflection
    try {
      java.lang.reflect.Field helixManagerField = instanceSelector.getClass().getSuperclass().getDeclaredField("_helixManager");
      helixManagerField.setAccessible(true);
      helixManagerField.set(instanceSelector, _helixManager);
    } catch (Exception e) {
      fail("Failed to set _helixManager field", e);
    }

    // Create ideal state where the segment is only available on non-matching zones (ZONE_B, ZONE_C)
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    Map<String, String> instanceStateMap = new HashMap<>();
    // Don't include INSTANCE_1 (the same-zone instance)
    instanceStateMap.put(INSTANCE_2, "ONLINE");
    instanceStateMap.put(INSTANCE_3, "ONLINE");
    idealState.setInstanceStateMap(SEGMENT_NAME, instanceStateMap);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    // Create external view with the same states as ideal state
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState(SEGMENT_NAME, INSTANCE_2, "ONLINE");
    externalView.setState(SEGMENT_NAME, INSTANCE_3, "ONLINE");

    // Initialize the instance selector
    Set<String> onlineSegments = new HashSet<>(Collections.singletonList(SEGMENT_NAME));
    Set<String> enabledInstances = new HashSet<>(Arrays.asList(INSTANCE_2, INSTANCE_3));
    instanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // Since we have strict zone matching enabled but no matching instances, selection should fail
    List<String> segments = Collections.singletonList(SEGMENT_NAME);
    BrokerRequest brokerRequest = new BrokerRequest();
    InstanceSelector.SelectionResult selectionResult = instanceSelector.select(brokerRequest, segments, 123L);

    // With strict zone matching, should result in an empty map
    assertTrue(selectionResult.getSegmentToInstanceMap().getLeft().isEmpty(), 
        "Strict zone matching should result in no selection");
    assertEquals(selectionResult.getUnavailableSegments().size(), 1, 
        "All segments should be marked as unavailable when strict zone matching fails");
    
    // Verify metrics for failure
    verify(_brokerMetrics, times(1)).addMeteredTableValue(
        eq(OFFLINE_TABLE_NAME), eq(org.apache.pinot.common.metrics.BrokerMeter.ZONE_AWARE_ROUTING_FAILURES), eq(1L));
  }
  
  @Test
  public void testPartialZonePreference() {
    // Set up instance configs for two instances in two different zones
    InstanceConfig instance1Config = new InstanceConfig(INSTANCE_1);
    Map<String, String> instance1Env = new HashMap<>();
    instance1Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_A);
    instance1Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance1Env);
    
    InstanceConfig instance2Config = new InstanceConfig(INSTANCE_2);
    Map<String, String> instance2Env = new HashMap<>();
    instance2Env.put(CommonConstants.INSTANCE_FAILURE_DOMAIN, ZONE_B);
    instance2Config.getRecord().setMapField(CommonConstants.ENVIRONMENT_IDENTIFIER, instance2Env);
    
    // Mock HelixManager behavior for getting instance configs
    when(_keyBuilder.instanceConfig(INSTANCE_1)).thenReturn(null);
    when(_keyBuilder.instanceConfig(INSTANCE_2)).thenReturn(null);
    
    when(_helixDataAccessor.getProperty(null)).thenReturn(instance1Config, instance2Config);

    // Create a zone-aware instance selector with broker in ZONE_A and 50% same-zone preference
    ZoneAwareInstanceSelector instanceSelector = new ZoneAwareInstanceSelector(
        OFFLINE_TABLE_NAME, _propertyStore, _brokerMetrics, null, Clock.systemUTC(), 
        false, 5, 0.5, false, ZONE_A);
    
    // Setup the Helix manager in the instance selector through reflection
    try {
      java.lang.reflect.Field helixManagerField = instanceSelector.getClass().getSuperclass().getDeclaredField("_helixManager");
      helixManagerField.setAccessible(true);
      helixManagerField.set(instanceSelector, _helixManager);
    } catch (Exception e) {
      fail("Failed to set _helixManager field", e);
    }

    // Create ideal state with both instances
    IdealState idealState = new IdealState(OFFLINE_TABLE_NAME);
    
    // Create two segments for testing
    String segment1 = "segment1";
    String segment2 = "segment2";
    
    Map<String, String> instanceStateMap1 = new HashMap<>();
    instanceStateMap1.put(INSTANCE_1, "ONLINE");
    instanceStateMap1.put(INSTANCE_2, "ONLINE");
    idealState.setInstanceStateMap(segment1, instanceStateMap1);
    
    Map<String, String> instanceStateMap2 = new HashMap<>();
    instanceStateMap2.put(INSTANCE_1, "ONLINE");
    instanceStateMap2.put(INSTANCE_2, "ONLINE");
    idealState.setInstanceStateMap(segment2, instanceStateMap2);
    
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);

    // Create external view with the same states as ideal state
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.setState(segment1, INSTANCE_1, "ONLINE");
    externalView.setState(segment1, INSTANCE_2, "ONLINE");
    externalView.setState(segment2, INSTANCE_1, "ONLINE");
    externalView.setState(segment2, INSTANCE_2, "ONLINE");

    // Initialize the instance selector
    Set<String> onlineSegments = new HashSet<>(Arrays.asList(segment1, segment2));
    Set<String> enabledInstances = new HashSet<>(Arrays.asList(INSTANCE_1, INSTANCE_2));
    instanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // Select instances for two segments with 50% preference
    List<String> segments = Arrays.asList(segment1, segment2);
    Pair<Map<String, String>, Map<String, String>> selectionResult = 
        instanceSelector.select(segments, 0, instanceSelector._segmentStates, new HashMap<>());
    
    Map<String, String> selectedInstances = selectionResult.getLeft();
    
    // With 50% preference, one segment should be in ZONE_A and one segment could be in either zone
    int sameZoneCount = 0;
    for (String instance : selectedInstances.values()) {
      if (instance.equals(INSTANCE_1)) {
        sameZoneCount++;
      }
    }
    
    // We should have at least 1 segment in the same zone (due to the 50% preference)
    assertTrue(sameZoneCount >= 1, "At least 1 segment should be in same zone due to 50% preference");
    
    // Verify metrics updated with actual percentage
    double expectedPercentage = (sameZoneCount * 100.0) / segments.size();
    verify(_brokerMetrics, times(1)).addValueToTableGauge(
        eq(OFFLINE_TABLE_NAME), eq(ZoneAwareInstanceSelector.METRIC_SAME_ZONE_SERVER_PERCENTAGE), eq(expectedPercentage));
  }
}