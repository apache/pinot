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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.CONSUMING;
import static org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ERROR;
import static org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.OFFLINE;
import static org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.RealtimeSegmentOnlineOfflineStateModel.ONLINE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class InstanceSelectorTest {

  @Test
  public void testInstanceSelectorFactory() {
    TableConfig tableConfig = mock(TableConfig.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);

    // Routing config is missing
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics) instanceof BalancedInstanceSelector);

    // Instance selector type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics) instanceof BalancedInstanceSelector);

    // Replica-group instance selector should be returned
    when(routingConfig.getInstanceSelectorType()).thenReturn(RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
    assertTrue(InstanceSelectorFactory
        .getInstanceSelector(tableConfig, brokerMetrics) instanceof ReplicaGroupInstanceSelector);

    // Should be backward-compatible with legacy config
    when(routingConfig.getInstanceSelectorType()).thenReturn(null);
    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(routingConfig.getRoutingTableBuilderName())
        .thenReturn(InstanceSelectorFactory.LEGACY_REPLICA_GROUP_OFFLINE_ROUTING);
    assertTrue(InstanceSelectorFactory
        .getInstanceSelector(tableConfig, brokerMetrics) instanceof ReplicaGroupInstanceSelector);
    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    when(routingConfig.getRoutingTableBuilderName())
        .thenReturn(InstanceSelectorFactory.LEGACY_REPLICA_GROUP_REALTIME_ROUTING);
    assertTrue(InstanceSelectorFactory
        .getInstanceSelector(tableConfig, brokerMetrics) instanceof ReplicaGroupInstanceSelector);
  }

  @Test
  public void testInstanceSelector() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BalancedInstanceSelector balancedInstanceSelector = new BalancedInstanceSelector(offlineTableName, brokerMetrics);
    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, brokerMetrics);

    Set<String> enabledInstances = new HashSet<>();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    // NOTE: Online segments is not used in the current implementation
    Set<String> onlineSegments = Collections.emptySet();

    // 'instance0' and 'instance1' are in the same replica-group, 'instance2' and 'instance3' are in the same
    // replica-group; 'instance0' and 'instance2' serve the same segments, 'instance1' and 'instance3' serve the same
    // segments
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    String instance3 = "instance3";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);
    enabledInstances.add(instance3);

    // Add 2 instances with segments in ERROR state
    String errorInstance0 = "errorInstance0";
    String errorInstance1 = "errorInstance1";
    enabledInstances.add(errorInstance0);
    enabledInstances.add(errorInstance1);

    // Add 2 segments to each instance
    //   [segment0, segment1] -> [instance0, instance2, errorInstance0]
    //   [segment2, segment3] -> [instance1, instance3, errorInstance1]
    Map<String, String> instanceStateMap0 = new TreeMap<>();
    instanceStateMap0.put(instance0, ONLINE);
    instanceStateMap0.put(instance2, ONLINE);
    instanceStateMap0.put(errorInstance0, ERROR);
    String segment0 = "segment0";
    String segment1 = "segment1";
    segmentAssignment.put(segment0, instanceStateMap0);
    segmentAssignment.put(segment1, instanceStateMap0);
    Map<String, String> instanceStateMap1 = new TreeMap<>();
    instanceStateMap1.put(instance1, ONLINE);
    instanceStateMap1.put(instance3, ONLINE);
    instanceStateMap1.put(errorInstance1, ERROR);
    String segment2 = "segment2";
    String segment3 = "segment3";
    segmentAssignment.put(segment2, instanceStateMap1);
    segmentAssignment.put(segment3, instanceStateMap1);
    List<String> segments = Arrays.asList(segment0, segment1, segment2, segment3);

    balancedInstanceSelector.init(enabledInstances, externalView, onlineSegments);
    replicaGroupInstanceSelector.init(enabledInstances, externalView, onlineSegments);

    // For the first request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance0
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //   ReplicaGroupInstanceSelector:
    //     segment0 -> instance0
    //     segment1 -> instance0
    //     segment2 -> instance1
    //     segment3 -> instance1
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance0);
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    InstanceSelector.SelectionResult selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the second request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance0
    //     segment2 -> instance3
    //     segment3 -> instance1
    //   ReplicaGroupInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance3
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance2);
    expectedBalancedInstanceSelectorResult.put(segment1, instance0);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Disable instance0
    enabledInstances.remove(instance0);
    balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));
    replicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));

    // For the third request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //   ReplicaGroupInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance1
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance2);
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the fourth request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //   ReplicaGroupInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance3
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance2);
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Remove segment0 and add segment4
    segmentAssignment.remove(segment0);
    String segment4 = "segment4";
    segmentAssignment.put(segment4, instanceStateMap0);
    segments = Arrays.asList(segment1, segment2, segment3, segment4);

    // Requests arrived before changes got picked up

    // For the fifth request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> null
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> null
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the sixth request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //     segment4 -> null
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance3
    //     segment4 -> null
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Process the changes
    balancedInstanceSelector.onExternalViewChange(externalView, onlineSegments);
    replicaGroupInstanceSelector.onExternalViewChange(externalView, onlineSegments);

    // For the seventy request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> instance2
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> instance2
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    expectedBalancedInstanceSelectorResult.put(segment4, instance2);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the eighth request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //     segment4 -> instance2
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance3
    //     segment4 -> instance2
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    expectedBalancedInstanceSelectorResult.put(segment4, instance2);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Re-enable instance0
    enabledInstances.add(instance0);
    balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));
    replicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));

    // For the ninth request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance0
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> instance2
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance0
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> instance0
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance0);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    expectedBalancedInstanceSelectorResult.put(segment4, instance2);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance0);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the tenth request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //     segment4 -> instance0
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance3
    //     segment4 -> instance2
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    expectedBalancedInstanceSelectorResult.put(segment4, instance0);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testUnavailableSegments() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BalancedInstanceSelector balancedInstanceSelector = new BalancedInstanceSelector(offlineTableName, brokerMetrics);

    Set<String> enabledInstances = new HashSet<>();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    // NOTE: Online segments is not used in the current implementation
    Set<String> onlineSegments = Collections.emptySet();

    String instance = "instance";
    String errorInstance = "errorInstance";
    Map<String, String> instanceStateMap = new TreeMap<>();
    instanceStateMap.put(instance, CONSUMING);
    instanceStateMap.put(errorInstance, ERROR);
    String segment = "segment";
    segmentAssignment.put(segment, instanceStateMap);
    List<String> segments = Collections.singletonList(segment);

    // Initialize with no enabled instance, segment should be unavailable
    // {
    //   (disabled) instance: CONSUMING
    //   (disabled) errorInstance: ERROR
    // }
    balancedInstanceSelector.init(enabledInstances, externalView, onlineSegments);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    InstanceSelector.SelectionResult selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
    assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment));

    // Iterate 5 times
    for (int i = 0; i < 5; i++) {

      // Enable the ERROR instance, segment should be unavailable
      // {
      //   (disabled) instance: CONSUMING
      //   (enabled)  errorInstance: ERROR
      // }
      enabledInstances.add(errorInstance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(errorInstance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment));

      // Enable the CONSUMING instance, segment should be available
      // {
      //   (enabled)  instance: CONSUMING
      //   (enabled)  errorInstance: ERROR
      // }
      enabledInstances.add(instance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap(), Collections.singletonMap(segment, instance));
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Change the CONSUMING instance to ONLINE, segment should be available
      // {
      //   (enabled)  instance: ONLINE
      //   (enabled)  errorInstance: ERROR
      // }
      instanceStateMap.put(instance, ONLINE);
      balancedInstanceSelector.onExternalViewChange(externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap(), Collections.singletonMap(segment, instance));
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Change the ONLINE instance to OFFLINE, both segment to instance map and unavailable segment should be empty
      // {
      //   (enabled)  instance: OFFLINE
      //   (enabled)  errorInstance: ERROR
      // }
      instanceStateMap.put(instance, OFFLINE);
      balancedInstanceSelector.onExternalViewChange(externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Disable the OFFLINE instance, segment should be unavailable
      // {
      //   (disabled) instance: OFFLINE
      //   (enabled)  errorInstance: ERROR
      // }
      enabledInstances.remove(instance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment));

      // Change the ERROR instance to ONLINE, segment should be available
      // {
      //   (disabled) instance: OFFLINE
      //   (enabled)  errorInstance: ONLINE
      // }
      instanceStateMap.put(errorInstance, ONLINE);
      balancedInstanceSelector.onExternalViewChange(externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap(), Collections.singletonMap(segment, errorInstance));
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Disable the ONLINE instance, segment should be unavailable
      // {
      //   (disabled) instance: OFFLINE
      //   (disabled) errorInstance: ONLINE
      // }
      enabledInstances.remove(errorInstance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(errorInstance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment));

      // Change back to initial state, segment should be unavailable
      // {
      //   (disabled) instance: CONSUMING
      //   (disabled) errorInstance: ERROR
      // }
      instanceStateMap.put(instance, CONSUMING);
      instanceStateMap.put(errorInstance, ERROR);
      balancedInstanceSelector.onExternalViewChange(externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment));
    }
  }
}
