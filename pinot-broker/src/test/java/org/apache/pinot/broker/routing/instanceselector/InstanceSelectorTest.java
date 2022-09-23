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
import org.apache.helix.model.IdealState;
import org.apache.pinot.broker.routing.adaptiveserverselector.AdaptiveServerSelector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class InstanceSelectorTest {
  private final static List<String> SEGMENTS = Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4",
      "segment5", "segment6", "segment7", "segment8", "segment9", "segment10", "segment11");

  @Test
  public void testInstanceSelectorFactory() {
    TableConfig tableConfig = mock(TableConfig.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    AdaptiveServerSelector adaptiveServerSelector = null;

    // Routing config is missing
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics,
        adaptiveServerSelector) instanceof BalancedInstanceSelector);

    // Instance selector type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics,
        adaptiveServerSelector) instanceof BalancedInstanceSelector);

    // Replica-group instance selector should be returned
    when(routingConfig.getInstanceSelectorType()).thenReturn(RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics,
        adaptiveServerSelector) instanceof ReplicaGroupInstanceSelector);

    // Strict replica-group instance selector should be returned
    when(routingConfig.getInstanceSelectorType()).thenReturn(RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics,
        adaptiveServerSelector) instanceof StrictReplicaGroupInstanceSelector);

    // Should be backward-compatible with legacy config
    when(routingConfig.getInstanceSelectorType()).thenReturn(null);
    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        InstanceSelectorFactory.LEGACY_REPLICA_GROUP_OFFLINE_ROUTING);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics,
        adaptiveServerSelector) instanceof ReplicaGroupInstanceSelector);
    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        InstanceSelectorFactory.LEGACY_REPLICA_GROUP_REALTIME_ROUTING);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, brokerMetrics,
        adaptiveServerSelector) instanceof ReplicaGroupInstanceSelector);
  }

  @Test
  public void testInstanceSelector() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    AdaptiveServerSelector adaptiveServerSelector = null;
    BalancedInstanceSelector balancedInstanceSelector = new BalancedInstanceSelector(offlineTableName, brokerMetrics,
        adaptiveServerSelector);
    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);
    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

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
    String segment0 = "segment0";
    String segment1 = "segment1";
    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    idealStateInstanceStateMap0.put(instance0, ONLINE);
    idealStateInstanceStateMap0.put(instance2, ONLINE);
    idealStateInstanceStateMap0.put(errorInstance0, ONLINE);
    idealStateSegmentAssignment.put(segment0, idealStateInstanceStateMap0);
    idealStateSegmentAssignment.put(segment1, idealStateInstanceStateMap0);
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();
    externalViewInstanceStateMap0.put(instance0, ONLINE);
    externalViewInstanceStateMap0.put(instance2, ONLINE);
    externalViewInstanceStateMap0.put(errorInstance0, ERROR);
    externalViewSegmentAssignment.put(segment0, externalViewInstanceStateMap0);
    externalViewSegmentAssignment.put(segment1, externalViewInstanceStateMap0);
    onlineSegments.add(segment0);
    onlineSegments.add(segment1);
    String segment2 = "segment2";
    String segment3 = "segment3";
    Map<String, String> idealStateInstanceStateMap1 = new TreeMap<>();
    idealStateInstanceStateMap1.put(instance1, ONLINE);
    idealStateInstanceStateMap1.put(instance3, ONLINE);
    idealStateInstanceStateMap1.put(errorInstance1, ONLINE);
    idealStateSegmentAssignment.put(segment2, idealStateInstanceStateMap1);
    idealStateSegmentAssignment.put(segment3, idealStateInstanceStateMap1);
    Map<String, String> externalViewInstanceStateMap1 = new TreeMap<>();
    externalViewInstanceStateMap1.put(instance1, ONLINE);
    externalViewInstanceStateMap1.put(instance3, ONLINE);
    externalViewInstanceStateMap1.put(errorInstance1, ERROR);
    externalViewSegmentAssignment.put(segment2, externalViewInstanceStateMap1);
    externalViewSegmentAssignment.put(segment3, externalViewInstanceStateMap1);
    onlineSegments.add(segment2);
    onlineSegments.add(segment3);
    List<String> segments = Arrays.asList(segment0, segment1, segment2, segment3);

    balancedInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);

    // For the 1st request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance0
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
    //     segment0 -> instance0
    //     segment1 -> instance0
    //     segment2 -> instance1
    //     segment3 -> instance1
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(null);
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the 2nd request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance0
    //     segment2 -> instance3
    //     segment3 -> instance1
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Disable instance0
    enabledInstances.remove(instance0);
    balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));
    replicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));
    strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));

    // For the 3rd request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the 4th request:
    //   BalancedInstanceSelector:
    //     segment0 -> instance2
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Remove segment0 and add segment4
    idealStateSegmentAssignment.remove(segment0);
    externalViewSegmentAssignment.remove(segment0);
    onlineSegments.remove(segment0);
    String segment4 = "segment4";
    idealStateSegmentAssignment.put(segment4, idealStateInstanceStateMap0);
    externalViewSegmentAssignment.put(segment4, externalViewInstanceStateMap0);
    onlineSegments.add(segment4);
    segments = Arrays.asList(segment1, segment2, segment3, segment4);

    // Requests arrived before changes got picked up

    // For the 5th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> null
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the 6th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //     segment4 -> null
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Process the changes
    balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // For the 7th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> instance2
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the 8th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //     segment4 -> instance2
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Re-enable instance0
    enabledInstances.add(instance0);
    balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));
    replicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));
    strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance0));

    // For the 9th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance0
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> instance2
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the 10th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //     segment4 -> instance0
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // segment1 run into ERROR state on instance0
    Map<String, String> externalViewInstanceStateMap2 = new TreeMap<>();
    externalViewInstanceStateMap2.put(instance0, ERROR);
    externalViewInstanceStateMap2.put(instance2, ONLINE);
    externalViewInstanceStateMap2.put(errorInstance0, ERROR);
    externalViewSegmentAssignment.put(segment1, externalViewInstanceStateMap2);
    balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // For the 11th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> instance2
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> instance0
    //   StrictReplicaGroupInstanceSelector:
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
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance0);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    Map<String, String> expectedStrictReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedStrictReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // For the 12th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance3
    //     segment4 -> instance0
    //   ReplicaGroupInstanceSelector/StrictReplicaGroupInstanceSelector:
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
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsToQuery() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    // numReplicas = 3, fanning the query to 2 replica groups
    queryOptions.put("numReplicaGroupsToQuery", "2");
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);
    AdaptiveServerSelector adaptiveServerSelector = null;

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
      externalViewInstanceStateMap0.put(instance, ONLINE);
    }

    List<String> segments = getSegments();
    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment: segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    //   ReplicaGroupInstanceSelector
    //     segment0 -> instance0
    //     segment2 -> instance0
    //     segment4 -> instance0
    //     segment6 -> instance0
    //     segment8 -> instance0
    //     segment10 -> instance0
    //     segment1 -> instance1
    //     segment3 -> instance1
    //     segment5 -> instance1
    //     segment7 -> instance1
    //     segment9 -> instance1
    //     segment11 -> instance1

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(0), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(1), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(2), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(3), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(4), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(5), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(6), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(7), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(8), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(9), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(10), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(11), instance1);
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }


  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsToQueryGreaterThanReplicas() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("numReplicaGroupsToQuery", "4");
    AdaptiveServerSelector adaptiveServerSelector = null;

    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    List<String> segments = getSegments();

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
      externalViewInstanceStateMap0.put(instance, ONLINE);
    }

    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment: segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    //   ReplicaGroupInstanceSelector
    //     segment0 -> instance0
    //     segment3 -> instance0
    //     segment6 -> instance0
    //     segment9 -> instance0
    //     segment1 -> instance1
    //     segment4 -> instance1
    //     segment7 -> instance1
    //     segment10 -> instance1
    //     segment2 -> instance2
    //     segment5 -> instance2
    //     segment8 -> instance2
    //     segment11 -> instance2

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(0), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(1), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(2), instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(3), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(4), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(5), instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(6), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(7), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(8), instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(9), instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(10), instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segments.get(11), instance2);
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsNotSet() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    AdaptiveServerSelector adaptiveServerSelector = null;

    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // 12 online segments with each segment having all 3 instances as online
    // replicas are 3
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    enabledInstances.add(instance0);
    enabledInstances.add(instance1);
    enabledInstances.add(instance2);

    List<String> segments = getSegments();

    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();

    for (String instance : enabledInstances) {
      idealStateInstanceStateMap0.put(instance, ONLINE);
      externalViewInstanceStateMap0.put(instance, ONLINE);
    }

    // add all segments to both idealStateSegmentAssignment and externalViewSegmentAssignment maps and also to online
    // segments
    for (String segment: segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    // since numReplicaGroupsToQuery is not set, first query should go to first replica group,
    // 2nd query should go to next replica group

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    for (String segment: segments) {
      expectedReplicaGroupInstanceSelectorResult.put(segment, instance0);
    }
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);

    for (String segment: segments) {
      expectedReplicaGroupInstanceSelectorResult.put(segment, instance1);
    }
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
  }

  @Test
  public void testUnavailableSegments() {
    String offlineTableName = "testTable_OFFLINE";
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    AdaptiveServerSelector adaptiveServerSelector = null;
    BalancedInstanceSelector balancedInstanceSelector = new BalancedInstanceSelector(offlineTableName, brokerMetrics,
        adaptiveServerSelector);
    // ReplicaGroupInstanceSelector has the same behavior as BalancedInstanceSelector for the unavailable segments
    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(offlineTableName, brokerMetrics, adaptiveServerSelector);

    Set<String> enabledInstances = new HashSet<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    String instance = "instance";
    String errorInstance = "errorInstance";
    String segment0 = "segment0";
    String segment1 = "segment1";
    Map<String, String> idealStateInstanceStateMap = new TreeMap<>();
    idealStateInstanceStateMap.put(instance, CONSUMING);
    idealStateInstanceStateMap.put(errorInstance, ONLINE);
    idealStateSegmentAssignment.put(segment0, idealStateInstanceStateMap);
    idealStateSegmentAssignment.put(segment1, idealStateInstanceStateMap);
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();
    externalViewInstanceStateMap0.put(instance, CONSUMING);
    externalViewInstanceStateMap0.put(errorInstance, ERROR);
    Map<String, String> externalViewInstanceStateMap1 = new TreeMap<>();
    externalViewInstanceStateMap1.put(instance, CONSUMING);
    externalViewInstanceStateMap1.put(errorInstance, ERROR);
    externalViewSegmentAssignment.put(segment0, externalViewInstanceStateMap0);
    externalViewSegmentAssignment.put(segment1, externalViewInstanceStateMap1);
    onlineSegments.add(segment0);
    onlineSegments.add(segment1);
    List<String> segments = Arrays.asList(segment0, segment1);

    // Initialize with no enabled instance, both segments should be unavailable
    // {
    //   segment0: {
    //     (disabled) instance: CONSUMING,
    //     (disabled) errorInstance: ERROR
    //   },
    //   segment1: {
    //     (disabled) instance: CONSUMING,
    //     (disabled) errorInstance: ERROR
    //   }
    // }
    balancedInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    strictReplicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(null);
    InstanceSelector.SelectionResult selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
    assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
    assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
    assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
    assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

    // Iterate 5 times
    for (int i = 0; i < 5; i++) {

      // Enable the ERROR instance, both segments should be unavailable
      // {
      //   segment0: {
      //     (disabled) instance: CONSUMING,
      //     (enabled)  errorInstance: ERROR
      //   },
      //   segment1: {
      //     (disabled) instance: CONSUMING,
      //     (enabled)  errorInstance: ERROR
      //   }
      // }
      enabledInstances.add(errorInstance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(errorInstance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(errorInstance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

      // Enable the CONSUMING instance, both segments should be available
      // {
      //   segment0: {
      //     (enabled)  instance: CONSUMING,
      //     (enabled)  errorInstance: ERROR
      //   },
      //   segment1: {
      //     (enabled)  instance: CONSUMING,
      //     (enabled)  errorInstance: ERROR
      //   }
      // }
      enabledInstances.add(instance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Change ideal state for the CONSUMING instance to ONLINE, both segments should be available
      idealStateInstanceStateMap.put(instance, ONLINE);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Change external view for the CONSUMING instance to ONLINE, both segments should be available
      // {
      //   segment0: {
      //     (enabled)  instance: ONLINE,
      //     (enabled)  errorInstance: ERROR
      //   },
      //   segment1: {
      //     (enabled)  instance: ONLINE,
      //     (enabled)  errorInstance: ERROR
      //   }
      // }
      externalViewInstanceStateMap0.put(instance, ONLINE);
      externalViewInstanceStateMap1.put(instance, ONLINE);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Remove ONLINE instance from the ideal state, both segments should be unavailable
      idealStateInstanceStateMap.remove(instance);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

      // Add back the ONLINE instance to the ideal state, switch the instance state for segment1, both segments should
      // be available for BalancedInstanceSelector, but unavailable for StrictReplicaGroupInstanceSelector
      // {
      //   segment0: {
      //     (enabled)  instance: ONLINE,
      //     (enabled)  errorInstance: ERROR
      //   },
      //   segment1: {
      //     (enabled)  instance: ERROR,
      //     (enabled)  errorInstance: ONLINE
      //   }
      // }
      idealStateInstanceStateMap.put(instance, ONLINE);
      externalViewInstanceStateMap1.put(instance, ERROR);
      externalViewInstanceStateMap1.put(errorInstance, ONLINE);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

      // Switch back the instance state for segment1 and change the ONLINE instance to OFFLINE, both segment to instance
      // map and unavailable segments should be empty
      // {
      //   segment0: {
      //     (enabled)  instance: OFFLINE,
      //     (enabled)  errorInstance: ERROR
      //   },
      //   segment1: {
      //     (enabled)  instance: OFFLINE,
      //     (enabled)  errorInstance: ERROR
      //   }
      // }
      externalViewInstanceStateMap0.put(instance, OFFLINE);
      externalViewInstanceStateMap1.put(instance, OFFLINE);
      externalViewInstanceStateMap1.put(errorInstance, ERROR);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Disable the OFFLINE instance, both segments should be unavailable
      // {
      //   segment0: {
      //     (disabled) instance: OFFLINE,
      //     (enabled)  errorInstance: ERROR
      //   },
      //   segment1: {
      //     (disabled) instance: OFFLINE,
      //     (enabled)  errorInstance: ERROR
      //   }
      // }
      enabledInstances.remove(instance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(instance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

      // Change the ERROR instance of segment0 to ONLINE, segment0 should be available
      // (Note that for StrictReplicaGroupInstanceSelector, segment1 does not have any ONLINE/CONSUMING instance, so it
      // won't mark instance down for segment0)
      // {
      //   segment0: {
      //     (disabled) instance: OFFLINE,
      //     (enabled)  errorInstance: ONLINE
      //   },
      //   segment1: {
      //     (disabled) instance: OFFLINE,
      //     (enabled)  errorInstance: ERROR
      //   }
      // }
      externalViewInstanceStateMap0.put(errorInstance, ONLINE);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 1);
      assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 1);
      assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment1));

      // Disable the ONLINE instance, both segments should be unavailable
      // {
      //   segment0: {
      //     (disabled) instance: OFFLINE,
      //     (disabled) errorInstance: ONLINE
      //   },
      //   segment1: {
      //     (disabled) instance: OFFLINE,
      //     (disabled) errorInstance: ERROR
      //   }
      // }
      enabledInstances.remove(errorInstance);
      balancedInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(errorInstance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, Collections.singletonList(errorInstance));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

      // Change back to initial state, both segments should be unavailable
      // {
      //   segment0: {
      //     (disabled) instance: CONSUMING,
      //     (enabled)  errorInstance: ERROR
      //   },
      //   segment1: {
      //     (disabled) instance: CONSUMING,
      //     (enabled)  errorInstance: ERROR
      //   }
      // }
      idealStateInstanceStateMap.put(instance, CONSUMING);
      externalViewInstanceStateMap0.put(instance, CONSUMING);
      externalViewInstanceStateMap0.put(errorInstance, ERROR);
      externalViewInstanceStateMap1.put(instance, CONSUMING);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
    }
  }

  private static List<String> getSegments() {
    return SEGMENTS;
  }
}
