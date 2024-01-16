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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mercateo.test.clock.TestClock;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.broker.routing.instanceselector.InstanceSelectorUtils.NEW_SEGMENT_EXPIRATION_MILLIS;
import static org.apache.pinot.spi.config.table.RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.config.table.RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


@SuppressWarnings("unchecked")
public class InstanceSelectorTest {
  private AutoCloseable _mocks;

  @Mock
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Mock
  private BrokerMetrics _brokerMetrics;

  @Mock
  private BrokerRequest _brokerRequest;

  @Mock
  private PinotQuery _pinotQuery;

  @Mock
  private TableConfig _tableConfig;

  private TestClock _mutableClock;

  private static final String TABLE_NAME = "testTable_OFFLINE";

  private static final String BALANCED_INSTANCE_SELECTOR = "balanced";

  private static List<String> getSegments() {
    return SEGMENTS;
  }

  private final static List<String> SEGMENTS =
      Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4", "segment5", "segment6", "segment7",
          "segment8", "segment9", "segment10", "segment11");

  private void createSegments(List<Pair<String, Long>> segmentCreationTimeMsPairs) {
    List<String> segmentZKMetadataPaths = new ArrayList<>();
    List<ZNRecord> zkRecords = new ArrayList<>();
    for (Pair<String, Long> segmentCreationTimeMsPair : segmentCreationTimeMsPairs) {
      String segment = segmentCreationTimeMsPair.getLeft();
      long creationTimeMs = segmentCreationTimeMsPair.getRight();
      SegmentZKMetadata offlineSegmentZKMetadata0 = new SegmentZKMetadata(segment);
      offlineSegmentZKMetadata0.setCreationTime(creationTimeMs);
      offlineSegmentZKMetadata0.setTimeUnit(TimeUnit.MILLISECONDS);
      ZNRecord record = offlineSegmentZKMetadata0.toZNRecord();
      segmentZKMetadataPaths.add(ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE_NAME, segment));
      zkRecords.add(record);
    }
    when(_propertyStore.get(eq(segmentZKMetadataPaths), any(), anyInt(), anyBoolean())).thenReturn(zkRecords);
  }

  private IdealState createIdealState(Map<String, List<Pair<String, String>>> segmentState) {
    IdealState idealState = new IdealState(TABLE_NAME);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    for (Map.Entry<String, List<Pair<String, String>>> entry : segmentState.entrySet()) {
      Map<String, String> externalViewInstanceStateMap = new TreeMap<>();
      for (Pair<String, String> instanceState : entry.getValue()) {
        externalViewInstanceStateMap.put(instanceState.getLeft(), instanceState.getRight());
      }
      idealStateSegmentAssignment.put(entry.getKey(), externalViewInstanceStateMap);
    }
    return idealState;
  }

  private ExternalView createExternalView(Map<String, List<Pair<String, String>>> segmentState) {
    ExternalView externalView = new ExternalView(TABLE_NAME);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    for (Map.Entry<String, List<Pair<String, String>>> entry : segmentState.entrySet()) {
      Map<String, String> externalViewInstanceStateMap = new TreeMap<>();
      for (Pair<String, String> instanceState : entry.getValue()) {
        externalViewInstanceStateMap.put(instanceState.getLeft(), instanceState.getRight());
      }
      externalViewSegmentAssignment.put(entry.getKey(), externalViewInstanceStateMap);
    }
    return externalView;
  }

  private static boolean isReplicaGroupType(String selectorType) {
    return selectorType.equals(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) || selectorType.equals(
        STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
  }

  private InstanceSelector createTestInstanceSelector(String selectorType) {
    RoutingConfig config = new RoutingConfig(null, null, selectorType);
    when(_tableConfig.getRoutingConfig()).thenReturn(config);
    return InstanceSelectorFactory.getInstanceSelector(_tableConfig, _propertyStore, _brokerMetrics, null,
        _mutableClock);
  }

  @DataProvider(name = "selectorType")
  public Object[] getSelectorType() {
    return new Object[]{
        REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
        BALANCED_INSTANCE_SELECTOR
    };
  }

  @BeforeMethod
  public void setUp() {
    _mutableClock = TestClock.fixed(Instant.now(), ZoneId.systemDefault());
    _mocks = MockitoAnnotations.openMocks(this);
    when(_brokerRequest.getPinotQuery()).thenReturn(_pinotQuery);
    when(_pinotQuery.getQueryOptions()).thenReturn(null);
    when(_tableConfig.getTableName()).thenReturn(TABLE_NAME);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    clearInvocations(_tableConfig);
    _mocks.close();
  }

  @Test
  public void testInstanceSelectorFactory() {
    TableConfig tableConfig = mock(TableConfig.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);

    // Routing config is missing
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore,
        brokerMetrics) instanceof BalancedInstanceSelector);

    // Instance selector type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore,
        brokerMetrics) instanceof BalancedInstanceSelector);

    // Replica-group instance selector should be returned
    when(routingConfig.getInstanceSelectorType()).thenReturn(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore,
        brokerMetrics) instanceof ReplicaGroupInstanceSelector);

    // Strict replica-group instance selector should be returned
    when(routingConfig.getInstanceSelectorType()).thenReturn(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore,
        brokerMetrics) instanceof StrictReplicaGroupInstanceSelector);

    // Should be backward-compatible with legacy config
    when(routingConfig.getInstanceSelectorType()).thenReturn(null);
    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        InstanceSelectorFactory.LEGACY_REPLICA_GROUP_OFFLINE_ROUTING);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore,
        brokerMetrics) instanceof ReplicaGroupInstanceSelector);
    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        InstanceSelectorFactory.LEGACY_REPLICA_GROUP_REALTIME_ROUTING);
    assertTrue(InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore,
        brokerMetrics) instanceof ReplicaGroupInstanceSelector);
  }

  @Test
  public void testInstanceSelector() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BalancedInstanceSelector balancedInstanceSelector =
        new BalancedInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());
    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());
    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());

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

    int requestId = 0;

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
    Map<String, String> queryOptions = new HashMap<>();
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);
    Map<String, String> expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance0);
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    InstanceSelector.SelectionResult selectionResult =
        balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance2);
    expectedBalancedInstanceSelectorResult.put(segment1, instance0);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance2);
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment0, instance2);
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment0, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Process the changes
    // segment4 is a newly added segment with 3 online instances in idealStage and 2 online instances in externalView.
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    expectedBalancedInstanceSelectorResult.put(segment4, instance2);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    expectedBalancedInstanceSelectorResult.put(segment4, instance2);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance0);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    expectedBalancedInstanceSelectorResult.put(segment4, instance2);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance0);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance0);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    expectedBalancedInstanceSelectorResult.put(segment4, instance0);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
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
    //   instance0 is excluded from serving segment1/4 because instance0 has error for serving segment1
    //   StrictReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> instance2
    //   StrictReplicaGroupInstanceSelector(useCompleteReplicaGroup=false) picks instance0, and reports segment1 as
    //   unavailable on it.
    //     segment1 -> null
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> instance0
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    expectedBalancedInstanceSelectorResult.put(segment4, instance2);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance0);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    Map<String, String> expectedStrictReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedStrictReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    queryOptions.put("useCompleteReplicaGroup", "false");
    expectedStrictReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment4, instance0);
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    queryOptions.remove("useCompleteReplicaGroup");
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedStrictReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), Collections.singleton(segment1));

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
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance1);
    expectedBalancedInstanceSelectorResult.put(segment3, instance3);
    expectedBalancedInstanceSelectorResult.put(segment4, instance0);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance3);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance2);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // segment1 has run into ERROR state on instance0,
    // now make segment4 into ERROR state on instance2.
    Map<String, String> externalViewInstanceStateMap3 = new TreeMap<>();
    externalViewInstanceStateMap3.put(instance0, ONLINE);
    externalViewInstanceStateMap3.put(instance2, ERROR);
    externalViewInstanceStateMap3.put(errorInstance0, ERROR);
    externalViewSegmentAssignment.put(segment4, externalViewInstanceStateMap3);
    balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    replicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // For the 13th request:
    //   BalancedInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance3
    //     segment3 -> instance1
    //     segment4 -> instance0
    //   ReplicaGroupInstanceSelector:
    //     segment1 -> instance2
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> instance0
    //   instance0 is excluded from serving segment1/4 because of error segment1
    //   instance2 is excluded from serving segment1/4 because of error segment4
    //   and this makes no replica group fully available for segment 1/4
    //   StrictReplicaGroupInstanceSelector:
    //     segment1 -> null
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> null
    //   StrictReplicaGroupInstanceSelector(useCompleteReplicaGroup=false) picks instance0, and reports segment1 as
    //   unavailable on it.
    //     segment1 -> null
    //     segment2 -> instance1
    //     segment3 -> instance1
    //     segment4 -> instance0
    requestId++;
    expectedBalancedInstanceSelectorResult = new HashMap<>();
    expectedBalancedInstanceSelectorResult.put(segment1, instance2);
    expectedBalancedInstanceSelectorResult.put(segment2, instance3);
    expectedBalancedInstanceSelectorResult.put(segment3, instance1);
    expectedBalancedInstanceSelectorResult.put(segment4, instance0);
    selectionResult = balancedInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedReplicaGroupInstanceSelectorResult.put(segment1, instance2);
    expectedReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedReplicaGroupInstanceSelectorResult.put(segment4, instance0);
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    expectedStrictReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedStrictReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableSet.of(segment1, segment4));
    queryOptions.put("useCompleteReplicaGroup", "false");
    expectedStrictReplicaGroupInstanceSelectorResult = new HashMap<>();
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment2, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment3, instance1);
    expectedStrictReplicaGroupInstanceSelectorResult.put(segment4, instance0);
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, requestId);
    queryOptions.remove("useCompleteReplicaGroup");
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedStrictReplicaGroupInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), Collections.singleton(segment1));
  }

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsToQuery() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    // numReplicas = 3, fanning the query to 2 replica groups
    queryOptions.put("numReplicaGroupsToQuery", "2");
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());

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
    for (String segment : segments) {
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
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsToQueryGreaterThanReplicas() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put("numReplicaGroupsToQuery", "4");

    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());

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
    for (String segment : segments) {
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
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test
  public void testReplicaGroupInstanceSelectorNumReplicaGroupsNotSet() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();

    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector =
        new ReplicaGroupInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());

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
    for (String segment : segments) {
      idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
      externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      onlineSegments.add(segment);
    }

    replicaGroupInstanceSelector.init(enabledInstances, idealState, externalView, onlineSegments);
    // since numReplicaGroupsToQuery is not set, first query should go to first replica group,
    // 2nd query should go to next replica group

    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    for (String segment : segments) {
      expectedReplicaGroupInstanceSelectorResult.put(segment, instance0);
    }
    InstanceSelector.SelectionResult selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);

    for (String segment : segments) {
      expectedReplicaGroupInstanceSelectorResult.put(segment, instance1);
    }
    selectionResult = replicaGroupInstanceSelector.select(brokerRequest, segments, 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);
  }

  @Test
  public void testMultiStageStrictReplicaGroupSelector() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    // Create instance-partitions with two replica-groups and 1 partition. Each replica-group has 2 instances.
    List<String> replicaGroup0 = ImmutableList.of("instance-0", "instance-1");
    List<String> replicaGroup1 = ImmutableList.of("instance-2", "instance-3");
    Map<String, List<String>> partitionToInstances = ImmutableMap.of("0_0", replicaGroup0, "0_1", replicaGroup1);
    InstancePartitions instancePartitions = new InstancePartitions(offlineTableName);
    instancePartitions.setInstances(0, 0, partitionToInstances.get("0_0"));
    instancePartitions.setInstances(0, 1, partitionToInstances.get("0_1"));
    BrokerRequest brokerRequest = mock(BrokerRequest.class);
    PinotQuery pinotQuery = mock(PinotQuery.class);
    Map<String, String> queryOptions = new HashMap<>();
    when(brokerRequest.getPinotQuery()).thenReturn(pinotQuery);
    when(pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    MultiStageReplicaGroupSelector multiStageSelector =
        new MultiStageReplicaGroupSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());
    multiStageSelector = spy(multiStageSelector);
    doReturn(instancePartitions).when(multiStageSelector).getInstancePartitions();

    List<String> enabledInstances = new ArrayList<>();
    IdealState idealState = new IdealState(offlineTableName);
    Map<String, Map<String, String>> idealStateSegmentAssignment = idealState.getRecord().getMapFields();
    ExternalView externalView = new ExternalView(offlineTableName);
    Map<String, Map<String, String>> externalViewSegmentAssignment = externalView.getRecord().getMapFields();
    Set<String> onlineSegments = new HashSet<>();

    // Mark all instances as enabled
    for (int i = 0; i < 4; i++) {
      enabledInstances.add(String.format("instance-%d", i));
    }

    List<String> segments = getSegments();

    // Create two idealState and externalView maps. One is used for segments with replica-group=0 and the other for rg=1
    Map<String, String> idealStateInstanceStateMap0 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap0 = new TreeMap<>();
    Map<String, String> idealStateInstanceStateMap1 = new TreeMap<>();
    Map<String, String> externalViewInstanceStateMap1 = new TreeMap<>();

    // instance-0 and instance-2 mirror each other in the two replica-groups. Same for instance-1 and instance-3.
    for (int i = 0; i < 4; i++) {
      String instance = enabledInstances.get(i);
      if (i % 2 == 0) {
        idealStateInstanceStateMap0.put(instance, ONLINE);
        externalViewInstanceStateMap0.put(instance, ONLINE);
      } else {
        idealStateInstanceStateMap1.put(instance, ONLINE);
        externalViewInstanceStateMap1.put(instance, ONLINE);
      }
    }

    // Even numbered segments get assigned to [instance-0, instance-2], and odd numbered segments get assigned to
    // [instance-1,instance-3].
    for (int segmentNum = 0; segmentNum < segments.size(); segmentNum++) {
      String segment = segments.get(segmentNum);
      if (segmentNum % 2 == 0) {
        idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap0);
        externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap0);
      } else {
        idealStateSegmentAssignment.put(segment, idealStateInstanceStateMap1);
        externalViewSegmentAssignment.put(segment, externalViewInstanceStateMap1);
      }
      onlineSegments.add(segment);
    }

    multiStageSelector.init(new HashSet<>(enabledInstances), idealState, externalView, onlineSegments);

    // Using requestId=0 should select replica-group 0. Even segments get assigned to instance-0 and odd segments get
    // assigned to instance-1.
    Map<String, String> expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    for (int segmentNum = 0; segmentNum < segments.size(); segmentNum++) {
      expectedReplicaGroupInstanceSelectorResult.put(segments.get(segmentNum), replicaGroup0.get(segmentNum % 2));
    }
    InstanceSelector.SelectionResult selectionResult = multiStageSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);

    // Using same requestId again should return the same selection
    selectionResult = multiStageSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);

    // Using requestId=1 should select replica-group 1
    expectedReplicaGroupInstanceSelectorResult = new HashMap<>();
    for (int segmentNum = 0; segmentNum < segments.size(); segmentNum++) {
      expectedReplicaGroupInstanceSelectorResult.put(segments.get(segmentNum), replicaGroup1.get(segmentNum % 2));
    }
    selectionResult = multiStageSelector.select(brokerRequest, segments, 1);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);

    // If instance-0 is down, replica-group 1 should be picked even with requestId=0
    enabledInstances.remove("instance-0");
    multiStageSelector.init(new HashSet<>(enabledInstances), idealState, externalView, onlineSegments);
    selectionResult = multiStageSelector.select(brokerRequest, segments, 0);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedReplicaGroupInstanceSelectorResult);

    // If instance-2 also goes down, no replica-group is eligible
    enabledInstances.remove("instance-2");
    multiStageSelector.init(new HashSet<>(enabledInstances), idealState, externalView, onlineSegments);
    try {
      multiStageSelector.select(brokerRequest, segments, 0);
      fail("Method call above should have failed");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testUnavailableSegments() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BalancedInstanceSelector balancedInstanceSelector =
        new BalancedInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());
    // ReplicaGroupInstanceSelector has the same behavior as BalancedInstanceSelector for the unavailable segments
    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector =
        new StrictReplicaGroupInstanceSelector(offlineTableName, propertyStore, brokerMetrics, null, Clock.systemUTC());

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
    InstanceSelector.SelectionResult selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
    assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
    assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
    selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Change ideal state for the CONSUMING instance to ONLINE, both segments should be available
      idealStateInstanceStateMap.put(instance, ONLINE);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());

      // Remove ONLINE instance from the ideal state, both segments should be unavailable
      idealStateInstanceStateMap.remove(instance);
      balancedInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      strictReplicaGroupInstanceSelector.onAssignmentChange(idealState, externalView, onlineSegments);
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 2);
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      // segment1 and segment0 are considered old therefore we should report it as unavailable.
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

      // Change the ERROR instance of segment0 to ONLINE, segment0 should be available
      // (Note that for StrictReplicaGroupInstanceSelector, segment1 but it is old so it will mark instance down for
      // segment0)
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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertEquals(selectionResult.getSegmentToInstanceMap().size(), 1);
      assertEquals(selectionResult.getUnavailableSegments(), Collections.singletonList(segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));

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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
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
      selectionResult = balancedInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
      selectionResult = strictReplicaGroupInstanceSelector.select(brokerRequest, segments, 0);
      assertTrue(selectionResult.getSegmentToInstanceMap().isEmpty());
      assertEquals(selectionResult.getUnavailableSegments(), Arrays.asList(segment0, segment1));
    }
  }

  @Test(dataProvider = "selectorType")
  public void testNewSegmentFromZKMetadataSelection(String selectorType) {
    String oldSeg = "segment0";
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance1, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);
    InstanceSelector selector = createTestInstanceSelector(selectorType);

    selector.init(enabledInstances, idealState, externalView, onlineSegments);

    {
      int requestId = 0;
      // First selection, we select instance0 for oldSeg and instance1 for newSeg in balance selector
      // For replica group, we select instance0 for oldSeg and newSeg. Because newSeg is not online in instance0, so
      // we exclude it from selection result.
      InstanceSelector.SelectionResult selectionResult =
          selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
      if (isReplicaGroupType(selectorType)) {
        assertEquals(selectionResult.getSegmentToInstanceMap(), ImmutableMap.of(oldSeg, instance0));
        assertEquals(selectionResult.getOptionalSegmentToInstanceMap(), ImmutableMap.of(newSeg, instance0));
      } else {
        assertEquals(selectionResult.getSegmentToInstanceMap(), ImmutableMap.of(oldSeg, instance0, newSeg, instance1));
        assertTrue(selectionResult.getOptionalSegmentToInstanceMap().isEmpty());
      }
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    }
    {
      int requestId = 1;
      // Second selection, we select instance1 for oldSeg and instance0 for newSeg in balance selector
      // Because newSeg is not online in instance0, so we exclude it from selection result.
      // For replica group, we select instance1 for oldSeg and newSeg.
      InstanceSelector.SelectionResult selectionResult =
          selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
      switch (selectorType) {
        case BALANCED_INSTANCE_SELECTOR:
          assertEquals(selectionResult.getSegmentToInstanceMap(), ImmutableMap.of(oldSeg, instance1));
          assertEquals(selectionResult.getOptionalSegmentToInstanceMap(), ImmutableMap.of(newSeg, instance0));
          break;
        case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE: // fall through
        case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
          assertEquals(selectionResult.getSegmentToInstanceMap(),
              ImmutableMap.of(oldSeg, instance1, newSeg, instance1));
          assertTrue(selectionResult.getOptionalSegmentToInstanceMap().isEmpty());
          break;
        default:
          throw new RuntimeException("unsupported selector type:" + selectorType);
      }
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    }
    // Advance the clock to make newSeg to old segment.
    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS + 10));
    // Upon re-initialization, newly old segments can only be served from online instances: instance1
    selector.init(enabledInstances, idealState, externalView, onlineSegments);
    {
      int requestId = 0;
      InstanceSelector.SelectionResult selectionResult =
          selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
      switch (selectorType) {
        case BALANCED_INSTANCE_SELECTOR: // fall through
        case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
          assertEquals(selectionResult.getSegmentToInstanceMap(),
              ImmutableMap.of(oldSeg, instance0, newSeg, instance1));
          break;
        case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
          assertEquals(selectionResult.getSegmentToInstanceMap(),
              ImmutableMap.of(oldSeg, instance1, newSeg, instance1));
          break;
        default:
          throw new RuntimeException("unsupported selector type:" + selectorType);
      }
      assertTrue(selectionResult.getOptionalSegmentToInstanceMap().isEmpty());
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    }
    {
      int requestId = 1;
      Map<String, String> expectedSelectionResult = ImmutableMap.of(oldSeg, instance1, newSeg, instance1);
      InstanceSelector.SelectionResult selectionResult =
          selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
      assertEquals(selectionResult.getSegmentToInstanceMap(), expectedSelectionResult);
      assertTrue(selectionResult.getOptionalSegmentToInstanceMap().isEmpty());
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    }
  }

  // Test that we don't report new segment as unavailable till it gets old.
  @Test(dataProvider = "selectorType")
  public void testNewSegmentFromZKMetadataReportingUnavailable(String selectorType) {
    // Set segment0 as new segment
    String newSeg = "segment0";
    String oldSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100),
            Pair.of(oldSeg, _mutableClock.millis() - NEW_SEGMENT_EXPIRATION_MILLIS - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(newSeg, oldSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for new segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for new segments
    //   [segment0] -> []
    //   [segment1] -> [instance0: online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(newSeg, ImmutableList.of(), oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector = createTestInstanceSelector(selectorType);
    selector.init(enabledInstances, idealState, externalView, onlineSegments);
    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedResult = ImmutableMap.of(oldSeg, instance0);
    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertEquals(selectionResult.getOptionalSegmentToInstanceMap(), ImmutableMap.of(newSeg, instance0));
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment and we see newSeg is reported as unavailable segment.
    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS + 10));
    selector.init(enabledInstances, idealState, externalView, onlineSegments);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    if (STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equals(selectorType)) {
      expectedResult = ImmutableMap.of();
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg, oldSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
    }
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getOptionalSegmentToInstanceMap().isEmpty());
  }

  @Test(dataProvider = "selectorType")
  public void testNewSegmentGetsOldWithErrorState(String selectorType) {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of());

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector = createTestInstanceSelector(selectorType);
    selector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedResult = ImmutableMap.of(oldSeg, instance0);

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Report error instance for segment1 since segment1 becomes old and we should report it as unavailable.
    externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ERROR)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    if (selectorType == STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) {
      expectedResult = ImmutableMap.of();
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg, newSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
    }
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);

    // Get segment1 back online in instance1
    externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ERROR), Pair.of(instance1, ONLINE)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    if (selectorType == STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) {
      expectedResult = ImmutableMap.of(oldSeg, instance1, newSeg, instance1);
    } else {
      expectedResult = ImmutableMap.of(oldSeg, instance0, newSeg, instance1);
    }
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  // Test that we mark new segment as old when external view state converges with ideal state.
  @Test(dataProvider = "selectorType")
  public void testNewSegmentGetsOldWithStateConverge(String selectorType) {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of());

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector = createTestInstanceSelector(selectorType);
    selector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = ImmutableMap.of(oldSeg, instance0);

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Segment1 is not old anymore with state converge.
    externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Segment1 becomes unavailable.
    externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of());

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    if (selectorType == STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) {
      expectedBalancedInstanceSelectorResult = ImmutableMap.of();
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg, newSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
    }
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
  }

  @Test(dataProvider = "selectorType")
  public void testNewSegmentsFromIDWithMissingEV(String selectorType) {
    String oldSeg0 = "segment0";
    String oldSeg1 = "segment1";
    Set<String> onlineSegments = ImmutableSet.of(oldSeg0, oldSeg1);

    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);

    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(oldSeg0, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(oldSeg0, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector = createTestInstanceSelector(selectorType);
    selector.init(enabledInstances, idealState, externalView, onlineSegments);

    // Add a new segment to ideal state with missing external view.
    String newSeg = "segment2";
    onlineSegments = ImmutableSet.of(oldSeg0, oldSeg1, newSeg);
    idealSateMap =
        ImmutableMap.of(oldSeg0, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    idealState = createIdealState(idealSateMap);
    externalViewMap =
        ImmutableMap.of(oldSeg0, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    int requestId = 0;
    Map<String, String> expectedResult;
    switch (selectorType) {
      case BALANCED_INSTANCE_SELECTOR:
        expectedResult = ImmutableMap.of(oldSeg0, instance0, oldSeg1, instance1);
        break;
      case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE: // fall through
      case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
        expectedResult = ImmutableMap.of(oldSeg0, instance0, oldSeg1, instance0);
        break;
      default:
        throw new RuntimeException("unsupported type:" + selectorType);
    }

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment.
    // On state update, all segments become unavailable.
    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS + 10));
    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    if (selectorType == STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) {
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg0, oldSeg1, newSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(newSeg));
    }
  }

  // Test that on instance change, we exclude not enabled instance from serving for new segments.
  @Test(dataProvider = "selectorType")
  public void testExcludeNotEnabledInstanceForNewSegment(String selectorType) {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector = createTestInstanceSelector(selectorType);
    selector.init(enabledInstances, idealState, externalView, onlineSegments);

    // First selection, we select instance1 for newSeg.
    int requestId = 0;
    Map<String, String> expectedResult;
    switch (selectorType) {
      case BALANCED_INSTANCE_SELECTOR:
        expectedResult = ImmutableMap.of(oldSeg, instance0);
        break;
      case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
      case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
        expectedResult = ImmutableMap.of(oldSeg, instance0, newSeg, instance0);
        break;
      default:
        throw new RuntimeException("Unsupported type:" + selectorType);
    }

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Remove instance0 from enabledInstances.
    enabledInstances = ImmutableSet.of(instance1);
    List<String> changeInstance = ImmutableList.of(instance0);
    selector.onInstancesChange(enabledInstances, changeInstance);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    // We don't include instance0 in selection anymore.
    expectedResult = ImmutableMap.of(oldSeg, instance1);

    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test(dataProvider = "selectorType")
  public void testExcludeInstanceNotInIdealState(String selectorType) {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        ImmutableList.of(Pair.of(oldSeg, _mutableClock.millis() - NEW_SEGMENT_EXPIRATION_MILLIS - 100),
            Pair.of(newSeg, _mutableClock.millis() - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(oldSeg, newSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1, instance2);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance2: online]
    //   [segment1] -> [instance2: online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(oldSeg, ImmutableList.of(Pair.of(instance2, ONLINE)), newSeg,
            ImmutableList.of(Pair.of(instance2, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector = createTestInstanceSelector(selectorType);
    selector.init(enabledInstances, idealState, externalView, onlineSegments);

    // No selection because the external view is not in ideal state.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = ImmutableMap.of();
    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), ImmutableList.of(oldSeg));
  }

  @Test(dataProvider = "selectorType")
  public void testExcludeIdealStateOffline(String selectorType) {
    // Set segment0 as old segment
    String newSeg = "segment0";
    String oldSeg = "segment1";

    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        ImmutableList.of(Pair.of(newSeg, _mutableClock.millis() - 100),
            Pair.of(oldSeg, _mutableClock.millis() - NEW_SEGMENT_EXPIRATION_MILLIS - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(newSeg, oldSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:offline, instance1:online]
    //   [segment1] -> [instance0:offline, instance1:online]
    Map<String, List<Pair<String, String>>> idealSateMap =
        ImmutableMap.of(newSeg, ImmutableList.of(Pair.of(instance0, OFFLINE), Pair.of(instance1, ONLINE)), oldSeg,
            ImmutableList.of(Pair.of(instance0, OFFLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        ImmutableMap.of(newSeg, ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg,
            ImmutableList.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector = createTestInstanceSelector(selectorType);
    selector.init(enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = ImmutableMap.of(oldSeg, instance1, newSeg, instance1);

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }
}
