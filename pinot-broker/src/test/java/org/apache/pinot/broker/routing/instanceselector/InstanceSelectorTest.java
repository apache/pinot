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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.FailOpenPolicy;
import org.apache.pinot.broker.routing.adaptiveserverselector.HybridSelector;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TestClock;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.config.table.RoutingConfig.REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.config.table.RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.ORDERED_PREFERRED_POOLS;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.USE_FIXED_REPLICA;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


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

  private static final Map<String, ServerInstance> EMPTY_SERVER_MAP = Collections.EMPTY_MAP;

  private static final String TABLE_NAME = "testTable_OFFLINE";

  private static final String BALANCED_INSTANCE_SELECTOR = "balanced";

  private static List<String> getSegments() {
    return SEGMENTS;
  }

  private final static List<String> SEGMENTS =
      Arrays.asList("segment0", "segment1", "segment2", "segment3", "segment4", "segment5", "segment6", "segment7",
          "segment8", "segment9", "segment10", "segment11");
  private static final long NEW_SEGMENT_EXPIRATION_SECONDS = 300;
  private static final long NEW_SEGMENT_EXPIRATION_MILLIS = TimeUnit.SECONDS.toMillis(NEW_SEGMENT_EXPIRATION_SECONDS);
  private final static InstanceSelectorConfig INSTANCE_SELECTOR_CONFIG =
      new InstanceSelectorConfig(false, NEW_SEGMENT_EXPIRATION_SECONDS, false);

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

  private InstanceSelector createTestInstanceSelector(String selectorType, Set<String> enabledInstances,
      IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
    RoutingConfig config = new RoutingConfig(null, null, selectorType, false);
    when(_tableConfig.getRoutingConfig()).thenReturn(config);
    return InstanceSelectorFactory.getInstanceSelector(_tableConfig, _propertyStore, _brokerMetrics, null,
        _mutableClock, new PinotConfiguration(), enabledInstances, EMPTY_SERVER_MAP, idealState, externalView,
        onlineSegments);
  }

  private InstanceSelector createTestInstanceSelector(String selectorType, Set<String> enabledInstances,
      Map<String, ServerInstance> enabledServerMap, IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, FailOpenPolicy failOpenPolicy, FailureDetector failureDetector) {
    RoutingConfig config = new RoutingConfig(null, null, selectorType, false);
    when(_tableConfig.getRoutingConfig()).thenReturn(config);
    return InstanceSelectorFactory.getInstanceSelector(_tableConfig, _propertyStore, _brokerMetrics, null,
        _mutableClock, new PinotConfiguration(), enabledInstances, enabledServerMap, idealState, externalView,
        onlineSegments, failOpenPolicy, failureDetector);
  }

  @DataProvider(name = "selectorType")
  public Object[] getSelectorType() {
    return new Object[]{
        REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, BALANCED_INSTANCE_SELECTOR
    };
  }

  @BeforeMethod
  public void setUp() {
    _mutableClock = new TestClock(Instant.now(), ZoneId.systemDefault());
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
    when(tableConfig.getTableName()).thenReturn("testTable_OFFLINE");

    // Routing config is missing
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore, brokerMetrics, new PinotConfiguration(),
            Set.of(), Map.of(), new IdealState("testTable_OFFLINE"), new ExternalView("testTable_OFFLINE"),
            Set.of()) instanceof BalancedInstanceSelector);

    // Instance selector type is not configured
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore, brokerMetrics, new PinotConfiguration(),
            Set.of(), Map.of(), new IdealState("testTable_OFFLINE"), new ExternalView("testTable_OFFLINE"),
            Set.of()) instanceof BalancedInstanceSelector);

    // Replica-group instance selector should be returned
    when(routingConfig.getInstanceSelectorType()).thenReturn(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore, brokerMetrics, new PinotConfiguration(),
            Set.of(), Map.of(), new IdealState("testTable_OFFLINE"), new ExternalView("testTable_OFFLINE"),
            Set.of()) instanceof ReplicaGroupInstanceSelector);

    // Strict replica-group instance selector should be returned
    when(routingConfig.getInstanceSelectorType()).thenReturn(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore, brokerMetrics, new PinotConfiguration(),
            Set.of(), Map.of(), new IdealState("testTable_OFFLINE"), new ExternalView("testTable_OFFLINE"),
            Set.of()) instanceof StrictReplicaGroupInstanceSelector);

    // Should be backward-compatible with legacy config
    when(routingConfig.getInstanceSelectorType()).thenReturn(null);
    when(tableConfig.getTableType()).thenReturn(TableType.OFFLINE);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        InstanceSelectorFactory.LEGACY_REPLICA_GROUP_OFFLINE_ROUTING);
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore, brokerMetrics, new PinotConfiguration(),
            Set.of(), Map.of(), new IdealState("testTable_OFFLINE"), new ExternalView("testTable_OFFLINE"),
            Set.of()) instanceof ReplicaGroupInstanceSelector);
    when(tableConfig.getTableType()).thenReturn(TableType.REALTIME);
    when(routingConfig.getRoutingTableBuilderName()).thenReturn(
        InstanceSelectorFactory.LEGACY_REPLICA_GROUP_REALTIME_ROUTING);
    assertTrue(
        InstanceSelectorFactory.getInstanceSelector(tableConfig, propertyStore, brokerMetrics, new PinotConfiguration(),
            Set.of(), Map.of(), new IdealState("testTable_OFFLINE"), new ExternalView("testTable_OFFLINE"),
            Set.of()) instanceof ReplicaGroupInstanceSelector);
  }

  @Test
  public void testFactoryDisablesAdaptiveRoutingForLegacyUpsertReplicaGroupSelector() {
    TableConfig tableConfig = mock(TableConfig.class);
    RoutingConfig routingConfig = mock(RoutingConfig.class);
    HybridSelector hybridSelector = mock(HybridSelector.class);
    when(tableConfig.getTableName()).thenReturn("legacyUpsert_REALTIME");
    when(tableConfig.getRoutingConfig()).thenReturn(routingConfig);
    when(tableConfig.isUpsertEnabled()).thenReturn(true);
    when(routingConfig.getInstanceSelectorType()).thenReturn(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE);

    ReplicaGroupInstanceSelector instanceSelector =
        (ReplicaGroupInstanceSelector) InstanceSelectorFactory.getInstanceSelector(tableConfig, _propertyStore,
            _brokerMetrics, hybridSelector, new PinotConfiguration(), Set.of(), Map.of(),
            new IdealState("legacyUpsert_REALTIME"), new ExternalView("legacyUpsert_REALTIME"), Set.of());

    assertNull(instanceSelector._adaptiveServerSelector);
    assertNull(instanceSelector._priorityPoolInstanceSelector);
  }

  @Test
  public void testInstanceSelector() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BalancedInstanceSelector balancedInstanceSelector = new BalancedInstanceSelector();
    ReplicaGroupInstanceSelector replicaGroupInstanceSelector = new ReplicaGroupInstanceSelector();
    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector = new StrictReplicaGroupInstanceSelector();

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

    balancedInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);
    replicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);
    strictReplicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);

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
    when(pinotQuery.getQueryOptions()).thenReturn(null);
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
    balancedInstanceSelector.onInstancesChange(enabledInstances, List.of(instance0));
    replicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(instance0));
    strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(instance0));

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
    balancedInstanceSelector.onInstancesChange(enabledInstances, List.of(instance0));
    replicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(instance0));
    strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(instance0));

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
    //   instance0 is excluded from serving segment4 because instance0 has error for serving segment1
    //   StrictReplicaGroupInstanceSelector:
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

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector = new ReplicaGroupInstanceSelector();

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

    replicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);
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

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector = new ReplicaGroupInstanceSelector();

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

    replicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);
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

    ReplicaGroupInstanceSelector replicaGroupInstanceSelector = new ReplicaGroupInstanceSelector();

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

    replicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);
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
  public void testUnavailableSegments() {
    String offlineTableName = "testTable_OFFLINE";
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    BrokerMetrics brokerMetrics = mock(BrokerMetrics.class);
    BalancedInstanceSelector balancedInstanceSelector = new BalancedInstanceSelector();
    // ReplicaGroupInstanceSelector has the same behavior as BalancedInstanceSelector for the unavailable segments
    StrictReplicaGroupInstanceSelector strictReplicaGroupInstanceSelector = new StrictReplicaGroupInstanceSelector();

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
    Map<String, ServerInstance> failOpenEligibleServers =
        Map.of(instance, mock(ServerInstance.class), errorInstance, mock(ServerInstance.class));

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
    balancedInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, failOpenEligibleServers, idealState, externalView,
        onlineSegments);
    strictReplicaGroupInstanceSelector.init(_tableConfig, propertyStore, brokerMetrics, null, Clock.systemUTC(),
        INSTANCE_SELECTOR_CONFIG, enabledInstances, failOpenEligibleServers, idealState, externalView,
        onlineSegments);
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
    verify(brokerMetrics, times(2)).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ELIGIBLE_REQUESTS, 1);
    // Both selectors transition segment0 and segment1 into the unavailable-with-candidates state on init.
    verify(brokerMetrics, times(4))
        .addMeteredTableValue(offlineTableName, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);

    // An unchanged routing refresh should not re-count the transition meter since the segments were already
    // unavailable with candidates.
    balancedInstanceSelector.onInstancesChange(enabledInstances, List.of());
    strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of());
    verify(brokerMetrics, times(4))
        .addMeteredTableValue(offlineTableName, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);

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
      balancedInstanceSelector.onInstancesChange(enabledInstances, List.of(errorInstance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(errorInstance));
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
      balancedInstanceSelector.onInstancesChange(enabledInstances, List.of(instance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(instance));
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
      balancedInstanceSelector.onInstancesChange(enabledInstances, List.of(instance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(instance));
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
      assertEquals(selectionResult.getUnavailableSegments(), List.of(segment1));
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
      balancedInstanceSelector.onInstancesChange(enabledInstances, List.of(errorInstance));
      strictReplicaGroupInstanceSelector.onInstancesChange(enabledInstances, List.of(errorInstance));
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
        List.of(Pair.of(newSeg, _mutableClock.millis() - 100));
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
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));
    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance1, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);
    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);

    {
      int requestId = 0;
      // First selection, we select instance0 for oldSeg and instance1 for newSeg in balance selector
      // For replica group, we select instance0 for oldSeg and newSeg. Because newSeg is not online in instance0, so
      // we exclude it from selection result.
      InstanceSelector.SelectionResult selectionResult =
          selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
      if (isReplicaGroupType(selectorType)) {
        assertEquals(selectionResult.getSegmentToInstanceMap(), Map.of(oldSeg, instance0));
        assertEquals(selectionResult.getOptionalSegmentToInstanceMap(), Map.of(newSeg, instance0));
      } else {
        assertEquals(selectionResult.getSegmentToInstanceMap(), Map.of(oldSeg, instance0, newSeg, instance1));
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
          assertEquals(selectionResult.getSegmentToInstanceMap(), Map.of(oldSeg, instance1));
          assertEquals(selectionResult.getOptionalSegmentToInstanceMap(), Map.of(newSeg, instance0));
          break;
        case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE: // fall through
        case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
          assertEquals(selectionResult.getSegmentToInstanceMap(),
              Map.of(oldSeg, instance1, newSeg, instance1));
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
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, _mutableClock,
        INSTANCE_SELECTOR_CONFIG, enabledInstances, EMPTY_SERVER_MAP, idealState, externalView, onlineSegments);
    {
      int requestId = 0;
      InstanceSelector.SelectionResult selectionResult =
          selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
      switch (selectorType) {
        case BALANCED_INSTANCE_SELECTOR: // fall through
        case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
          assertEquals(selectionResult.getSegmentToInstanceMap(),
              Map.of(oldSeg, instance0, newSeg, instance1));
          break;
        case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
          assertEquals(selectionResult.getSegmentToInstanceMap(),
              Map.of(oldSeg, instance1, newSeg, instance1));
          break;
        default:
          throw new RuntimeException("unsupported selector type:" + selectorType);
      }
      assertTrue(selectionResult.getOptionalSegmentToInstanceMap().isEmpty());
      assertTrue(selectionResult.getUnavailableSegments().isEmpty());
    }
    {
      int requestId = 1;
      Map<String, String> expectedSelectionResult = Map.of(oldSeg, instance1, newSeg, instance1);
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
        List.of(Pair.of(newSeg, _mutableClock.millis() - 100),
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
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for new segments
    //   [segment0] -> []
    //   [segment1] -> [instance0: online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(newSeg, List.of(), oldSeg, List.of(Pair.of(instance0, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);
    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedResult = Map.of(oldSeg, instance0);
    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertEquals(selectionResult.getOptionalSegmentToInstanceMap(), Map.of(newSeg, instance0));
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Advance the clock to make newSeg to old segment and we see newSeg is reported as unavailable segment.
    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS + 10));
    selector = createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    if (STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equals(selectorType)) {
      expectedResult = Map.of();
      assertEquals(selectionResult.getUnavailableSegments(), List.of(newSeg, oldSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), List.of(newSeg));
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
        List.of(Pair.of(newSeg, _mutableClock.millis() - 100));
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
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of());

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedResult = Map.of(oldSeg, instance0);

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Report error instance for segment1 since segment1 becomes old and we should report it as unavailable.
    externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ERROR)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    if (selectorType == STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) {
      expectedResult = Map.of();
      assertEquals(selectionResult.getUnavailableSegments(), List.of(oldSeg, newSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), List.of(newSeg));
    }
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);

    // Get segment1 back online in instance1
    externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ERROR), Pair.of(instance1, ONLINE)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    if (selectorType == STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) {
      expectedResult = Map.of(oldSeg, instance1, newSeg, instance1);
    } else {
      expectedResult = Map.of(oldSeg, instance0, newSeg, instance1);
    }
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenEligibleSegmentsMeter(String selectorType) {
    String segment = "segment0";
    String instance = "instance0";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of()));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(),
        Map.of(instance, mock(ServerInstance.class)), idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);

    // Segment has no candidates at all yet, so it is not a transition into the unavailable-with-candidates state.
    verify(_brokerMetrics, never())
        .addMeteredTableValue(TABLE_NAME, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);

    selector.select(_brokerRequest, List.of(segment), 0);
    selector.select(_brokerRequest, List.of(segment), 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ELIGIBLE_REQUESTS, 1);

    externalView = createExternalView(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Segment transitioned from "no candidates" to "unavailable with a disabled candidate".
    verify(_brokerMetrics)
        .addMeteredTableValue(TABLE_NAME, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);

    selector.select(_brokerRequest, List.of(segment), 2);
    selector.select(_brokerRequest, List.of(segment), 3);
    verify(_brokerMetrics, times(2)).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ELIGIBLE_REQUESTS, 1);

    // An unchanged refresh must not re-count the transition meter since the segment was already unavailable with a
    // candidate.
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    verify(_brokerMetrics, times(1))
        .addMeteredTableValue(TABLE_NAME, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);

    selector.onInstancesChange(Set.of(instance), List.of(instance));

    // Recovery is not a transition into the unhealthy state, so the meter must not increment.
    verify(_brokerMetrics, times(1))
        .addMeteredTableValue(TABLE_NAME, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);

    selector.select(_brokerRequest, List.of(segment), 4);
    verify(_brokerMetrics, times(2)).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ELIGIBLE_REQUESTS, 1);

    // Disable the instance again to re-enter the unavailable-with-candidates state after recovery; the meter must
    // fire again since this is a fresh transition, not a repeat of the earlier one.
    selector.onInstancesChange(Set.of(), List.of(instance));

    verify(_brokerMetrics, times(2))
        .addMeteredTableValue(TABLE_NAME, BrokerMeter.FAIL_OPEN_ELIGIBLE_SEGMENTS, 1);
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenUsesRequestIdDeterministically(String selectorType) {
    String segment = "segment0";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment,
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment,
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(), enabledServerMap, idealState,
        externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult first = selector.select(_brokerRequest, List.of(segment), 0);
    InstanceSelector.SelectionResult repeated = selector.select(_brokerRequest, List.of(segment), 0);
    InstanceSelector.SelectionResult second = selector.select(_brokerRequest, List.of(segment), 1);

    assertEquals(first.getSegmentToInstanceMap(), Map.of(segment, instance0));
    assertEquals(repeated.getSegmentToInstanceMap(), first.getSegmentToInstanceMap());
    assertEquals(second.getSegmentToInstanceMap(), Map.of(segment, instance1));
    assertTrue(first.getUnavailableSegments().isEmpty());
    // All three select() calls above found a fail-open candidate.
    verify(_brokerMetrics, times(3)).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
  }

  // Fixed-replica ("sticky") routing uses a deterministic formula over whatever candidate list it's given, so it
  // works correctly over the fail-open-expanded pool exactly as it does over the normal pool. Fail-open must not
  // disable it: both calls below must resolve to the exact same instance, not alternate via round robin.
  @Test
  public void testReplicaGroupFailOpenPreservesFixedReplicaQueryOption() {
    String segment = "segment0";
    String instance0 = "instance0";
    String instance1 = "instance1";
    List<Pair<String, String>> assignments =
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment, assignments));
    ExternalView externalView = createExternalView(Map.of(segment, assignments));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    InstanceSelector selector = createTestInstanceSelector(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class)), idealState, externalView,
        Set.of(segment), new FailOpenPolicy(false, true), failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_FIXED_REPLICA, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult first = selector.select(_brokerRequest, List.of(segment), 0);
    InstanceSelector.SelectionResult second = selector.select(_brokerRequest, List.of(segment), 1);

    assertEquals(first.getSegmentToInstanceMap(), second.getSegmentToInstanceMap());
  }

  // segment0 is healthy (candidates [instance0, instance1]); segment1 is unavailable and needs fail-open (candidates
  // [instance2, instance3]). Fail-open reselects both segments together over the full per-query segment list, so the
  // shared round-robin counter visits segment0 first (consuming index 0) and segment1 second (consuming index 1) -
  // exactly as the ideal single pass over the full list would have. If fail-open instead restarted its own counter
  // from scratch for just the unavailable subset (the pre-fix behavior), segment1 would land on index 0
  // (instance2) instead of index 1 (instance3).
  @Test
  public void testBalancedFailOpenContinuesRoundRobinSequence() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    String instance3 = "instance3";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    IdealState idealState = createIdealState(Map.of(
        segment0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)),
        segment1, List.of(Pair.of(instance2, ONLINE), Pair.of(instance3, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        segment0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)),
        segment1, List.of(Pair.of(instance2, ONLINE), Pair.of(instance3, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance2, instance3));
    Map<String, ServerInstance> enabledServerMap = Map.of(
        instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class),
        instance2, mock(ServerInstance.class), instance3, mock(ServerInstance.class));
    // Only instance0/instance1 (segment0's instances) are routable; segment1's instances are excluded from normal
    // routing (so segment1 needs fail-open) but remain fail-open eligible via enabledServerMap.
    InstanceSelector selector = createTestInstanceSelector(BALANCED_INSTANCE_SELECTOR, Set.of(instance0, instance1),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true),
        failureDetector);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment0, segment1), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(segment0, instance0, segment1, instance3));
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  // 4-segment query with NUM_REPLICA_GROUPS_TO_QUERY=2; segment2 is unavailable (its instances are fail-open
  // eligible but not routable) and needs fail-open, while segment0/1/3 share a separate, healthy instance pool.
  // The ideal pass walks replicaOffset 0,1,0,1 across the 4 segments (2-and-2 split). Fail-open reselects the full
  // list together, so segment2 lands in its rightful replicaOffset=0 slot (continuing the sequence) rather than
  // restarting its own 0..1 sequence over just the unavailable subset (the pre-fix behavior), which would have
  // broken the requested 2-and-2 split.
  @Test
  public void testReplicaGroupFailOpenContinuesNumReplicaGroupsToQuerySequence() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String segment2 = "segment2";
    String segment3 = "segment3";
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    String instance3 = "instance3";
    List<Pair<String, String>> healthyAssignments =
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    List<Pair<String, String>> failOpenAssignments =
        List.of(Pair.of(instance2, ONLINE), Pair.of(instance3, ONLINE));
    Set<String> onlineSegments = Set.of(segment0, segment1, segment2, segment3);
    IdealState idealState = createIdealState(Map.of(
        segment0, healthyAssignments, segment1, healthyAssignments, segment2, failOpenAssignments, segment3,
        healthyAssignments));
    ExternalView externalView = createExternalView(Map.of(
        segment0, healthyAssignments, segment1, healthyAssignments, segment2, failOpenAssignments, segment3,
        healthyAssignments));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance2, instance3));
    Map<String, ServerInstance> enabledServerMap = Map.of(
        instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class),
        instance2, mock(ServerInstance.class), instance3, mock(ServerInstance.class));
    // Only instance0/instance1 are routable; segment2's instances (instance2/instance3) are excluded from normal
    // routing (so segment2 needs fail-open) but remain fail-open eligible via enabledServerMap.
    InstanceSelector selector = createTestInstanceSelector(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
        Set.of(instance0, instance1), enabledServerMap, idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(NUM_REPLICA_GROUPS_TO_QUERY, "2"));

    InstanceSelector.SelectionResult result =
        selector.select(_brokerRequest, List.of(segment0, segment1, segment2, segment3), 0);

    // replicaOffset sequence across the full list is 0,1,0,1: segment0 (offset0->instance0), segment1
    // (offset1->instance1), segment2 (offset0->instance2, continuing the sequence despite needing fail-open),
    // segment3 (offset1->instance1).
    assertEquals(result.getSegmentToInstanceMap(),
        Map.of(segment0, instance0, segment1, instance1, segment2, instance2, segment3, instance1));
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  // Fail-open recomputes healthy segments too (to preserve round-robin/fan-out sequencing continuity), but pool
  // metrics are only recorded once per query - centrally, from the final mapping that is actually returned to the
  // caller - so this must not double-count the healthy segment's pool.
  @Test
  public void testBalancedFailOpenDoesNotDoubleCountPoolMetrics() {
    String healthySegment = "segment0";
    String failOpenSegment = "segment1";
    String healthyInstance = "instance0";
    String unhealthyInstance = "instance1";
    Set<String> onlineSegments = Set.of(healthySegment, failOpenSegment);
    IdealState idealState = createIdealState(Map.of(healthySegment, List.of(Pair.of(healthyInstance, ONLINE)),
        failOpenSegment, List.of(Pair.of(unhealthyInstance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(healthySegment, List.of(Pair.of(healthyInstance, ONLINE)),
        failOpenSegment, List.of(Pair.of(unhealthyInstance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(unhealthyInstance));
    ServerInstance healthyServer = mock(ServerInstance.class);
    when(healthyServer.getPool()).thenReturn(1);
    ServerInstance unhealthyServer = mock(ServerInstance.class);
    when(unhealthyServer.getPool()).thenReturn(2);
    InstanceSelector selector = createTestInstanceSelector(BALANCED_INSTANCE_SELECTOR, Set.of(healthyInstance),
        Map.of(healthyInstance, healthyServer, unhealthyInstance, unhealthyServer), idealState, externalView,
        onlineSegments, new FailOpenPolicy(false, true), failureDetector);
    clearInvocations(_brokerMetrics);

    selector.select(_brokerRequest, List.of(healthySegment, failOpenSegment), 0);

    verify(_brokerMetrics, times(1)).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(null), "1");
    verify(_brokerMetrics, times(1)).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(null), "2");
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenExcludesAdministrativelyDisabledServer(String selectorType) {
    String segment = "segment0";
    String instance = "instance0";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(),
        Map.of(instance, mock(ServerInstance.class)), idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);

    selector.onInstancesChange(Set.of(), Set.of(), List.of(instance));
    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment), 0);

    assertTrue(result.getSegmentToInstanceMap().isEmpty());
    assertEquals(result.getUnavailableSegments(), List.of(segment));
  }

  @Test
  public void testInstanceChangePreservesLegacyOverrideDispatch() {
    BaseInstanceSelector selector = spy((BaseInstanceSelector) createTestInstanceSelector(BALANCED_INSTANCE_SELECTOR,
        Set.of(), new IdealState(TABLE_NAME), new ExternalView(TABLE_NAME), Set.of()));
    Set<String> routableInstances = Set.of("instance0");
    List<String> changedInstances = List.of("instance0");

    selector.onInstancesChange(routableInstances, routableInstances, changedInstances);

    verify(selector).onInstancesChange(routableInstances, changedInstances);
  }

  @Test
  public void testFailOpenDoesNotSuppressUnexpectedSelectionFailure() {
    String segment = "segment0";
    String instance = "instance0";
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance));
    BaseInstanceSelector selector = spy((BaseInstanceSelector) createTestInstanceSelector(BALANCED_INSTANCE_SELECTOR,
        Set.of(), Map.of(instance, mock(ServerInstance.class)), idealState, externalView, Set.of(segment),
        new FailOpenPolicy(false, true), failureDetector));
    RuntimeException failure = new RuntimeException("Unexpected selector failure");
    doThrow(failure).when(selector).select(anyList(), anyInt(), any(SegmentStates.class), anyMap());

    RuntimeException thrown = Assert.expectThrows(RuntimeException.class,
        () -> selector.select(_brokerRequest, List.of(segment), 0));

    Assert.assertSame(thrown, failure);
  }

  // Scenario: both segments of a single partition share two fail-open-eligible instances. Verifies that fail-open
  // selection moves the whole partition together as requestId changes (segment0 and segment1 always land on the
  // same instance as each other), rather than reselecting each segment independently.
  @Test
  public void testStrictReplicaGroupFailOpenPreservesReplicaGroup() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    List<Pair<String, String>> assignments =
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment0, assignments, segment1, assignments));
    ExternalView externalView = createExternalView(Map.of(segment0, assignments, segment1, assignments));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult first = selector.select(_brokerRequest, List.of(segment0, segment1), 0);
    InstanceSelector.SelectionResult second = selector.select(_brokerRequest, List.of(segment0, segment1), 1);

    assertEquals(first.getSegmentToInstanceMap(), Map.of(segment0, instance0, segment1, instance0));
    assertEquals(second.getSegmentToInstanceMap(), Map.of(segment0, instance1, segment1, instance1));
    assertTrue(first.getUnavailableSegments().isEmpty());
    assertTrue(second.getUnavailableSegments().isEmpty());
  }

  // Scenario: three segments of a single partition share three fail-open-eligible instances. Extends
  // testStrictReplicaGroupFailOpenPreservesReplicaGroup beyond a 2-instance pool (where a same-instance match across
  // segments has a 50% chance of being coincidental) to a 3-instance pool, where every one of the 3 segments landing
  // on the identical instance for every requestId is overwhelmingly unlikely unless the selector is deliberately
  // picking one instance for the whole partition rather than independently "best attempt" redirecting each segment.
  @Test
  public void testStrictReplicaGroupFailOpenNeverSplitsAcrossMultipleCandidates() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String segment2 = "segment2";
    String instance0 = "instance0";
    String instance1 = "instance1";
    String instance2 = "instance2";
    List<String> segments = List.of(segment0, segment1, segment2);
    Set<String> onlineSegments = Set.copyOf(segments);
    List<Pair<String, String>> assignments =
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE), Pair.of(instance2, ONLINE));
    Map<String, List<Pair<String, String>>> assignmentsBySegment =
        Map.of(segment0, assignments, segment1, assignments, segment2, assignments);
    IdealState idealState = createIdealState(assignmentsBySegment);
    ExternalView externalView = createExternalView(assignmentsBySegment);
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1, instance2));
    Map<String, ServerInstance> enabledServerMap = Map.of(instance0, mock(ServerInstance.class), instance1,
        mock(ServerInstance.class), instance2, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    for (int requestId = 0; requestId < 3; requestId++) {
      InstanceSelector.SelectionResult result = selector.select(_brokerRequest, segments, requestId);
      Map<String, String> segmentToInstanceMap = result.getSegmentToInstanceMap();
      String selectedInstance = segmentToInstanceMap.get(segment0);
      assertTrue(Set.of(instance0, instance1, instance2).contains(selectedInstance));
      assertEquals(segmentToInstanceMap.get(segment1), selectedInstance);
      assertEquals(segmentToInstanceMap.get(segment2), selectedInstance);
      assertTrue(result.getUnavailableSegments().isEmpty());
    }
  }

  // Scenario: segment0 is online on both instances, but segment1 (same partition) is online only on instance1.
  // Verifies fail-open finds the single instance common to every segment's candidate set when one exists, and
  // rejects the whole partition (no partial/per-segment selection) when the only fail-open-eligible instance isn't
  // common to all segments.
  @Test
  public void testStrictReplicaGroupFailOpenUsesCommonEligibleReplica() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    List<Pair<String, String>> idealAssignments =
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment0, idealAssignments, segment1, idealAssignments));
    ExternalView externalView = createExternalView(Map.of(segment0, idealAssignments, segment1,
        List.of(Pair.of(instance1, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance1));
    InstanceSelector.SelectionResult selected = selector.select(_brokerRequest, List.of(segment0, segment1), 0);
    assertEquals(selected.getSegmentToInstanceMap(), Map.of(segment0, instance1, segment1, instance1));
    assertTrue(selected.getUnavailableSegments().isEmpty());

    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0));
    InstanceSelector.SelectionResult rejected = selector.select(_brokerRequest, List.of(segment0, segment1), 0);
    assertTrue(rejected.getSegmentToInstanceMap().isEmpty());
    assertEquals(Set.copyOf(rejected.getUnavailableSegments()), onlineSegments);
    // The first select() above found a fail-open candidate (success); the second could not (failure).
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
  }

  // Scenario: a brand-new segment with no external-view entries yet shares a partition (same ideal-state instance
  // set) with an already-healthy sibling segment. Verifies the new segment is pulled into fail-open reselection
  // alongside its healthy sibling and resolves to the exact same instance, so the strict guarantee holds even for
  // segments that haven't finished propagating.
  @Test
  public void testStrictReplicaGroupFailOpenCoversSiblingNewSegment() {
    String oldSegment = "segment0";
    String newSegment = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    List<Pair<String, String>> idealAssignments =
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    createSegments(List.of(Pair.of(newSegment, _mutableClock.millis())));
    Set<String> onlineSegments = Set.of(oldSegment, newSegment);
    IdealState idealState = createIdealState(Map.of(oldSegment, idealAssignments, newSegment, idealAssignments));
    // oldSegment is healthy; newSegment (same partition/instance set) has no online instances yet, as expected for a
    // brand-new segment that hasn't finished propagating.
    ExternalView externalView =
        createExternalView(Map.of(oldSegment, idealAssignments, newSegment, List.of()));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult first = selector.select(_brokerRequest, List.of(oldSegment, newSegment), 0);
    InstanceSelector.SelectionResult second = selector.select(_brokerRequest, List.of(oldSegment, newSegment), 1);

    // The healthy oldSegment and its brand-new partition sibling must resolve to the same fail-open instance;
    // otherwise the strict replica-group guarantee is silently broken for this partition.
    assertEquals(first.getSegmentToInstanceMap(), Map.of(oldSegment, instance0));
    assertEquals(first.getOptionalSegmentToInstanceMap(), Map.of(newSegment, instance0));
    assertEquals(second.getSegmentToInstanceMap(), Map.of(oldSegment, instance1));
    assertEquals(second.getOptionalSegmentToInstanceMap(), Map.of(newSegment, instance1));
    assertTrue(first.getUnavailableSegments().isEmpty());
    assertTrue(second.getUnavailableSegments().isEmpty());
  }

  // Scenario: segment0 is fully unavailable (its only ideal-state instance, instanceA, is unhealthy), forcing fail-open
  // to fire. segment1 also lists unhealthy instanceA, but still has healthy instanceB and instanceC. Verifies that
  // fail-open keeps segment1 on the instance the ideal pass picked (it has a healthy route, so it must not be moved
  // onto the unhealthy server), and that POOL_SEG_QUERIES reflects each segment's final instance/pool.
  @Test
  public void testStrictReplicaGroupFailOpenKeepsHealthySegmentOnHealthyServer() {
    String unavailableSegment = "segment0";
    String healthySegment = "segment1";
    String instanceA = "instanceA";
    String instanceB = "instanceB";
    String instanceC = "instanceC";
    IdealState idealState = createIdealState(Map.of(
        unavailableSegment, List.of(Pair.of(instanceA, ONLINE)),
        healthySegment, List.of(Pair.of(instanceA, ONLINE), Pair.of(instanceB, ONLINE), Pair.of(instanceC, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        unavailableSegment, List.of(Pair.of(instanceA, ONLINE)),
        healthySegment, List.of(Pair.of(instanceA, ONLINE), Pair.of(instanceB, ONLINE), Pair.of(instanceC, ONLINE))));
    Set<String> onlineSegments = Set.of(unavailableSegment, healthySegment);
    // instanceA is fail-open eligible (present in enabledServerMap, i.e. not disabled by an operator) but not routable
    // (excluded from enabledInstances, simulating failure-detection exclusion) and reported unhealthy, so it's a valid
    // fail-open target.
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instanceA));
    ServerInstance serverA = mock(ServerInstance.class);
    when(serverA.getPool()).thenReturn(1);
    ServerInstance serverB = mock(ServerInstance.class);
    when(serverB.getPool()).thenReturn(2);
    ServerInstance serverC = mock(ServerInstance.class);
    when(serverC.getPool()).thenReturn(3);
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instanceA, serverA, instanceB, serverB, instanceC, serverC);
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
        Set.of(instanceB, instanceC), enabledServerMap, idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);
    clearInvocations(_brokerMetrics);

    // requestId=0: the ideal pass picks instanceB (pool2) for healthySegment from its 2 enabled candidates
    // [instanceB, instanceC]. Fail-open only routes unavailableSegment to instanceA (pool1).
    InstanceSelector.SelectionResult result =
        selector.select(_brokerRequest, List.of(unavailableSegment, healthySegment), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(unavailableSegment, instanceA, healthySegment, instanceB));
    assertTrue(result.getUnavailableSegments().isEmpty());
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(null), "1");
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(null), "2");
  }

  // Scenario: healthySegment's group {instanceA, instanceB} has unhealthy instanceB, while unavailableSegment's group
  // {instanceC, instanceD} is entirely unhealthy, forcing fail-open. Verifies that across request ids, with both round
  // robin and fixed-replica routing, healthySegment stays on its only healthy server instanceA instead of being moved
  // onto unhealthy instanceB, and unavailableSegment still fails open.
  @Test
  public void testStrictReplicaGroupFailOpenDoesNotMoveHealthyGroupOntoUnhealthyServer() {
    String healthySegment = "segment0";
    String unavailableSegment = "segment1";
    String instanceA = "instanceA";
    String instanceB = "instanceB";
    String instanceC = "instanceC";
    String instanceD = "instanceD";
    Map<String, List<Pair<String, String>>> assignment = Map.of(
        healthySegment, List.of(Pair.of(instanceA, ONLINE), Pair.of(instanceB, ONLINE)),
        unavailableSegment, List.of(Pair.of(instanceC, ONLINE), Pair.of(instanceD, ONLINE)));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instanceB, instanceC, instanceD));
    Map<String, ServerInstance> enabledServerMap = Map.of(instanceA, mock(ServerInstance.class), instanceB,
        mock(ServerInstance.class), instanceC, mock(ServerInstance.class), instanceD, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
        Set.of(instanceA), enabledServerMap, createIdealState(assignment), createExternalView(assignment),
        Set.of(healthySegment, unavailableSegment), new FailOpenPolicy(false, true), failureDetector);

    for (boolean useFixedReplica : new boolean[]{false, true}) {
      when(_pinotQuery.getQueryOptions()).thenReturn(
          useFixedReplica ? Map.of(USE_FIXED_REPLICA, Boolean.TRUE.toString()) : null);
      String fixedReplicaFailOpenInstance = null;
      for (int requestId = 0; requestId < 4; requestId++) {
        InstanceSelector.SelectionResult result =
            selector.select(_brokerRequest, List.of(healthySegment, unavailableSegment), requestId);

        String context = "useFixedReplica=" + useFixedReplica + ", requestId=" + requestId;
        assertEquals(result.getSegmentToInstanceMap().get(healthySegment), instanceA, context);
        String failOpenInstance = result.getSegmentToInstanceMap().get(unavailableSegment);
        if (useFixedReplica) {
          // Fixed-replica routing must stick to one fail-open instance across requests.
          if (fixedReplicaFailOpenInstance == null) {
            fixedReplicaFailOpenInstance = failOpenInstance;
          }
          assertEquals(failOpenInstance, fixedReplicaFailOpenInstance, context);
        } else {
          // Round robin over the fail-open candidates [instanceC, instanceD], in sorted ideal-state order.
          assertEquals(failOpenInstance, requestId % 2 == 0 ? instanceC : instanceD, context);
        }
        assertTrue(Set.of(instanceC, instanceD).contains(failOpenInstance), context);
        assertTrue(result.getUnavailableSegments().isEmpty());
      }
    }
  }

  // Scenario: fail-open is enabled on the selector, but nothing is actually unhealthy. Verifies that merely having
  // fail-open configured doesn't change routing versus a selector with fail-open disabled entirely - fail-open must
  // be a no-op when it's never needed, across multiple requestIds.
  @Test
  public void testStrictReplicaGroupFailOpenMatchesIdealWhenAllHealthy() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    List<Pair<String, String>> assignments =
        List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment0, assignments, segment1, assignments));
    ExternalView externalView = createExternalView(Map.of(segment0, assignments, segment1, assignments));
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));

    // Nothing is unhealthy, so the fail-open-enabled selector must produce exactly the same mapping as a selector
    // with fail-open disabled: enabling fail-open must not perturb routing when it is never actually needed.
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of());
    InstanceSelector failOpenSelector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
        Set.of(instance0, instance1), enabledServerMap, idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);
    InstanceSelector idealSelector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
        Set.of(instance0, instance1), idealState, externalView, onlineSegments);

    for (int requestId = 0; requestId < 4; requestId++) {
      InstanceSelector.SelectionResult failOpenResult =
          failOpenSelector.select(_brokerRequest, List.of(segment0, segment1), requestId);
      InstanceSelector.SelectionResult idealResult =
          idealSelector.select(_brokerRequest, List.of(segment0, segment1), requestId);
      assertEquals(failOpenResult.getSegmentToInstanceMap(), idealResult.getSegmentToInstanceMap());
      assertTrue(failOpenResult.getUnavailableSegments().isEmpty());
      assertTrue(idealResult.getUnavailableSegments().isEmpty());
    }
  }

  // Scenario: two independent partitions (disjoint ideal-state instance sets) in the same request - one fully
  // healthy, one fully degraded and relying on fail-open. Verifies the degraded partition's fail-open reselection
  // obeys the strict guarantee (both its segments land on the same instance) without leaking into or perturbing the
  // healthy partition's normal, non-fail-open mapping.
  @Test
  public void testStrictReplicaGroupFailOpenIsolatedToHealthyPartition() {
    String healthySegment0 = "healthySegment0";
    String healthySegment1 = "healthySegment1";
    String degradedSegment0 = "degradedSegment0";
    String degradedSegment1 = "degradedSegment1";
    String healthyInstance0 = "healthyInstance0";
    String healthyInstance1 = "healthyInstance1";
    String degradedInstance0 = "degradedInstance0";
    String degradedInstance1 = "degradedInstance1";
    Set<String> onlineSegments =
        Set.of(healthySegment0, healthySegment1, degradedSegment0, degradedSegment1);
    // Two independent partitions: segments are grouped by their ideal-state instance set, not by name, so giving
    // each pair a disjoint instance set makes them independent partitions for strict routing purposes.
    List<Pair<String, String>> healthyAssignments =
        List.of(Pair.of(healthyInstance0, ONLINE), Pair.of(healthyInstance1, ONLINE));
    List<Pair<String, String>> degradedAssignments =
        List.of(Pair.of(degradedInstance0, ONLINE), Pair.of(degradedInstance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(
        healthySegment0, healthyAssignments, healthySegment1, healthyAssignments,
        degradedSegment0, degradedAssignments, degradedSegment1, degradedAssignments));
    ExternalView externalView = createExternalView(Map.of(
        healthySegment0, healthyAssignments, healthySegment1, healthyAssignments,
        degradedSegment0, degradedAssignments, degradedSegment1, degradedAssignments));
    Map<String, ServerInstance> enabledServerMap = Map.of(
        healthyInstance0, mock(ServerInstance.class), healthyInstance1, mock(ServerInstance.class),
        degradedInstance0, mock(ServerInstance.class), degradedInstance1, mock(ServerInstance.class));
    // Only the healthy partition's instances are "enabled" (routable), so its segments are always served by normal,
    // non-fail-open selection. The degraded partition's instances are never enabled, so its segments always need
    // fail-open; only degradedInstance1 is reported as a usable fail-open target, so it must be the only candidate
    // the degraded partition can resolve to.
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(degradedInstance1));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
        Set.of(healthyInstance0, healthyInstance1), enabledServerMap, idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);

    List<String> allSegments =
        List.of(healthySegment0, healthySegment1, degradedSegment0, degradedSegment1);
    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, allSegments, 0);

    // The healthy partition is served normally (no fail-open needed) and consistently within its own partition,
    // exactly as the ideal condition would select it.
    String healthyInstanceSelected = result.getSegmentToInstanceMap().get(healthySegment0);
    assertTrue(Set.of(healthyInstance0, healthyInstance1).contains(healthyInstanceSelected));
    assertEquals(result.getSegmentToInstanceMap().get(healthySegment1), healthyInstanceSelected);
    // The degraded partition still obeys the strict guarantee: both its segments resolve to the same fail-open
    // instance, and fail-open on the degraded partition never leaks into or alters the healthy partition's mapping.
    assertEquals(result.getSegmentToInstanceMap().get(degradedSegment0), degradedInstance1);
    assertEquals(result.getSegmentToInstanceMap().get(degradedSegment1), degradedInstance1);
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenPreservesHealthyMappingsAndPoolMetrics(String selectorType) {
    String healthySegment = "segment0";
    String failOpenSegment = "segment1";
    String healthyInstance = "instance0";
    String unhealthyInstance = "instance1";
    Set<String> onlineSegments = Set.of(healthySegment, failOpenSegment);
    IdealState idealState = createIdealState(Map.of(healthySegment, List.of(Pair.of(healthyInstance, ONLINE)),
        failOpenSegment, List.of(Pair.of(unhealthyInstance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(healthySegment, List.of(Pair.of(healthyInstance, ONLINE)),
        failOpenSegment, List.of(Pair.of(unhealthyInstance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(unhealthyInstance));
    ServerInstance healthyServer = mock(ServerInstance.class);
    when(healthyServer.getPool()).thenReturn(1);
    ServerInstance unhealthyServer = mock(ServerInstance.class);
    when(unhealthyServer.getPool()).thenReturn(2);
    BaseInstanceSelector selector = spy((BaseInstanceSelector) createTestInstanceSelector(selectorType,
        Set.of(healthyInstance), Map.of(healthyInstance, healthyServer, unhealthyInstance, unhealthyServer), idealState,
        externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector));
    clearInvocations(_brokerMetrics);

    InstanceSelector.SelectionResult result =
        selector.select(_brokerRequest, List.of(healthySegment, failOpenSegment), 0);

    assertEquals(result.getSegmentToInstanceMap(),
        Map.of(healthySegment, healthyInstance, failOpenSegment, unhealthyInstance));
    // Fail-open always recomputes over the full, original per-query segment list (not just the unavailable subset),
    // so that every selector type sees the exact same iteration order/positions the ideal select() call used.
    verify(selector).selectForFailOpen(eq(List.of(healthySegment, failOpenSegment)), anyInt(),
        any(SegmentStates.class), anyMap());
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(null), "1");
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(null), "2");
  }

  @Test(dataProvider = "selectorType")
  public void testOptionalSegmentIncludedInPoolMetrics(String selectorType) {
    String segment = "segment0";
    String instance = "instance0";
    createSegments(List.of(Pair.of(segment, _mutableClock.millis())));
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of()));
    ServerInstance server = mock(ServerInstance.class);
    when(server.getPool()).thenReturn(3);
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(instance), Map.of(instance, server),
        idealState, externalView, Set.of(segment), FailOpenPolicy.disabled(), mock(FailureDetector.class));
    clearInvocations(_brokerMetrics);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment), 0);

    assertEquals(result.getOptionalSegmentToInstanceMap(), Map.of(segment, instance));
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(null), "3");
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenDisabled(String selectorType) {
    String segment = "segment0";
    String instance = "instance0";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(),
        Map.of(instance, mock(ServerInstance.class)), idealState, externalView, onlineSegments,
        FailOpenPolicy.disabled(), failureDetector);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment), 0);

    assertTrue(result.getSegmentToInstanceMap().isEmpty());
    assertEquals(result.getUnavailableSegments(), List.of(segment));
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ELIGIBLE_REQUESTS, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
    verify(failureDetector, never()).getUnhealthyServers();
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenEnabledForMultiStageQuery(String selectorType) {
    String segment = "segment0";
    String instance = "instance0";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(),
        Map.of(instance, mock(ServerInstance.class)), idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(segment, instance));
    assertTrue(result.getUnavailableSegments().isEmpty());
    verify(failureDetector).getUnhealthyServers();
    verify(failureDetector, never()).markServerHealthy(anyString(), any());
    verify(failureDetector, never()).markServerUnhealthy(anyString(), any());
  }

  @Test(dataProvider = "selectorType")
  public void testMultiStageFailOpenSkipsCandidateLessNewLLCSegment(String selectorType) {
    String oldSegment = "segment0";
    String newSegment = new LLCSegmentName("testTable", 0, 1, _mutableClock.millis()).getSegmentName();
    String unhealthyInstance = "instance0";
    String administrativelyDisabledInstance = "instance1";
    createSegments(List.of(Pair.of(newSegment, _mutableClock.millis())));
    Set<String> onlineSegments = Set.of(oldSegment, newSegment);
    IdealState idealState = createIdealState(Map.of(oldSegment, List.of(Pair.of(unhealthyInstance, ONLINE)),
        newSegment, List.of(Pair.of(administrativelyDisabledInstance, CONSUMING))));
    ExternalView externalView = createExternalView(Map.of(oldSegment, List.of(Pair.of(unhealthyInstance, ONLINE)),
        newSegment, List.of()));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(unhealthyInstance));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(),
        Map.of(unhealthyInstance, mock(ServerInstance.class)), idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_MULTISTAGE_ENGINE, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult result =
        selector.select(_brokerRequest, List.of(oldSegment, newSegment), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(oldSegment, unhealthyInstance));
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenRequiresFullCoverage(String selectorType) {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    IdealState idealState = createIdealState(Map.of(segment0, List.of(Pair.of(instance0, ONLINE)), segment1,
        List.of(Pair.of(instance1, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment0, List.of(Pair.of(instance0, ONLINE)), segment1,
        List.of(Pair.of(instance1, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(),
        Map.of(instance0, mock(ServerInstance.class)), idealState, externalView, onlineSegments,
        new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment0, segment1), 0);

    assertTrue(result.getSegmentToInstanceMap().isEmpty());
    assertEquals(Set.copyOf(result.getUnavailableSegments()), onlineSegments);
    // Fail-open was attempted (eligible and enabled) but could not cover every segment.
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenRejectsAdministrativelyDisabledServer(String selectorType) {
    String segment = "segment0";
    String instance = "instance0";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of(Pair.of(instance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(), Map.of(), idealState, externalView,
        onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment), 0);

    assertTrue(result.getSegmentToInstanceMap().isEmpty());
    assertEquals(result.getUnavailableSegments(), List.of(segment));
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenRejectsUnassignedUnhealthyServer(String selectorType) {
    String segment = "segment0";
    String assignedInstance = "instance0";
    String unassignedInstance = "instance1";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment, List.of(Pair.of(assignedInstance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment, List.of(Pair.of(assignedInstance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(unassignedInstance));
    Map<String, ServerInstance> enabledServerMap = Map.of(assignedInstance, mock(ServerInstance.class),
        unassignedInstance, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(), enabledServerMap, idealState,
        externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment), 0);

    assertTrue(result.getSegmentToInstanceMap().isEmpty());
    assertEquals(result.getUnavailableSegments(), List.of(segment));
  }

  @Test(dataProvider = "selectorType")
  public void testFailOpenDoesNotOverrideHealthyRoutingOrDetectorHealth(String selectorType) {
    String segment = "segment0";
    String healthyInstance = "instance0";
    String unhealthyInstance = "instance1";
    Set<String> onlineSegments = Set.of(segment);
    IdealState idealState = createIdealState(Map.of(segment,
        List.of(Pair.of(healthyInstance, ONLINE), Pair.of(unhealthyInstance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(segment,
        List.of(Pair.of(healthyInstance, ONLINE), Pair.of(unhealthyInstance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(unhealthyInstance));
    Map<String, ServerInstance> enabledServerMap = Map.of(healthyInstance, mock(ServerInstance.class),
        unhealthyInstance, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(healthyInstance), enabledServerMap,
        idealState, externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(segment, healthyInstance));
    assertTrue(result.getUnavailableSegments().isEmpty());
    verify(failureDetector, never()).markServerHealthy(eq(unhealthyInstance), any());
  }

  // Test that we mark new segment as old when external view state converges with ideal state.
  @Test(dataProvider = "selectorType")
  public void testNewSegmentGetsOldWithStateConverge(String selectorType) {
    // Set segment0 as old segment
    String oldSeg = "segment0";
    // Set segment1 as new segment
    String newSeg = "segment1";
    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        List.of(Pair.of(newSeg, _mutableClock.millis() - 100));
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
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> []
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of());

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = Map.of(oldSeg, instance0);

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());

    // Segment1 is not old anymore with state converge.
    externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Segment1 becomes unavailable.
    externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of());

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    selector.onAssignmentChange(idealState, externalView, onlineSegments);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    if (selectorType == STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) {
      expectedBalancedInstanceSelectorResult = Map.of();
      assertEquals(selectionResult.getUnavailableSegments(), List.of(oldSeg, newSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), List.of(newSeg));
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
        Map.of(oldSeg0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online, instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(oldSeg0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);

    // Add a new segment to ideal state with missing external view.
    String newSeg = "segment2";
    onlineSegments = ImmutableSet.of(oldSeg0, oldSeg1, newSeg);
    idealSateMap =
        Map.of(oldSeg0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    idealState = createIdealState(idealSateMap);
    externalViewMap =
        Map.of(oldSeg0, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg1,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    externalView = createExternalView(externalViewMap);
    selector.onAssignmentChange(idealState, externalView, onlineSegments);

    int requestId = 0;
    Map<String, String> expectedResult;
    switch (selectorType) {
      case BALANCED_INSTANCE_SELECTOR:
        expectedResult = Map.of(oldSeg0, instance0, oldSeg1, instance1);
        break;
      case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE: // fall through
      case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
        expectedResult = Map.of(oldSeg0, instance0, oldSeg1, instance0);
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
      assertEquals(selectionResult.getUnavailableSegments(), List.of(oldSeg0, oldSeg1, newSeg));
    } else {
      assertEquals(selectionResult.getUnavailableSegments(), List.of(newSeg));
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
        List.of(Pair.of(newSeg, _mutableClock.millis() - 100));
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
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:online, instance1:online]
    //   [segment1] -> [instance0:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);

    // First selection, we select instance1 for newSeg.
    int requestId = 0;
    Map<String, String> expectedResult;
    switch (selectorType) {
      case BALANCED_INSTANCE_SELECTOR:
        expectedResult = Map.of(oldSeg, instance0);
        break;
      case REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
      case STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE:
        expectedResult = Map.of(oldSeg, instance0, newSeg, instance0);
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
    List<String> changeInstance = List.of(instance0);
    selector.onInstancesChange(enabledInstances, changeInstance);
    selectionResult = selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    // We don't include instance0 in selection anymore.
    expectedResult = Map.of(oldSeg, instance1);

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
        List.of(Pair.of(oldSeg, _mutableClock.millis() - NEW_SEGMENT_EXPIRATION_MILLIS - 100),
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
        Map.of(oldSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), newSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance2: online]
    //   [segment1] -> [instance2: online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(oldSeg, List.of(Pair.of(instance2, ONLINE)), newSeg,
            List.of(Pair.of(instance2, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);

    // No selection because the external view is not in ideal state.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = Map.of();
    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertEquals(selectionResult.getUnavailableSegments(), List.of(oldSeg));
  }

  @Test(dataProvider = "selectorType")
  public void testExcludeIdealStateOffline(String selectorType) {
    // Set segment0 as old segment
    String newSeg = "segment0";
    String oldSeg = "segment1";

    List<Pair<String, Long>> segmentCreationTimeMsPairs =
        List.of(Pair.of(newSeg, _mutableClock.millis() - 100),
            Pair.of(oldSeg, _mutableClock.millis() - NEW_SEGMENT_EXPIRATION_MILLIS - 100));
    createSegments(segmentCreationTimeMsPairs);
    Set<String> onlineSegments = ImmutableSet.of(newSeg, oldSeg);

    // Set up instances
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> enabledInstances = ImmutableSet.of(instance0, instance1);
    // Set up ideal state:
    // Ideal states for two segments
    //   [segment0] -> [instance0:online, instance1:offline]
    Map<String, List<Pair<String, String>>> idealSateMap =
        Map.of(newSeg, List.of(Pair.of(instance0, OFFLINE), Pair.of(instance1, ONLINE)), oldSeg,
            List.of(Pair.of(instance0, OFFLINE), Pair.of(instance1, ONLINE)));

    IdealState idealState = createIdealState(idealSateMap);

    // Set up external view:
    // External view for two segments
    //   [segment0] -> [instance0:offline, instance1:online]
    Map<String, List<Pair<String, String>>> externalViewMap =
        Map.of(newSeg, List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)), oldSeg,
            List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE)));

    ExternalView externalView = createExternalView(externalViewMap);

    InstanceSelector selector =
        createTestInstanceSelector(selectorType, enabledInstances, idealState, externalView, onlineSegments);

    // We don't mark segment as unavailable.
    int requestId = 0;
    Map<String, String> expectedBalancedInstanceSelectorResult = Map.of(oldSeg, instance1, newSeg, instance1);

    InstanceSelector.SelectionResult selectionResult =
        selector.select(_brokerRequest, Lists.newArrayList(onlineSegments), requestId);
    assertEquals(selectionResult.getSegmentToInstanceMap(), expectedBalancedInstanceSelectorResult);
    assertTrue(selectionResult.getUnavailableSegments().isEmpty());
  }

  // Replica health metrics
  //
  // The scenarios below all use the same three instances and assert on the TableReplicaHealth the
  // selector derives, since that is what the gauges are emitted from. Each segment's percentage is
  // measured against the replicas its own ideal state assigns, so segments do not have to be uniformly
  // replicated for the numbers to make sense.

  private static final String REPLICA_INSTANCE_0 = "instance0";
  private static final String REPLICA_INSTANCE_1 = "instance1";
  private static final String REPLICA_INSTANCE_2 = "instance2";
  private static final Set<String> REPLICA_INSTANCES =
      ImmutableSet.of(REPLICA_INSTANCE_0, REPLICA_INSTANCE_1, REPLICA_INSTANCE_2);

  /// Returns the ideal state assignment placing the segment on all three instances as ONLINE.
  private static List<Pair<String, String>> allOnline() {
    return List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, ONLINE), new ImmutablePair<>(REPLICA_INSTANCE_1, ONLINE),
        new ImmutablePair<>(REPLICA_INSTANCE_2, ONLINE));
  }

  /// Returns an assignment placing the segment on two of the three instances as ONLINE.
  private static List<Pair<String, String>> twoReplicas() {
    return List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, ONLINE), new ImmutablePair<>(REPLICA_INSTANCE_1, ONLINE));
  }

  /// Returns an external view assignment where the first `numOnline` of the three instances are ONLINE
  /// and the rest are OFFLINE, so the segment looks partially loaded.
  private static List<Pair<String, String>> partiallyOnline(int numOnline) {
    List<String> instances = List.of(REPLICA_INSTANCE_0, REPLICA_INSTANCE_1, REPLICA_INSTANCE_2);
    List<Pair<String, String>> assignment = new ArrayList<>(instances.size());
    for (int i = 0; i < instances.size(); i++) {
      assignment.add(new ImmutablePair<>(instances.get(i), i < numOnline ? ONLINE : OFFLINE));
    }
    return assignment;
  }

  /// Returns an external view assignment where every instance but `offlineInstance` is ONLINE, so that
  /// different segments can be made to lose different replicas.
  private static List<Pair<String, String>> onlineExcept(String offlineInstance) {
    List<Pair<String, String>> assignment = new ArrayList<>(REPLICA_INSTANCES.size());
    for (String instance : List.of(REPLICA_INSTANCE_0, REPLICA_INSTANCE_1, REPLICA_INSTANCE_2)) {
      assignment.add(new ImmutablePair<>(instance, instance.equals(offlineInstance) ? OFFLINE : ONLINE));
    }
    return assignment;
  }

  private BaseInstanceSelector createReplicaHealthSelector(String selectorType, Set<String> enabledInstances,
      Map<String, List<Pair<String, String>>> idealStateAssignment,
      Map<String, List<Pair<String, String>>> externalViewAssignment) {
    // Sorted so that the order the selector looks up segment metadata in is deterministic, which is what
    // the stub set up by createSegmentCreationTimes matches on
    return (BaseInstanceSelector) createTestInstanceSelector(selectorType, enabledInstances,
        createIdealState(idealStateAssignment), createExternalView(externalViewAssignment),
        new TreeSet<>(externalViewAssignment.keySet()));
  }

  /// Stubs the segment metadata lookup with the given creation times, in the order the selector reads them.
  private void createSegmentCreationTimes(Map<String, Long> creationTimeMsBySegment) {
    List<Pair<String, Long>> creationTimes = new ArrayList<>(creationTimeMsBySegment.size());
    for (String segment : new TreeSet<>(creationTimeMsBySegment.keySet())) {
      creationTimes.add(new ImmutablePair<>(segment, creationTimeMsBySegment.get(segment)));
    }
    createSegments(creationTimes);
  }

  /// Marks the given segments as created long enough ago that they are no longer treated as new, so that
  /// they count towards the replica health even though their external view has not converged.
  private void createOldSegments(List<String> segments) {
    long creationTimeMs = _mutableClock.millis() - NEW_SEGMENT_EXPIRATION_MILLIS - 1;
    Map<String, Long> creationTimes = new HashMap<>();
    for (String segment : segments) {
      creationTimes.put(segment, creationTimeMs);
    }
    createSegmentCreationTimes(creationTimes);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthFullyReplicated(String selectorType) {
    // Every segment is ONLINE everywhere the ideal state assigns it
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allOnline(), "segment1", allOnline()),
        Map.of("segment0", allOnline(), "segment1", allOnline()));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 0);
    // Every measured segment sits at the minimum when the minimum is 100, so the count is the measured
    // population rather than 0 - it counts what the percentage speaks for, not what is wrong
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 2);
    // Nothing is degraded, so no expected replica count has to be remembered
    assertTrue(selector._oldSegmentExpectedReplicasMap.isEmpty());
  }

  /// Returns an external view assignment with the first `numOnline` of the three instances ONLINE and the
  /// rest in ERROR, i.e. replicas that failed their state transition rather than merely being offline.
  private static List<Pair<String, String>> partiallyOnlineRestInError(int numOnline) {
    List<String> instances = List.of(REPLICA_INSTANCE_0, REPLICA_INSTANCE_1, REPLICA_INSTANCE_2);
    List<Pair<String, String>> assignment = new ArrayList<>(instances.size());
    for (int i = 0; i < instances.size(); i++) {
      assignment.add(new ImmutablePair<>(instances.get(i), i < numOnline ? ONLINE : ERROR));
    }
    return assignment;
  }

  private static List<Pair<String, String>> singleReplica(String state) {
    return List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, state));
  }

  /// Returns an ideal state assignment placing the segment on all three instances as CONSUMING, i.e. a
  /// segment the controller still considers in progress.
  private static List<Pair<String, String>> allConsuming() {
    return List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, CONSUMING),
        new ImmutablePair<>(REPLICA_INSTANCE_1, CONSUMING), new ImmutablePair<>(REPLICA_INSTANCE_2, CONSUMING));
  }

  @Test(dataProvider = "selectorType")
  public void testConsumingSegmentMeasuredLikeAnyOther(String selectorType) {
    // Consuming segments are deliberately not special-cased. A partition whose replicas are all gone will also
    // raise an ingestion alert, and reconciling that overlap belongs in the alerting pipeline rather than in a
    // metric that would otherwise stop meaning what its name says.
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allConsuming()), Map.of("segment0", partiallyOnline(0)));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 0);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 1);
  }

  @Test(dataProvider = "selectorType")
  public void testHealthyConsumingSegmentReportsFullyReplicated(String selectorType) {
    // The flip side of not excluding them: a partition consuming normally on every replica must read 100%, or
    // every real-time table would look permanently degraded. CONSUMING counts as serving for routing.
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allConsuming()), Map.of("segment0", allConsuming()));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 0);
  }

  @Test(dataProvider = "selectorType")
  public void testCommittedSegmentCountedWhilePeersStillDownloading(String selectorType) {
    // The exclusion has to end at the commit, not when the last replica finishes downloading. The ideal state
    // turns ONLINE at commit while peers still report CONSUMING, and that segment is an ordinary immutable
    // one whose replicas are genuinely missing.
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        // Committed: ideal state ONLINE everywhere
        Map.of("segment0", allOnline()),
        // Only the committer has it; the peers have dropped out rather than reporting CONSUMING
        Map.of("segment0", partiallyOnline(1)));

    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS + 1));
    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 33);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
  }

  @Test(dataProvider = "selectorType")
  public void testCommittingSegmentStillRoutableFromPeersReportingConsuming(String selectorType) {
    // The normal commit window: ideal state ONLINE, peers still CONSUMING in the external view. They remain
    // routable, so nothing is short of replicas and no clock starts.
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allOnline()),
        Map.of("segment0", List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, ONLINE),
            new ImmutablePair<>(REPLICA_INSTANCE_1, CONSUMING), new ImmutablePair<>(REPLICA_INSTANCE_2, CONSUMING))));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
  }

  @Test(dataProvider = "selectorType")
  public void testSegmentsAtMinPercentIgnoresSingleReplicaSegments(String selectorType) {
    // The count is drawn from the same population as the percentage, so a segment the ideal state assigns one
    // replica is left out of it too - otherwise a table that is single-replica by design would report every
    // one of its segments as sitting at the worst level.
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", singleReplica(ONLINE)), Map.of("segment0", singleReplica(OFFLINE)));

    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS * 2));
    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
    // Nothing measured at all, which is the only way the count reaches 0
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 0);
  }

  @Test
  public void testSegmentsAtMinPercentCountsOnlyTheWorstLevel() {
    // segment0 is at 2 of 3 and segment1 at 1 of 3. The count belongs to the percentage that is reported, so
    // only segment1 is in it - a segment that is degraded but better off than the worst must not inflate the
    // blast radius the minimum is describing.
    // Balanced routing only: under strict replica groups segment1's gaps would exclude those groups for
    // segment0 as well, taking both segments to 1 of 3 and hiding the distinction this asserts on.
    createOldSegments(List.of("segment0", "segment1"));
    BaseInstanceSelector selector = createReplicaHealthSelector(BALANCED_INSTANCE_SELECTOR, REPLICA_INSTANCES,
        Map.of("segment0", allOnline(), "segment1", allOnline()),
        Map.of("segment0", partiallyOnline(2), "segment1", partiallyOnline(1)));

    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS * 2));
    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 33);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
  }

  @Test(dataProvider = "selectorType")
  public void testSegmentsAtMinPercentComparesUnevenlyReplicatedSegments(String selectorType) {
    // segment0 is down to 1 of its 2 assigned replicas and segment1 to 1 of its 3. Both have lost all but one
    // replica, yet the percentages differ - 50 against 33 - and it is the percentage that decides membership,
    // so only segment1 is counted. Measuring each segment against its own assignment is what makes the two
    // comparable in the first place.
    createOldSegments(List.of("segment0", "segment1"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", twoReplicas(), "segment1", allOnline()),
        Map.of("segment0", List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, ONLINE),
            new ImmutablePair<>(REPLICA_INSTANCE_1, OFFLINE)), "segment1", partiallyOnline(1)));

    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS * 2));
    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    // The percentage reports the worse of the two, which is the 1-of-3 segment
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 33);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
  }

  @Test(dataProvider = "selectorType")
  public void testSegmentsAtMinPercentCountsTwoReplicaSegmentWithNoReplicaLeft(String selectorType) {
    // A two-replica segment is measured like any other, so losing both takes it to 0% and counts it. A rule
    // keyed on "three or more assigned" would miss a total loss on a two-replica table entirely.
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", twoReplicas()),
        Map.of("segment0", List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, OFFLINE),
            new ImmutablePair<>(REPLICA_INSTANCE_1, OFFLINE))));

    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS * 2));
    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 0);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
  }

  @Test
  public void testSegmentsAtMinPercentWatchesReplicatedPartOfMixedTable() {
    // A real-time table whose consuming segment lives on one replica while its completed segments live on
    // three. Losing two replicas of the completed segment has to be reported, and the thinly replicated
    // consuming segment must neither stop that from happening nor be counted alongside it.
    createOldSegments(List.of("completed"));
    BaseInstanceSelector selector = createReplicaHealthSelector(BALANCED_INSTANCE_SELECTOR, REPLICA_INSTANCES,
        Map.of("consuming", List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, CONSUMING)), "completed", allOnline()),
        Map.of("consuming", List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, CONSUMING)), "completed",
            partiallyOnline(1)));

    _mutableClock.fastForward(Duration.ofMillis(NEW_SEGMENT_EXPIRATION_MILLIS * 2));
    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 33);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthPartiallyReplicated(String selectorType) {
    // segment0 is only loaded on 1 of its 3 replicas, which is the threshold the low replica alert fires on
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allOnline()), Map.of("segment0", partiallyOnline(1)));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 33);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 0);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthUnavailableSegment(String selectorType) {
    // segment0 is loaded nowhere
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allOnline()), Map.of("segment0", partiallyOnline(0)));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 0);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 1);
    // A replicated segment that is unavailable is at 0%, which is as low as the minimum goes, so it is
    // counted by both gauges rather than moving from one to the other
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
  }

  @Test
  public void testReplicaHealthReportsWorstSegmentNotAverage() {
    // segment0 is loaded nowhere while the other two are fully loaded. Reporting the minimum is what keeps
    // a single unservable segment from being diluted by a large healthy table, and the count says how much
    // of the table that minimum is speaking for - here one segment out of the three measured.
    createOldSegments(List.of("segment0"));
    BaseInstanceSelector selector = createReplicaHealthSelector(BALANCED_INSTANCE_SELECTOR, REPLICA_INSTANCES,
        Map.of("segment0", allOnline(), "segment1", allOnline(), "segment2", allOnline()),
        Map.of("segment0", partiallyOnline(0), "segment1", allOnline(), "segment2", allOnline()));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 0);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 1);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 1);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthIgnoresNewSegments(String selectorType) {
    // segment0 was just created and is only loaded on 1 replica. That is expected right after a push, so
    // it must not drag the percentage down.
    createSegmentCreationTimes(Map.of("segment0", _mutableClock.millis()));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allOnline()), Map.of("segment0", partiallyOnline(1)));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthAccountsForDisabledInstance(String selectorType) {
    // A disabled instance reduces the replicas the broker can route to just as much as a missing external
    // view entry does, and it arrives through onInstancesChange rather than an assignment change.
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allOnline()), Map.of("segment0", allOnline()));
    assertEquals(selector.getReplicaHealth().getMinPercentOfReplicas(), 100);

    selector.onInstancesChange(ImmutableSet.of(REPLICA_INSTANCE_0, REPLICA_INSTANCE_1),
        List.of(REPLICA_INSTANCE_2));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 66);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 0);
  }

  @Test
  public void testReplicaHealthReflectsStrictReplicaGroupKnockout() {
    // The two segments share an ideal state assignment but are each missing a different instance: segment0
    // is not loaded on instance2, segment1 is not loaded on instance1. Strict replica group routing takes
    // both instances out of service for every segment in the group, so each segment is left with instance0
    // alone. The external view on its own only ever shows two of three replicas missing per segment, so the
    // group-wide knockout is degradation a metric computed from the external view cannot see.
    createOldSegments(List.of("segment0", "segment1"));
    Map<String, List<Pair<String, String>>> idealStateAssignment =
        Map.of("segment0", allOnline(), "segment1", allOnline());
    Map<String, List<Pair<String, String>>> externalViewAssignment =
        Map.of("segment0", onlineExcept(REPLICA_INSTANCE_2), "segment1", onlineExcept(REPLICA_INSTANCE_1));

    BaseInstanceSelector strictSelector =
        createReplicaHealthSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, REPLICA_INSTANCES,
            idealStateAssignment, externalViewAssignment);
    TableReplicaHealth strictReplicaHealth = strictSelector.getReplicaHealth();
    // 1 of 3, not the 2 of 3 the external view would suggest, and both segments are down to a single replica
    assertEquals(strictReplicaHealth.getMinPercentOfReplicas(), 33);
    assertEquals(strictReplicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 2);

    // Without the strict guarantee each segment only loses the replica its own external view is missing
    BaseInstanceSelector balancedSelector =
        createReplicaHealthSelector(BALANCED_INSTANCE_SELECTOR, REPLICA_INSTANCES, idealStateAssignment,
            externalViewAssignment);
    TableReplicaHealth balancedReplicaHealth = balancedSelector.getReplicaHealth();
    assertEquals(balancedReplicaHealth.getMinPercentOfReplicas(), 66);
    // Both segments tie at the milder minimum, so the count stays 2 while the percentage improves: it tracks
    // whatever level is currently worst, and is only ever read together with that level
    assertEquals(balancedReplicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 2);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthWithUnevenlyReplicatedSegments(String selectorType) {
    // segment0 is deliberately assigned a single replica, as happens when only part of a table's data sits
    // on a tier with fewer replicas. Measuring it against its own ideal state keeps it at 100 instead of
    // permanently reporting it as under-replicated.
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, ONLINE)), "segment1", allOnline()),
        Map.of("segment0", List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, ONLINE)), "segment1", allOnline()));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthIgnoresIdealStateOfflineReplicas(String selectorType) {
    // An instance the ideal state marks OFFLINE is not expected to serve the segment, so it must not count
    // against the segment either.
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0",
            List.of(new ImmutablePair<>(REPLICA_INSTANCE_0, ONLINE), new ImmutablePair<>(REPLICA_INSTANCE_1, ONLINE),
                new ImmutablePair<>(REPLICA_INSTANCE_2, OFFLINE))),
        Map.of("segment0", allOnline()));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthWithNoOldSegments(String selectorType) {
    // The state right after a table is created: every segment is new, so there is nothing to measure. It
    // has to read as fully replicated, otherwise it would trip the very alert the gauge exists for.
    createSegmentCreationTimes(Map.of("segment0", _mutableClock.millis()));
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        Map.of("segment0", allOnline()), Map.of("segment0", partiallyOnline(0)));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
    // The 100 above is the "nothing to measure" default rather than a measurement, and the count is what
    // tells the two apart
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 0);
    assertEquals(replicaHealth.getNumUnavailableSegments(), 0);
  }

  @Test(dataProvider = "selectorType")
  public void testReplicaHealthRecoversOnAssignmentChange(String selectorType) {
    // The expected replica counts are cached until the next assignment change rebuilds them, so a segment
    // that converges has to stop being reported as degraded.
    createOldSegments(List.of("segment0"));
    Map<String, List<Pair<String, String>>> idealStateAssignment = Map.of("segment0", allOnline());
    BaseInstanceSelector selector = createReplicaHealthSelector(selectorType, REPLICA_INSTANCES,
        idealStateAssignment, Map.of("segment0", partiallyOnline(1)));
    assertEquals(selector.getReplicaHealth().getMinPercentOfReplicas(), 33);

    selector.onAssignmentChange(createIdealState(idealStateAssignment),
        createExternalView(Map.of("segment0", allOnline())), Set.of("segment0"));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 100);
    // The cached counts have to be dropped on rebuild, or the segment stays degraded forever
    assertEquals(selector._oldSegmentExpectedReplicasMap, Map.of());
  }

  @Test
  public void testReplicaHealthWithUpsertTableAppliesStrictReplicaGroupRules() {
    // An upsert table gets the strict replica-group treatment even under the plain replica-group selector,
    // so the group-wide knockout has to be reflected there too.
    when(_tableConfig.isUpsertEnabled()).thenReturn(true);
    createOldSegments(List.of("segment0", "segment1"));
    // Same setup as testReplicaHealthReflectsStrictReplicaGroupKnockout: a different instance missing per
    // segment, so 33 can only come from the group-wide knockout and not from either segment's own view
    BaseInstanceSelector selector =
        createReplicaHealthSelector(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, REPLICA_INSTANCES,
            Map.of("segment0", allOnline(), "segment1", allOnline()),
            Map.of("segment0", onlineExcept(REPLICA_INSTANCE_2), "segment1", onlineExcept(REPLICA_INSTANCE_1)));

    TableReplicaHealth replicaHealth = selector.getReplicaHealth();
    assertEquals(replicaHealth.getMinPercentOfReplicas(), 33);
    assertEquals(replicaHealth.getNumSegmentsAtMinPercentOfReplicas(), 2);
  }

  @Test
  public void testReplicaHealthMeasuredForDisabledTable() {
    // The selector measures a disabled table like any other - it cannot tell the difference, and it is the
    // routing manager that decides a disabled table's gauges should be dropped rather than reported.
    createOldSegments(List.of("segment0"));
    IdealState disabledIdealState = createIdealState(Map.of("segment0", allOnline()));
    disabledIdealState.enable(false);
    BalancedInstanceSelector selector = new BalancedInstanceSelector();
    selector.init(_tableConfig, _propertyStore, _brokerMetrics, null, _mutableClock, INSTANCE_SELECTOR_CONFIG,
        REPLICA_INSTANCES, EMPTY_SERVER_MAP, disabledIdealState,
        createExternalView(Map.of("segment0", partiallyOnline(0))), Set.of("segment0"));

    assertEquals(selector.getReplicaHealth().getNumUnavailableSegments(), 1);
    // The selector never touches the replica health gauges itself
    verify(_brokerMetrics, never()).setValueOfTableGauge(eq(TABLE_NAME), any(BrokerGauge.class), anyLong());
    verify(_brokerMetrics, never()).removeTableGauge(eq(TABLE_NAME), any(BrokerGauge.class));
  }

  @DataProvider(name = "poolMetricsSelector")
  public Object[] getPoolMetricsSelector() {
    return new Object[]{BALANCED_INSTANCE_SELECTOR, REPLICA_GROUP_INSTANCE_SELECTOR_TYPE};
  }

  @Test(dataProvider = "poolMetricsSelector")
  public void testSelectedPoolMetricsAndRequestIsolation(String selectorType) {
    BaseInstanceSelector selector = selectorType.equals(BALANCED_INSTANCE_SELECTOR)
        ? new BalancedInstanceSelector()
        : new ReplicaGroupInstanceSelector();
    selector._brokerMetrics = _brokerMetrics;
    selector._config = INSTANCE_SELECTOR_CONFIG;
    Map<String, List<SegmentInstanceCandidate>> candidates = new HashMap<>();
    Map<String, String> expectedInstances = new HashMap<>();
    List<String> segments = new ArrayList<>();
    // Pool metrics are recorded from the final mapping, resolving each instance's pool through the server store.
    Map<String, ServerInstance> serverStore = new HashMap<>();
    selector._enabledServerStore = serverStore;
    // Exercise the fallback pool, the zero key, and IDs/counts outside the Integer cache.
    for (int pool : new int[]{-1, 0, 128}) {
      String instance = "instance" + pool;
      ServerInstance server = mock(ServerInstance.class);
      when(server.getPool()).thenReturn(pool);
      serverStore.put(instance, server);
      for (int i = 0; i < 129; i++) {
        String segment = "segment_" + pool + "_" + i;
        candidates.put(segment, List.of(new SegmentInstanceCandidate(instance, true, pool, 0)));
        expectedInstances.put(segment, instance);
        // Routing matches segment names by value, even when query and metadata use different String objects.
        segments.add(new String(segment));
      }
    }
    candidates.put("optional", List.of(new SegmentInstanceCandidate("instance128", false, 128, 0)));
    segments.addAll(List.of("optional", "unavailable", "pending"));
    selector._segmentStates = new SegmentStates(candidates, Set.of("instance-1", "instance0", "instance128"),
        Set.of("unavailable", "unrequestedUnavailable"));
    Map<String, String> queryOptions = Map.of(ORDERED_PREFERRED_POOLS, "128|0");
    when(_pinotQuery.getQueryOptions()).thenReturn(queryOptions);

    InstanceSelector.SelectionResult first = selector.select(_brokerRequest, segments, 0L);

    assertEquals(first.getSegmentToInstanceMap(), expectedInstances);
    assertEquals(first.getOptionalSegmentToInstanceMap(), Map.of("optional", "instance128"));
    assertEquals(first.getUnavailableSegments(), List.of("unavailable"));
    String preferredPoolTag = BrokerMetrics.getTagForPreferredPool(queryOptions);
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 129L, preferredPoolTag, "-1");
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 129L, preferredPoolTag, "0");
    // Optional segments count as selected; unavailable segments and metadata not yet present do not.
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 130L, preferredPoolTag, "128");
    verifyNoMoreInteractions(_brokerMetrics);

    clearInvocations(_brokerMetrics);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of());
    InstanceSelector.SelectionResult second = selector.select(_brokerRequest, List.of("segment_0_0"), 1L);

    assertEquals(second.getSegmentToInstanceMap(), Map.of("segment_0_0", "instance0"));
    assertTrue(second.getOptionalSegmentToInstanceMap().isEmpty());
    assertTrue(second.getUnavailableSegments().isEmpty());
    verify(_brokerMetrics).addMeteredValue(BrokerMeter.POOL_SEG_QUERIES, 1L,
        BrokerMetrics.getTagForPreferredPool(Map.of()), "0");
    verifyNoMoreInteractions(_brokerMetrics);
    assertEquals(first.getSegmentToInstanceMap(), expectedInstances);
    assertEquals(first.getOptionalSegmentToInstanceMap(), Map.of("optional", "instance128"));
  }

  @Test
  public void testSortByKeyIfNeeded() {
    Map<String, String> sorted = new LinkedHashMap<>();
    sorted.put("a", ONLINE);
    sorted.put("b", ONLINE);
    sorted.put("c", ONLINE);
    assertSame(BaseInstanceSelector.sortByKeyIfNeeded(sorted), sorted);

    Map<String, String> treeMap = new TreeMap<>(sorted);
    assertSame(BaseInstanceSelector.sortByKeyIfNeeded(treeMap), treeMap);

    Map<String, String> unsorted = new LinkedHashMap<>();
    unsorted.put("c", ONLINE);
    unsorted.put("a", ONLINE);
    unsorted.put("b", ONLINE);
    Map<String, String> sortedCopy = BaseInstanceSelector.sortByKeyIfNeeded(unsorted);
    assertNotSame(sortedCopy, unsorted);
    assertEquals(new ArrayList<>(sortedCopy.keySet()), List.of("a", "b", "c"));
    assertEquals(sortedCopy, unsorted);
  }

  /// Segments with equal ideal state and external view instance state maps share one candidate list within a
  /// rebuild. Checks that the sharing does not change any routing outcome, by comparing a selector that shares with
  /// one that builds a list per segment, and that the candidates keep the sorted instance order when the instance
  /// state maps are not in sorted order.
  @DataProvider(name = "sharedCandidateListsCases")
  public Object[][] getSharedCandidateListsCases() {
    return new Object[][]{
        {REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false}, {STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false},
        {BALANCED_INSTANCE_SELECTOR, false},
        // An upsert table makes the replica-group selector apply the strict replica-group rules. The strict
        // replica-group selector needs no upsert case, because it applies these rules for every table.
        {REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, true}
    };
  }

  @Test(dataProvider = "sharedCandidateListsCases")
  public void testSharedCandidateListsMatchPerSegmentLists(String selectorType, boolean upsert) {
    when(_tableConfig.isUpsertEnabled()).thenReturn(upsert);
    // Partition 0: the ideal state lists the instances out of order. One of its old segments has a replica OFFLINE in
    // the external view.
    // Partition 1: one of its instances is disabled.
    String[] partition0Instances = {"instance_c", "instance_a", "instance_b"};
    String[] partition1Instances = {"instance_d", "instance_e", "instance_f"};
    Set<String> enabledInstances =
        Set.of("instance_a", "instance_b", "instance_c", "instance_d", "instance_e", "instance_g");

    Map<String, Map<String, String>> idealStateMaps = new LinkedHashMap<>();
    Map<String, Map<String, String>> externalViewMaps = new LinkedHashMap<>();
    for (int i = 0; i < 4; i++) {
      idealStateMaps.put("p0_" + i, instanceStateMap(partition0Instances, ONLINE, ONLINE, ONLINE));
      externalViewMaps.put("p0_" + i, i == 3 ? instanceStateMap(partition0Instances, ONLINE, ONLINE, OFFLINE)
          : instanceStateMap(partition0Instances, ONLINE, ONLINE, ONLINE));
    }
    for (int i = 0; i < 3; i++) {
      String state = i == 2 ? CONSUMING : ONLINE;
      idealStateMaps.put("p1_" + i, instanceStateMap(partition1Instances, state, state, state));
      externalViewMaps.put("p1_" + i, instanceStateMap(partition1Instances, state, state, state));
    }
    // Old segment missing from the external view: unavailable
    idealStateMaps.put("p2_0", instanceStateMap(new String[]{"instance_g"}, ONLINE));
    IdealState initialIdealState = createInsertionOrderedIdealState(idealStateMaps);
    ExternalView initialExternalView = createInsertionOrderedExternalView(externalViewMaps);
    Set<String> initialOnlineSegments = new HashSet<>(idealStateMaps.keySet());

    // New segments, first seen by the assignment change: one missing from the external view, one partially online
    idealStateMaps.put("new_0", instanceStateMap(partition0Instances, ONLINE, ONLINE, ONLINE));
    idealStateMaps.put("new_1", instanceStateMap(partition0Instances, ONLINE, ONLINE, ONLINE));
    externalViewMaps.put("new_1", Map.of("instance_a", ONLINE));
    IdealState idealState = createInsertionOrderedIdealState(idealStateMaps);
    ExternalView externalView = createInsertionOrderedExternalView(externalViewMaps);
    Set<String> onlineSegments = new HashSet<>(idealStateMaps.keySet());

    BaseInstanceSelector sharingSelector = (BaseInstanceSelector) createTestInstanceSelector(selectorType,
        enabledInstances, initialIdealState, initialExternalView, initialOnlineSegments);
    sharingSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    BaseInstanceSelector perSegmentSelector = (BaseInstanceSelector) createTestInstanceSelector(selectorType,
        enabledInstances, initialIdealState, initialExternalView, initialOnlineSegments);
    perSegmentSelector._maxSharedCandidateLists = 0;
    perSegmentSelector.onAssignmentChange(idealState, externalView, onlineSegments);
    // Shares the lists of one pair of instance state maps only, so that one rebuild mixes shared and own lists
    BaseInstanceSelector boundedSelector = (BaseInstanceSelector) createTestInstanceSelector(selectorType,
        enabledInstances, initialIdealState, initialExternalView, initialOnlineSegments);
    boundedSelector._maxSharedCandidateLists = 1;
    boundedSelector.onAssignmentChange(idealState, externalView, onlineSegments);

    // Same candidates for every segment
    SegmentStates sharedStates = sharingSelector._segmentStates;
    SegmentStates perSegmentStates = perSegmentSelector._segmentStates;
    SegmentStates boundedStates = boundedSelector._segmentStates;
    for (String segment : onlineSegments) {
      assertEquals(describe(sharedStates.getCandidates(segment)), describe(perSegmentStates.getCandidates(segment)),
          segment);
      assertEquals(describe(boundedStates.getCandidates(segment)), describe(perSegmentStates.getCandidates(segment)),
          segment);
    }
    assertEquals(boundedStates.getUnavailableSegments(), perSegmentStates.getUnavailableSegments());
    assertEquals(boundedStates.getServingInstances(), perSegmentStates.getServingInstances());
    assertEquals(sharedStates.getUnavailableSegments(), Set.of("p2_0"));
    assertEquals(sharedStates.getUnavailableSegments(), perSegmentStates.getUnavailableSegments());
    assertEquals(sharedStates.getServingInstances(), perSegmentStates.getServingInstances());
    TableReplicaHealth sharedHealth = sharingSelector.getReplicaHealth();
    TableReplicaHealth perSegmentHealth = perSegmentSelector.getReplicaHealth();
    assertEquals(sharedHealth.getMinPercentOfReplicas(), perSegmentHealth.getMinPercentOfReplicas());
    assertEquals(sharedHealth.getNumSegmentsAtMinPercentOfReplicas(),
        perSegmentHealth.getNumSegmentsAtMinPercentOfReplicas());
    assertEquals(sharedHealth.getNumUnavailableSegments(), perSegmentHealth.getNumUnavailableSegments());

    // Candidates are in sorted instance order, with the replica id of the sorted ideal state, whatever the order of
    // the instance state maps
    boolean strict = selectorType.equals(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE) || upsert;
    // Strict replica-group takes instance_b out of service for the whole partition, because it is OFFLINE for p0_3
    assertEquals(describe(sharedStates.getCandidates("p0_0")),
        strict ? List.of("instance_a/0/true", "instance_c/2/true")
            : List.of("instance_a/0/true", "instance_b/1/true", "instance_c/2/true"));
    assertEquals(describe(sharedStates.getCandidates("p0_3")), List.of("instance_a/0/true", "instance_c/2/true"));
    // instance_f is disabled
    assertEquals(describe(sharedStates.getCandidates("p1_0")), List.of("instance_d/0/true", "instance_e/1/true"));
    assertEquals(describe(sharedStates.getCandidates("new_0")),
        strict ? List.of("instance_a/0/false", "instance_c/2/false")
            : List.of("instance_a/0/false", "instance_b/1/false", "instance_c/2/false"));
    assertEquals(describe(sharedStates.getCandidates("new_1")),
        strict ? List.of("instance_a/0/true", "instance_c/2/false")
            : List.of("instance_a/0/true", "instance_b/1/false", "instance_c/2/false"));

    // The sharing selector shares the lists of segments with equal instance state maps, the other one does not
    assertSame(sharedStates.getCandidates("p0_0"), sharedStates.getCandidates("p0_1"));
    assertSame(sharedStates.getCandidates("p0_0"), sharedStates.getCandidates("p0_2"));
    assertNotSame(perSegmentStates.getCandidates("p0_0"), perSegmentStates.getCandidates("p0_1"));

    // Same selection for every request
    List<String> segments = new ArrayList<>(idealStateMaps.keySet());
    for (long requestId = 0; requestId < 6; requestId++) {
      InstanceSelector.SelectionResult shared = sharingSelector.select(_brokerRequest, segments, requestId);
      InstanceSelector.SelectionResult perSegment = perSegmentSelector.select(_brokerRequest, segments, requestId);
      InstanceSelector.SelectionResult bounded = boundedSelector.select(_brokerRequest, segments, requestId);
      assertEquals(shared.getSegmentToInstanceMap(), perSegment.getSegmentToInstanceMap());
      assertEquals(shared.getOptionalSegmentToInstanceMap(), perSegment.getOptionalSegmentToInstanceMap());
      assertEquals(shared.getUnavailableSegments(), perSegment.getUnavailableSegments());
      assertEquals(shared.getUnavailableSegments(), List.of("p2_0"));
      assertEquals(bounded.getSegmentToInstanceMap(), perSegment.getSegmentToInstanceMap());
      assertEquals(bounded.getOptionalSegmentToInstanceMap(), perSegment.getOptionalSegmentToInstanceMap());
    }

    // Enabling the disabled instance keeps both selectors in step
    Set<String> allInstances = new HashSet<>(enabledInstances);
    allInstances.add("instance_f");
    sharingSelector.onInstancesChange(allInstances, List.of("instance_f"));
    perSegmentSelector.onInstancesChange(allInstances, List.of("instance_f"));
    assertEquals(describe(sharingSelector._segmentStates.getCandidates("p1_0")),
        List.of("instance_d/0/true", "instance_e/1/true", "instance_f/2/true"));
    assertSame(sharingSelector._segmentStates.getCandidates("p1_0"),
        sharingSelector._segmentStates.getCandidates("p1_1"));
    for (long requestId = 0; requestId < 6; requestId++) {
      InstanceSelector.SelectionResult shared = sharingSelector.select(_brokerRequest, segments, requestId);
      InstanceSelector.SelectionResult perSegment = perSegmentSelector.select(_brokerRequest, segments, requestId);
      assertEquals(shared.getSegmentToInstanceMap(), perSegment.getSegmentToInstanceMap());
      assertEquals(shared.getOptionalSegmentToInstanceMap(), perSegment.getOptionalSegmentToInstanceMap());
    }
  }

  /// Returns an insertion-ordered instance state map, keeping the order of the given instances.
  private static Map<String, String> instanceStateMap(String[] instances, String... states) {
    Map<String, String> instanceStateMap = new LinkedHashMap<>();
    for (int i = 0; i < instances.length; i++) {
      instanceStateMap.put(instances[i], states[i]);
    }
    return instanceStateMap;
  }

  private static IdealState createInsertionOrderedIdealState(Map<String, Map<String, String>> instanceStateMaps) {
    IdealState idealState = new IdealState(TABLE_NAME);
    for (Map.Entry<String, Map<String, String>> entry : instanceStateMaps.entrySet()) {
      idealState.getRecord().setMapField(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
    }
    return idealState;
  }

  private static ExternalView createInsertionOrderedExternalView(Map<String, Map<String, String>> instanceStateMaps) {
    ExternalView externalView = new ExternalView(TABLE_NAME);
    for (Map.Entry<String, Map<String, String>> entry : instanceStateMaps.entrySet()) {
      externalView.getRecord().setMapField(entry.getKey(), new LinkedHashMap<>(entry.getValue()));
    }
    return externalView;
  }

  private static List<String> describe(@Nullable List<SegmentInstanceCandidate> candidates) {
    if (candidates == null) {
      return null;
    }
    List<String> descriptions = new ArrayList<>(candidates.size());
    for (SegmentInstanceCandidate candidate : candidates) {
      descriptions.add(candidate.getInstance() + "/" + candidate.getIdealStateReplicaId() + "/" + candidate.isOnline());
    }
    return descriptions;
  }

  // Scenario: a second, uncached segment is unavailable and fail-open-eligible, but the request only asks for a
  // separate, fully healthy segment. Expected: fail-open must only engage for segments that are actually part of the
  // current request - the unrequested unavailable segment must not trigger a FailureDetector lookup or any fail-open
  // bookkeeping, and the result must be the plain healthy mapping.
  @Test(dataProvider = "selectorType")
  public void testFailOpenNotInvokedForUnavailableSegmentOutsideRequest(String selectorType) {
    String requestedSegment = "segment0";
    String unrequestedSegment = "segment1";
    String healthyInstance = "instance0";
    String unavailableInstance = "instance1";
    Set<String> onlineSegments = Set.of(requestedSegment, unrequestedSegment);
    IdealState idealState = createIdealState(Map.of(
        requestedSegment, List.of(Pair.of(healthyInstance, ONLINE)),
        unrequestedSegment, List.of(Pair.of(unavailableInstance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        requestedSegment, List.of(Pair.of(healthyInstance, ONLINE)),
        unrequestedSegment, List.of(Pair.of(unavailableInstance, ONLINE))));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(unavailableInstance));
    Map<String, ServerInstance> enabledServerMap = Map.of(
        healthyInstance, mock(ServerInstance.class), unavailableInstance, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(healthyInstance), enabledServerMap,
        idealState, externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(requestedSegment), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(requestedSegment, healthyInstance));
    assertTrue(result.getUnavailableSegments().isEmpty());
    verify(failureDetector, never()).getUnhealthyServers();
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
  }

  // Scenario: fixed-replica ("sticky") routing on BalancedInstanceSelector during fail-open. Mirrors
  // testReplicaGroupFailOpenPreservesFixedReplicaQueryOption but exercises BalancedInstanceSelector's own
  // _tableNameHashForFixedReplicaRouting-based fixed-replica formula. Expected: both calls resolve to the exact
  // same instance - fixed-replica stickiness holds over the fail-open-expanded candidate pool exactly as it does
  // over the normal pool, instead of alternating via round robin.
  @Test
  public void testBalancedFailOpenPreservesFixedReplicaQueryOption() {
    String segment = "segment0";
    String instance0 = "instance0";
    String instance1 = "instance1";
    List<Pair<String, String>> assignments = List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment, assignments));
    ExternalView externalView = createExternalView(Map.of(segment, assignments));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    InstanceSelector selector = createTestInstanceSelector(BALANCED_INSTANCE_SELECTOR, Set.of(),
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class)), idealState,
        externalView, Set.of(segment), new FailOpenPolicy(false, true), failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_FIXED_REPLICA, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult first = selector.select(_brokerRequest, List.of(segment), 0);
    InstanceSelector.SelectionResult second = selector.select(_brokerRequest, List.of(segment), 1);

    assertEquals(first.getSegmentToInstanceMap(), second.getSegmentToInstanceMap());
    assertTrue(first.getUnavailableSegments().isEmpty());
  }

  // Scenario: fixed-replica routing on StrictReplicaGroupInstanceSelector during fail-open, across a 2-segment
  // partition that shares both fail-open-eligible instances. Expected: both segments resolve to the same instance
  // as each other (strict colocation) on every call, and that instance does not change across requestIds
  // (fixed-replica stickiness), proving the two guarantees compose correctly during fail-open.
  @Test
  public void testStrictReplicaGroupFailOpenPreservesFixedReplicaQueryOption() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    List<Pair<String, String>> assignments = List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment0, assignments, segment1, assignments));
    ExternalView externalView = createExternalView(Map.of(segment0, assignments, segment1, assignments));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true),
        failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(USE_FIXED_REPLICA, Boolean.TRUE.toString()));

    InstanceSelector.SelectionResult first = selector.select(_brokerRequest, List.of(segment0, segment1), 0);
    InstanceSelector.SelectionResult second = selector.select(_brokerRequest, List.of(segment0, segment1), 1);

    String firstInstance = first.getSegmentToInstanceMap().get(segment0);
    assertEquals(first.getSegmentToInstanceMap(), Map.of(segment0, firstInstance, segment1, firstInstance));
    assertEquals(second.getSegmentToInstanceMap(), first.getSegmentToInstanceMap());
    assertTrue(first.getUnavailableSegments().isEmpty());
    assertTrue(second.getUnavailableSegments().isEmpty());
  }

  // Scenario: NUM_REPLICA_GROUPS_TO_QUERY (5) exceeds the number of fail-open candidates available (2). Mirrors
  // ReplicaGroupSelectorTest#testReplicaGroupInstanceSelectorNumReplicaGroupsToQueryGreaterThanReplicas, but under
  // fail-open. Expected: selectServers() clamps numReplicaGroups down to the candidate count (2) instead of trying
  // to cycle through replicaOffsets the candidate list can't satisfy, so the two segments still fan out across
  // exactly the two available instances (offset 0 then 1), the same as if NUM_REPLICA_GROUPS_TO_QUERY had been
  // requested as 2.
  @Test
  public void testReplicaGroupFailOpenNumReplicaGroupsToQueryGreaterThanReplicas() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    List<Pair<String, String>> assignments = List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment0, assignments, segment1, assignments));
    ExternalView externalView = createExternalView(Map.of(segment0, assignments, segment1, assignments));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true),
        failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(NUM_REPLICA_GROUPS_TO_QUERY, "5"));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment0, segment1), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(segment0, instance0, segment1, instance1));
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  // Scenario: NUM_REPLICA_GROUPS_TO_QUERY is set on a strict replica-group selector whose whole partition needs
  // fail-open. As StrictReplicaGroupInstanceSelector#select() documents, this option forces the non-adaptive
  // selectServers() path - the same path selectForFailOpen() already uses - but that path also advances
  // replicaOffset once per segment, which intentionally contradicts the strict same-instance-per-partition
  // guarantee. Expected: with 2 candidates and NUM_REPLICA_GROUPS_TO_QUERY=2, the two same-partition segments land
  // on different instances (offset 0 then 1) during fail-open, documenting this known, intentional contradiction
  // rather than silently masking it.
  @Test
  public void testStrictReplicaGroupFailOpenWithNumReplicaGroupsToQuery() {
    String segment0 = "segment0";
    String segment1 = "segment1";
    String instance0 = "instance0";
    String instance1 = "instance1";
    Set<String> onlineSegments = Set.of(segment0, segment1);
    List<Pair<String, String>> assignments = List.of(Pair.of(instance0, ONLINE), Pair.of(instance1, ONLINE));
    IdealState idealState = createIdealState(Map.of(segment0, assignments, segment1, assignments));
    ExternalView externalView = createExternalView(Map.of(segment0, assignments, segment1, assignments));
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of(instance0, instance1));
    Map<String, ServerInstance> enabledServerMap =
        Map.of(instance0, mock(ServerInstance.class), instance1, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, Set.of(),
        enabledServerMap, idealState, externalView, onlineSegments, new FailOpenPolicy(false, true),
        failureDetector);
    when(_pinotQuery.getQueryOptions()).thenReturn(Map.of(NUM_REPLICA_GROUPS_TO_QUERY, "2"));

    InstanceSelector.SelectionResult result = selector.select(_brokerRequest, List.of(segment0, segment1), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(segment0, instance0, segment1, instance1));
    assertTrue(result.getUnavailableSegments().isEmpty());
  }

  // Scenario: segment0 is normally routable (healthy); segment1 is unavailable, and its only fail-open-eligible
  // candidate is never reported unhealthy by the FailureDetector, so fail-open cannot find an eligible replacement
  // for it. Expected: fail-open fails safely for the whole query (selectFailOpen returns null internally), so the
  // final result falls back to the original ideal mapping - segment0's normal, healthy assignment is preserved -
  // rather than being wiped out just because segment1 could not be fail-opened. Only segment1 is reported
  // unavailable, and FAIL_OPEN_ROUTING_FAILURES increments while FAIL_OPEN_ROUTING_SUCCESSES does not. The two
  // segments use disjoint instance sets (independent partitions), so retaining the healthy segment is legal even
  // for the strict replica-group parameterization.
  @Test(dataProvider = "selectorType")
  public void testFailedFailOpenPreservesPartialNormalAssignment(String selectorType) {
    String healthySegment = "segment0";
    String failedSegment = "segment1";
    String healthyInstance = "instance0";
    String failOpenEligibleInstance = "instance1";
    Set<String> onlineSegments = Set.of(healthySegment, failedSegment);
    IdealState idealState = createIdealState(Map.of(
        healthySegment, List.of(Pair.of(healthyInstance, ONLINE)),
        failedSegment, List.of(Pair.of(failOpenEligibleInstance, ONLINE))));
    ExternalView externalView = createExternalView(Map.of(
        healthySegment, List.of(Pair.of(healthyInstance, ONLINE)),
        failedSegment, List.of(Pair.of(failOpenEligibleInstance, ONLINE))));
    // failOpenEligibleInstance is never reported unhealthy, so fail-open has no eligible candidate for failedSegment.
    FailureDetector failureDetector = mock(FailureDetector.class);
    when(failureDetector.getUnhealthyServers()).thenReturn(Set.of());
    Map<String, ServerInstance> enabledServerMap = Map.of(
        healthyInstance, mock(ServerInstance.class), failOpenEligibleInstance, mock(ServerInstance.class));
    InstanceSelector selector = createTestInstanceSelector(selectorType, Set.of(healthyInstance), enabledServerMap,
        idealState, externalView, onlineSegments, new FailOpenPolicy(false, true), failureDetector);

    InstanceSelector.SelectionResult result =
        selector.select(_brokerRequest, List.of(healthySegment, failedSegment), 0);

    assertEquals(result.getSegmentToInstanceMap(), Map.of(healthySegment, healthyInstance));
    assertEquals(result.getUnavailableSegments(), List.of(failedSegment));
    verify(_brokerMetrics, never()).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_SUCCESSES, 1);
    verify(_brokerMetrics).addMeteredGlobalValue(BrokerMeter.FAIL_OPEN_ROUTING_FAILURES, 1);
  }
}
