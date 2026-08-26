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
package org.apache.pinot.broker.routing.manager;

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.instanceselector.TableReplicaHealth;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelector;
import org.apache.pinot.broker.routing.tablesampler.TableSampler;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.utils.config.TableConfigSerDeUtils;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class BrokerRoutingManagerTest {
  private static final String SERVER_INSTANCE_ID = "Server_localhost_8000";
  private static final String SERVER_HOST = "localhost";
  private static final int SERVER_PORT = 8000;
  private static final String INSTANCE_CONFIGS_PATH = "/CONFIGS/PARTICIPANT";
  private static final String TEST_TABLE = "testTable_OFFLINE";
  private static final List<BrokerGauge> REPLICA_HEALTH_GAUGES =
      List.of(BrokerGauge.PERCENT_OF_REPLICAS, BrokerGauge.UNAVAILABLE_SEGMENTS,
          BrokerGauge.SEGMENTS_AT_MIN_PERCENT_OF_REPLICAS);

  private AutoCloseable _mocks;

  @Mock
  private BrokerMetrics _brokerMetrics;

  @Mock
  private ServerRoutingStatsManager _serverRoutingStatsManager;

  @Mock
  private HelixManager _helixManager;

  @Mock
  private HelixDataAccessor _helixDataAccessor;

  @Mock
  private BaseDataAccessor<ZNRecord> _zkDataAccessor;

  @Mock
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Mock
  private PropertyKey.Builder _keyBuilder;

  @Mock
  private PropertyKey _instanceConfigsKey;

  @Mock
  private Consumer<ServerInstance> _serverReenableCallback;

  private BrokerRoutingManager _routingManager;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);

    // Set up Helix mocks
    when(_helixManager.getHelixDataAccessor()).thenReturn(_helixDataAccessor);
    when(_helixManager.getHelixPropertyStore()).thenReturn(_propertyStore);
    when(_helixDataAccessor.getBaseDataAccessor()).thenReturn(_zkDataAccessor);
    when(_helixDataAccessor.keyBuilder()).thenReturn(_keyBuilder);
    when(_keyBuilder.instanceConfigs()).thenReturn(_instanceConfigsKey);
    when(_keyBuilder.externalViews()).thenReturn(mock(PropertyKey.class));
    when(_keyBuilder.idealStates()).thenReturn(mock(PropertyKey.class));
    when(_instanceConfigsKey.getPath()).thenReturn(INSTANCE_CONFIGS_PATH);

    // Mock paths for external views and ideal states
    PropertyKey evKey = mock(PropertyKey.class);
    PropertyKey isKey = mock(PropertyKey.class);
    when(_keyBuilder.externalViews()).thenReturn(evKey);
    when(_keyBuilder.idealStates()).thenReturn(isKey);
    when(evKey.getPath()).thenReturn("/EXTERNALVIEW");
    when(isKey.getPath()).thenReturn("/IDEALSTATES");

    // Create routing manager
    _routingManager = new BrokerRoutingManager(_brokerMetrics, _serverRoutingStatsManager, new PinotConfiguration());
    _routingManager.init(_helixManager);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testNoErrorWhenCallbackNotSet() {
    // Don't set callback

    // Enable server
    List<ZNRecord> instanceConfigs = List.of(createEnabledServerZNRecord(SERVER_INSTANCE_ID));
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
            anyInt(), anyInt())).thenReturn(instanceConfigs);
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Exclude server
    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    // Disable then re-enable
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
            anyInt(), anyInt())).thenReturn(List.of());
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
            anyInt(), anyInt())).thenReturn(instanceConfigs);

    // Should not throw NPE
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Server should be re-enabled in the map
    assertTrue(_routingManager.getEnabledServerInstanceMap().containsKey(SERVER_INSTANCE_ID));
  }

  @Test
  public void testServerReenableCallbackInvokedWhenExcludedServerReenabled() {
    // Set up callback
    _routingManager.setServerReenableCallback(_serverReenableCallback);

    // First, enable the server by processing instance config change
    List<ZNRecord> instanceConfigs = List.of(createEnabledServerZNRecord(SERVER_INSTANCE_ID));
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(instanceConfigs);

    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Verify server is now in enabled map
    assertTrue(_routingManager.getEnabledServerInstanceMap().containsKey(SERVER_INSTANCE_ID));

    // Exclude the server (simulating failure detector marking it unhealthy)
    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    // Now simulate server being disabled then re-enabled (e.g., restart)
    // First, disable
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(List.of());
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Then re-enable
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(instanceConfigs);
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Verify callback was invoked with correct ServerInstance
    ArgumentCaptor<ServerInstance> captor = ArgumentCaptor.forClass(ServerInstance.class);
    verify(_serverReenableCallback).accept(captor.capture());

    ServerInstance capturedInstance = captor.getValue();
    assertEquals(capturedInstance.getHostname(), SERVER_HOST);
    assertEquals(capturedInstance.getPort(), SERVER_PORT);
  }

  @Test
  public void testServerReenableCallbackNotInvokedForNewServer() {
    // Set up callback
    _routingManager.setServerReenableCallback(_serverReenableCallback);

    // Enable a new server (never excluded)
    List<ZNRecord> instanceConfigs = List.of(createEnabledServerZNRecord(SERVER_INSTANCE_ID));
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(instanceConfigs);

    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Verify callback was NOT invoked (server was never excluded)
    verify(_serverReenableCallback, never()).accept(any());
  }

  @Test
  public void testSamplerContextSharesTimeBoundaryAndPartitionMetadata()
      throws Exception {
    TimeBoundaryManager timeBoundaryManager = mock(TimeBoundaryManager.class);
    SegmentPartitionMetadataManager partitionMetadataManager = mock(SegmentPartitionMetadataManager.class);
    TimeBoundaryInfo expectedTimeBoundaryInfo = new TimeBoundaryInfo("DaysSinceEpoch", "20000");
    TablePartitionInfo expectedPartitionInfo =
        new TablePartitionInfo(TEST_TABLE, "partitionCol", "Modulo", 2,
            List.of(List.of(), List.of()), List.of());
    TablePartitionReplicatedServersInfo expectedReplicatedServersInfo = mock(TablePartitionReplicatedServersInfo.class);
    when(timeBoundaryManager.getTimeBoundaryInfo()).thenReturn(expectedTimeBoundaryInfo);
    when(partitionMetadataManager.getTablePartitionInfo()).thenReturn(expectedPartitionInfo);
    when(partitionMetadataManager.getTablePartitionReplicatedServersInfo()).thenReturn(expectedReplicatedServersInfo);

    Object routingEntry = createRoutingEntry(TEST_TABLE, timeBoundaryManager, partitionMetadataManager, Map.of());
    putRoutingEntry(TEST_TABLE, routingEntry);

    assertSame(_routingManager.getTimeBoundaryInfo(TEST_TABLE), expectedTimeBoundaryInfo);
    assertSame(_routingManager.getTablePartitionInfo(TEST_TABLE), expectedPartitionInfo);
    assertSame(_routingManager.getTablePartitionReplicatedServersInfo(TEST_TABLE), expectedReplicatedServersInfo);
  }

  @Test
  public void testGetPrunedSegmentsIsExactlySelectedMinusSurvivors()
      throws Exception {
    SegmentSelector segmentSelector = selectorOf("seg1", "seg2", "seg3", "seg4");
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, segmentSelector, List.of(prunerDropping("seg1", "seg3")),
            mock(InstanceSelector.class)));

    assertEquals(_routingManager.getPrunedSegments(brokerRequest(TEST_TABLE)), Set.of("seg1", "seg3"));
  }

  @Test
  public void testGetPrunedSegmentsChainsEveryPruner()
      throws Exception {
    SegmentPruner firstPruner = prunerDropping("seg1");
    SegmentPruner secondPruner = prunerDropping("seg3");
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, selectorOf("seg1", "seg2", "seg3"), List.of(firstPruner, secondPruner),
            mock(InstanceSelector.class)));

    assertEquals(_routingManager.getPrunedSegments(brokerRequest(TEST_TABLE)), Set.of("seg1", "seg3"));

    // The second pruner judges what the first left, so consulting only the last one would lose "seg1".
    ArgumentCaptor<Set<String>> captor = ArgumentCaptor.captor();
    verify(secondPruner).prune(any(), captor.capture());
    assertEquals(captor.getValue(), Set.of("seg2", "seg3"));
  }

  /// Nothing pruned must read as "proved nothing", not as "proved every selected segment empty" -- the latter would
  /// let a caller treat a table that fully matches the filter as a table with no matching rows.
  @Test
  public void testGetPrunedSegmentsIsEmptyWhenNothingWasPruned()
      throws Exception {
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, selectorOf("seg1", "seg2"), List.of(prunerDropping()),
            mock(InstanceSelector.class)));

    assertEquals(_routingManager.getPrunedSegments(brokerRequest(TEST_TABLE)), Set.of());
  }

  @Test
  public void testGetPrunedSegmentsIsEmptyWhenSelectionIsEmpty()
      throws Exception {
    SegmentPruner pruner = prunerDropping("seg1");
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, selectorOf(), List.of(pruner), mock(InstanceSelector.class)));

    assertEquals(_routingManager.getPrunedSegments(brokerRequest(TEST_TABLE)), Set.of());
    // An empty selection is answered without asking anyone, so the empty result cannot have come from a pruner.
    verify(pruner, never()).prune(any(), any());
  }

  /// A table this broker has no routing for is `null`, not an empty set: it eliminated nothing because it would have
  /// routed nothing, which is a different claim from "the pruners ran and proved nothing".
  @Test
  public void testGetPrunedSegmentsIsNullForUnknownTable() {
    assertNull(_routingManager.getPrunedSegments(brokerRequest("noSuchTable_OFFLINE")));
  }

  /// The whole point of the API: only presence in the result is a proof. A segment the selector never offered is
  /// absent from the survivors for a reason that has nothing to do with the filter -- here the selector withheld
  /// "seg3" -- and reporting it would let a caller skip a segment that may well hold matching rows.
  @Test
  public void testGetPrunedSegmentsDoesNotReportASegmentTheSelectorNeverOffered()
      throws Exception {
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, selectorOf("seg1", "seg2"), List.of(prunerDropping("seg1", "seg3")),
            mock(InstanceSelector.class)));

    Set<String> prunedSegments = _routingManager.getPrunedSegments(brokerRequest(TEST_TABLE));

    assertEquals(prunedSegments, Set.of("seg1"));
    assertFalse(prunedSegments.contains("seg3"));
  }

  /// Instance selection is what makes routing depend on the request id and on which replicas are up; keeping it out
  /// is what makes this deterministic enough to plan on.
  @Test
  public void testGetPrunedSegmentsNeverConsultsInstanceSelection()
      throws Exception {
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, selectorOf("seg1", "seg2"), List.of(prunerDropping("seg1")), instanceSelector));

    assertEquals(_routingManager.getPrunedSegments(brokerRequest(TEST_TABLE)), Set.of("seg1"));
    verifyNoInteractions(instanceSelector);
  }

  /// A pruner that edits the set it was handed leaves nothing to take a difference against. That has to degrade to
  /// "proved nothing" rather than to a wrong proof.
  @Test
  public void testGetPrunedSegmentsIsEmptyWhenAPrunerEditsInPlace()
      throws Exception {
    SegmentPruner pruner = mock(SegmentPruner.class);
    when(pruner.prune(any(), any())).thenAnswer(invocation -> {
      Set<String> segments = invocation.getArgument(1);
      segments.remove("seg1");
      return segments;
    });
    SegmentSelector segmentSelector = mock(SegmentSelector.class);
    when(segmentSelector.select(any())).thenReturn(new HashSet<>(Set.of("seg1", "seg2")));
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, segmentSelector, List.of(pruner), mock(InstanceSelector.class)));

    assertEquals(_routingManager.getPrunedSegments(brokerRequest(TEST_TABLE)), Set.of());
  }

  private static BrokerRequest brokerRequest(String tableNameWithType) {
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableNameWithType);
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setQuerySource(querySource);
    return brokerRequest;
  }

  private static SegmentSelector selectorOf(String... segments) {
    SegmentSelector segmentSelector = mock(SegmentSelector.class);
    when(segmentSelector.select(any())).thenReturn(Set.of(segments));
    return segmentSelector;
  }

  /// Mirrors [org.apache.pinot.broker.routing.segmentpruner.EmptySegmentPruner]: a fresh set when it prunes
  /// something, the very set it was handed when it does not.
  private static SegmentPruner prunerDropping(String... segments) {
    Set<String> droppedSegments = Set.of(segments);
    SegmentPruner segmentPruner = mock(SegmentPruner.class);
    when(segmentPruner.prune(any(), any())).thenAnswer(invocation -> {
      Set<String> candidateSegments = invocation.getArgument(1);
      if (droppedSegments.stream().noneMatch(candidateSegments::contains)) {
        return candidateSegments;
      }
      Set<String> survivingSegments = new HashSet<>(candidateSegments);
      survivingSegments.removeAll(droppedSegments);
      return survivingSegments;
    });
    return segmentPruner;
  }

  private static Object createRoutingEntry(String tableNameWithType, TimeBoundaryManager timeBoundaryManager,
      SegmentPartitionMetadataManager partitionMetadataManager, Map<String, ?> samplerInfos)
      throws Exception {
    return createRoutingEntry(tableNameWithType, mock(SegmentSelector.class), List.of(),
        mock(InstanceSelector.class), timeBoundaryManager, partitionMetadataManager, samplerInfos, false);
  }

  private static Object createRoutingEntry(String tableNameWithType, TimeBoundaryManager timeBoundaryManager,
      SegmentPartitionMetadataManager partitionMetadataManager, Map<String, ?> samplerInfos,
      InstanceSelector instanceSelector, boolean disabled)
      throws Exception {
    return createRoutingEntry(tableNameWithType, mock(SegmentSelector.class), List.of(), instanceSelector,
        timeBoundaryManager, partitionMetadataManager, samplerInfos, disabled);
  }

  private static Object createRoutingEntry(String tableNameWithType, SegmentSelector segmentSelector,
      List<SegmentPruner> segmentPruners, InstanceSelector instanceSelector)
      throws Exception {
    return createRoutingEntry(tableNameWithType, segmentSelector, segmentPruners, instanceSelector,
        mock(TimeBoundaryManager.class), mock(SegmentPartitionMetadataManager.class), Map.of(), false);
  }

  private static Object createRoutingEntry(String tableNameWithType, SegmentSelector segmentSelector,
      List<SegmentPruner> segmentPruners, InstanceSelector instanceSelector, TimeBoundaryManager timeBoundaryManager,
      SegmentPartitionMetadataManager partitionMetadataManager, Map<String, ?> samplerInfos, boolean disabled)
      throws Exception {
    Class<?> routingEntryClass = Class.forName(BaseBrokerRoutingManager.class.getName() + "$RoutingEntry");
    Constructor<?> constructor = routingEntryClass.getDeclaredConstructor(String.class, String.class, String.class,
        SegmentPreSelector.class, SegmentSelector.class, List.class, InstanceSelector.class, int.class, int.class,
        SegmentZkMetadataFetcher.class, TimeBoundaryManager.class, SegmentPartitionMetadataManager.class, Long.class,
        Map.class, boolean.class);
    constructor.setAccessible(true);
    return constructor.newInstance(tableNameWithType, "/IDEALSTATES/" + tableNameWithType,
        "/EXTERNALVIEW/" + tableNameWithType, mock(SegmentPreSelector.class), segmentSelector, segmentPruners,
        instanceSelector, 1, 1,
        mock(SegmentZkMetadataFetcher.class), timeBoundaryManager, partitionMetadataManager, null, samplerInfos,
        disabled);
  }

  private static Object createSamplerInfo(InstanceSelector instanceSelector)
      throws Exception {
    Class<?> samplerInfoClass = Class.forName(BaseBrokerRoutingManager.class.getName() + "$SamplerInfo");
    Constructor<?> constructor =
        samplerInfoClass.getDeclaredConstructor(TableSampler.class, SegmentSelector.class, InstanceSelector.class);
    constructor.setAccessible(true);
    return constructor.newInstance(mock(TableSampler.class), mock(SegmentSelector.class), instanceSelector);
  }

  /// Registers the test server as routable, so that the exclude/include paths actually walk the routing
  /// entries instead of short-circuiting.
  private void enableTestServer() {
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT), anyInt(), anyInt()))
        .thenReturn(List.of(createEnabledServerZNRecord(SERVER_INSTANCE_ID)));
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);
  }

  private void verifyReplicaHealthGaugesRemoved() {
    for (BrokerGauge gauge : REPLICA_HEALTH_GAUGES) {
      verify(_brokerMetrics).removeTableGauge(TEST_TABLE, gauge);
    }
  }

  private void verifyNoReplicaHealthGaugesReported() {
    for (BrokerGauge gauge : REPLICA_HEALTH_GAUGES) {
      verify(_brokerMetrics, never()).setValueOfTableGauge(eq(TEST_TABLE), eq(gauge), anyLong());
    }
  }

  @Test
  public void testRemoveRoutingRemovesReplicaHealthMetrics()
      throws Exception {
    // The routing owns the table's replica health gauges, so tearing it down has to stop them - otherwise
    // they keep being exported for a table this broker no longer serves
    putRoutingEntry(TEST_TABLE, createRoutingEntry(TEST_TABLE, null, null, Map.of()));

    _routingManager.removeRouting(TEST_TABLE);

    verifyReplicaHealthGaugesRemoved();
  }

  @Test
  public void testInstanceChangeReportsReplicaHealthMetrics()
      throws Exception {
    // The gauges have to track the routing, so an instance change re-reports what the selector now measures
    enableTestServer();
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    when(instanceSelector.getReplicaHealth()).thenReturn(new TableReplicaHealth(33, 2, 1));
    putRoutingEntry(TEST_TABLE, createRoutingEntry(TEST_TABLE, null, null, Map.of(), instanceSelector, false));

    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.PERCENT_OF_REPLICAS, 33);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.SEGMENTS_AT_MIN_PERCENT_OF_REPLICAS, 2);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.UNAVAILABLE_SEGMENTS, 1);
  }

  @Test
  public void testReplicaHealthMetricsDroppedForDisabledTable()
      throws Exception {
    // Disabling a table drives every replica OFFLINE on purpose, so its segments look unavailable. Reporting
    // that would fire the alert for a table nobody expects to be queryable. Drop the gauges instead, so the
    // series disappears rather than reading as an outage.
    enableTestServer();
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    when(instanceSelector.getReplicaHealth()).thenReturn(new TableReplicaHealth(0, 1, 1));
    putRoutingEntry(TEST_TABLE, createRoutingEntry(TEST_TABLE, null, null, Map.of(), instanceSelector, true));

    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    verifyReplicaHealthGaugesRemoved();
    verifyNoReplicaHealthGaugesReported();
  }

  /// Stubs the ZK reads that `processSegmentAssignmentChangeInternal` makes for the test table, so that the
  /// real assignment change path runs against the given ideal state.
  private void stubSegmentAssignmentChange(IdealState idealState) {
    String idealStatePath = "/IDEALSTATES/" + TEST_TABLE;
    String externalViewPath = "/EXTERNALVIEW/" + TEST_TABLE;
    // Any version other than the one the routing entry currently holds, so that the change is not skipped
    Stat changedStat = new Stat();
    changedStat.setVersion(2);
    when(_zkDataAccessor.getStats(eq(List.of(idealStatePath)), eq(AccessOption.PERSISTENT)))
        .thenReturn(new Stat[]{changedStat});
    when(_zkDataAccessor.getStats(eq(List.of(externalViewPath)), eq(AccessOption.PERSISTENT)))
        .thenReturn(new Stat[]{changedStat});
    when(_zkDataAccessor.get(eq(idealStatePath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(idealState.getRecord());
    when(_zkDataAccessor.get(eq(externalViewPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(new ExternalView(TEST_TABLE).getRecord());
  }

  private static IdealState createIdealState(boolean enabled) {
    IdealState idealState = new IdealState(TEST_TABLE);
    idealState.enable(enabled);
    return idealState;
  }

  @Test
  public void testBuildRoutingReportsReplicaHealthGauges()
      throws Exception {
    // Building the routing is the only path by which a newly served table's gauges first appear. Without a
    // report here they would only materialize at the next unrelated cluster change, and a table that was
    // never rebuilt would stay missing from the dashboard entirely.
    enableTestServer();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TableNameBuilder.extractRawTableName(TEST_TABLE))
            .build();
    when(_propertyStore.get(eq("/CONFIGS/TABLE/" + TEST_TABLE), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(TableConfigSerDeUtils.toZNRecord(tableConfig));
    when(_zkDataAccessor.get(eq("/IDEALSTATES/" + TEST_TABLE), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(createIdealState(true).getRecord());
    when(_zkDataAccessor.get(eq("/EXTERNALVIEW/" + TEST_TABLE), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(new ExternalView(TEST_TABLE).getRecord());

    _routingManager.buildRouting(TEST_TABLE);

    assertTrue(_routingManager.routingExists(TEST_TABLE));
    // A table with no segments has nothing to measure, so it reads as fully replicated rather than as an
    // outage - see TableReplicaHealth
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.PERCENT_OF_REPLICAS, 100);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.SEGMENTS_AT_MIN_PERCENT_OF_REPLICAS, 0);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.UNAVAILABLE_SEGMENTS, 0);
  }

  @Test
  public void testAssignmentChangeDropsThenRestoresGaugesAcrossDisableAndEnable()
      throws Exception {
    // Drives the real assignment change path rather than injecting the disabled flag, since that is the only
    // thing that ever flips it. Both directions matter: dropping the gauges of a disabled table is only
    // correct if enabling it brings them back, otherwise the table stays invisible for good.
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    when(instanceSelector.getReplicaHealth()).thenReturn(new TableReplicaHealth(100, 0, 0));
    putRoutingEntry(TEST_TABLE, createRoutingEntry(TEST_TABLE, null, null, Map.of(), instanceSelector, false));

    // Disabling a table drives every replica OFFLINE on purpose, so reporting it would be a false alarm
    stubSegmentAssignmentChange(createIdealState(false));
    _routingManager.processSegmentAssignmentChangeInternal();

    verifyReplicaHealthGaugesRemoved();
    verifyNoReplicaHealthGaugesReported();

    clearInvocations(_brokerMetrics);
    stubSegmentAssignmentChange(createIdealState(true));
    _routingManager.processSegmentAssignmentChangeInternal();

    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.PERCENT_OF_REPLICAS, 100);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.SEGMENTS_AT_MIN_PERCENT_OF_REPLICAS, 0);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.UNAVAILABLE_SEGMENTS, 0);
  }

  @Test
  public void testSamplerInstanceSelectorNeverReportsReplicaHealth()
      throws Exception {
    // A sampler's selector sees only a sampled subset of the table's segments and shares the table name with
    // the selector covering the whole table, so reporting from it would overwrite the table's real values.
    // Only the routing entry's own selector is ever asked.
    enableTestServer();
    InstanceSelector samplerInstanceSelector = mock(InstanceSelector.class);
    when(samplerInstanceSelector.getReplicaHealth()).thenReturn(new TableReplicaHealth(0, 5, 5));
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    when(instanceSelector.getReplicaHealth()).thenReturn(new TableReplicaHealth(100, 0, 0));
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, null, null, Map.of("sampler", createSamplerInfo(samplerInstanceSelector)),
            instanceSelector, false));

    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    // The sampler's selector is driven by the change like any other...
    verify(samplerInstanceSelector).onInstancesChange(any(), any());
    // ...but is never a source of the table's gauges
    verify(samplerInstanceSelector, never()).getReplicaHealth();
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.PERCENT_OF_REPLICAS, 100);
  }

  @Test
  public void testSelectorThatDoesNotMeasureReplicaHealthDropsTheGauges()
      throws Exception {
    // A custom instance selector that does not extend BaseInstanceSelector returns null replica health. Its
    // gauges are dropped rather than left at whatever a previous selector reported, so swapping a table to
    // such a selector ends the series instead of freezing it.
    enableTestServer();
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    when(instanceSelector.getReplicaHealth()).thenReturn(null);
    putRoutingEntry(TEST_TABLE, createRoutingEntry(TEST_TABLE, null, null, Map.of(), instanceSelector, false));

    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    // Asserted positively first, so the never() checks below cannot pass just because the reporting path
    // was never reached at all
    verify(instanceSelector).getReplicaHealth();
    verifyReplicaHealthGaugesRemoved();
    verifyNoReplicaHealthGaugesReported();
  }

  @Test
  public void testReplicaHealthStillReportedWhenTheAssignmentChangeThrows()
      throws Exception {
    // RoutingEntry.onAssignmentChange updates the instance selector before the time boundary manager, so by
    // the time this throws the replica health is already fresh but unreported. That is the window the
    // finally block exists for. Real callers reach it through MultiStageReplicaGroupSelector, whose own
    // onAssignmentChange recomputes the health via super and then throws from getInstancePartitions() when
    // the instance partitions ZNode is missing.
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    when(instanceSelector.getReplicaHealth()).thenReturn(new TableReplicaHealth(0, 4, 4));
    TimeBoundaryManager timeBoundaryManager = mock(TimeBoundaryManager.class);
    doThrow(new RuntimeException("simulated time boundary manager failure")).when(timeBoundaryManager)
        .onAssignmentChange(any(), any(), any());
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, timeBoundaryManager, null, Map.of(), instanceSelector, false));
    stubSegmentAssignmentChange(createIdealState(true));

    _routingManager.processSegmentAssignmentChangeInternal();

    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.PERCENT_OF_REPLICAS, 0);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.SEGMENTS_AT_MIN_PERCENT_OF_REPLICAS, 4);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.UNAVAILABLE_SEGMENTS, 4);
  }

  @Test
  public void testReplicaHealthStillReportedWhenTheInstanceChangeThrows()
      throws Exception {
    // Same window on the instance change path: RoutingEntry.onInstancesChange updates the table's own
    // selector before fanning out to the samplers', and unlike updateSamplerInfos it does not catch per
    // sampler, so a failing sampler selector aborts the update after the health is already fresh.
    enableTestServer();
    InstanceSelector instanceSelector = mock(InstanceSelector.class);
    when(instanceSelector.getReplicaHealth()).thenReturn(new TableReplicaHealth(0, 3, 3));
    InstanceSelector samplerInstanceSelector = mock(InstanceSelector.class);
    doThrow(new RuntimeException("simulated instance selector failure")).when(samplerInstanceSelector)
        .onInstancesChange(any(), any());
    putRoutingEntry(TEST_TABLE,
        createRoutingEntry(TEST_TABLE, null, null, Map.of("sampler", createSamplerInfo(samplerInstanceSelector)),
            instanceSelector, false));

    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.PERCENT_OF_REPLICAS, 0);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.SEGMENTS_AT_MIN_PERCENT_OF_REPLICAS, 3);
    verify(_brokerMetrics).setValueOfTableGauge(TEST_TABLE, BrokerGauge.UNAVAILABLE_SEGMENTS, 3);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void putRoutingEntry(String tableNameWithType, Object routingEntry) {
    Map routingEntries = _routingManager._routingEntryMap;
    routingEntries.put(tableNameWithType, routingEntry);
  }

  @Test
  public void testRoutableServerInstanceMapReflectsExclusion() {
    // Enable server
    List<ZNRecord> instanceConfigs = List.of(createEnabledServerZNRecord(SERVER_INSTANCE_ID));
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT), anyInt(), anyInt()))
        .thenReturn(instanceConfigs);
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Server is present in both the enabled and routable maps initially.
    assertTrue(_routingManager.getEnabledServerInstanceMap().containsKey(SERVER_INSTANCE_ID));
    assertTrue(_routingManager.getRoutableServerInstanceMap().containsKey(SERVER_INSTANCE_ID));

    // Exclude the server (simulating FailureDetector marking it unhealthy).
    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    // Routable map no longer contains the server, but enabled map still does.
    // This is the contract MSE WorkerManager relies on for intermediate-stage worker selection.
    assertTrue(_routingManager.getEnabledServerInstanceMap().containsKey(SERVER_INSTANCE_ID));
    assertFalse(_routingManager.getRoutableServerInstanceMap().containsKey(SERVER_INSTANCE_ID));

    // Re-include the server.
    _routingManager.includeServerToRouting(SERVER_INSTANCE_ID);

    // Routable map contains the server again.
    assertTrue(_routingManager.getEnabledServerInstanceMap().containsKey(SERVER_INSTANCE_ID));
    assertTrue(_routingManager.getRoutableServerInstanceMap().containsKey(SERVER_INSTANCE_ID));
  }

  /// Creates a ZNRecord representing an enabled server instance.
  private ZNRecord createEnabledServerZNRecord(String instanceId) {
    ZNRecord record = new ZNRecord(instanceId);
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), "true");
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_HOST.name(),
        instanceId.split("_")[1]); // Extract host from Server_host_port
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.name(),
        instanceId.split("_")[2]); // Extract port from Server_host_port
    // Don't set IS_SHUTDOWN_IN_PROGRESS or QUERIES_DISABLED (they default to false)
    return record;
  }
}
