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
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelector;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
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
    return createRoutingEntry(tableNameWithType, mock(SegmentSelector.class), List.of(), mock(InstanceSelector.class),
        timeBoundaryManager, partitionMetadataManager, samplerInfos);
  }

  private static Object createRoutingEntry(String tableNameWithType, SegmentSelector segmentSelector,
      List<SegmentPruner> segmentPruners, InstanceSelector instanceSelector)
      throws Exception {
    return createRoutingEntry(tableNameWithType, segmentSelector, segmentPruners, instanceSelector,
        mock(TimeBoundaryManager.class), mock(SegmentPartitionMetadataManager.class), Map.of());
  }

  private static Object createRoutingEntry(String tableNameWithType, SegmentSelector segmentSelector,
      List<SegmentPruner> segmentPruners, InstanceSelector instanceSelector, TimeBoundaryManager timeBoundaryManager,
      SegmentPartitionMetadataManager partitionMetadataManager, Map<String, ?> samplerInfos)
      throws Exception {
    Class<?> routingEntryClass = Class.forName(BaseBrokerRoutingManager.class.getName() + "$RoutingEntry");
    Constructor<?> constructor = routingEntryClass.getDeclaredConstructor(String.class, String.class, String.class,
        SegmentPreSelector.class, SegmentSelector.class, List.class, InstanceSelector.class, int.class, int.class,
        SegmentZkMetadataFetcher.class, TimeBoundaryManager.class, SegmentPartitionMetadataManager.class, Long.class,
        Map.class, boolean.class);
    constructor.setAccessible(true);
    return constructor.newInstance(tableNameWithType, "/IDEALSTATES/" + tableNameWithType,
        "/EXTERNALVIEW/" + tableNameWithType, mock(SegmentPreSelector.class), segmentSelector, segmentPruners,
        instanceSelector, 1, 1, mock(SegmentZkMetadataFetcher.class), timeBoundaryManager, partitionMetadataManager,
        null, samplerInfos, false);
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
