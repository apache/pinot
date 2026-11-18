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
package org.apache.pinot.controller.helix.core.realtime;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeFailoverManager.PendingFailure;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.BlockedReason;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.FailureReason;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.PartitionRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.RecoveryAction;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies failure observation, recovery planning and cleanup without performing consumption reassignment.
public class ReplicateModeFailoverManagerTest {
  private static final String CLUSTER = "testCluster";
  private static final String TABLE = "testTable_REALTIME";
  private static final String SERVER_A = "Server_a_1234";
  private static final String SERVER_B = "Server_b_1234";
  private static final String SERVER_C = "Server_c_1234";

  private PinotHelixResourceManager _resourceManager;
  private LeadControllerManager _leadControllerManager;
  private PinotLLCRealtimeSegmentManager _llcManager;
  private SegmentCompletionManager _completionManager;
  private TableConfig _tableConfig;
  private InstancePartitions _instancePartitions;
  private Map<String, InstanceConfig> _instanceConfigs;
  private Clock _clock;
  private IdealState _idealState;
  private ExternalView _externalView;
  private ReplicateModeFailoverManager _manager;
  private ReplicateModeRecoveryExecutor _executor;
  private AtomicReference<Consumer<String>> _consumptionStoppedListener;

  @BeforeMethod
  public void setUp() {
    _resourceManager = mock(PinotHelixResourceManager.class);
    _leadControllerManager = mock(LeadControllerManager.class);
    _llcManager = mock(PinotLLCRealtimeSegmentManager.class);
    _completionManager = mock(SegmentCompletionManager.class);
    _tableConfig = tableConfig("testTable");
    _instanceConfigs = new HashMap<>();
    for (String server : List.of(SERVER_A, SERVER_B, SERVER_C)) {
      InstanceConfig instanceConfig = new InstanceConfig(server);
      instanceConfig.addTag("DefaultTenant_REALTIME");
      _instanceConfigs.put(server, instanceConfig);
    }
    _instancePartitions = new InstancePartitions("testTable_CONSUMING");
    _instancePartitions.setInstances(0, 0, List.of(SERVER_A, SERVER_B, SERVER_C));
    _clock = mock(Clock.class);
    _idealState = new IdealState(TABLE);
    _idealState.enable(true);
    _externalView = new ExternalView(TABLE);
    _consumptionStoppedListener = new AtomicReference<>();
    when(_resourceManager.getHelixClusterName()).thenReturn(CLUSTER);
    when(_resourceManager.getAllTables()).thenReturn(List.of(TABLE));
    when(_resourceManager.getTableIdealState(TABLE)).thenReturn(_idealState);
    when(_resourceManager.getTableExternalView(TABLE)).thenReturn(_externalView);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_A, SERVER_B, SERVER_C));
    when(_resourceManager.getAllHelixInstanceConfigs()).thenAnswer(
        invocation -> List.copyOf(_instanceConfigs.values()));
    when(_resourceManager.getTableConfig(TABLE)).thenReturn(_tableConfig);
    when(_leadControllerManager.isLeaderForTable(TABLE)).thenReturn(true);
    when(_llcManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of());
    when(_llcManager.getConsumingInstancePartitions(_tableConfig)).thenReturn(_instancePartitions);
    when(_completionManager.getRecoveryStatus(anyString())).thenReturn(RecoveryStatus.NO_ACTIVE_COMMIT);
    when(_clock.millis()).thenReturn(1000L);
    when(_llcManager.getSegmentZKMetadataIfExists(anyString(), anyString(), any(Stat.class))).thenAnswer(invocation -> {
      Stat stat = invocation.getArgument(2);
      stat.setMtime(1L);
      return _resourceManager.getSegmentZKMetadata(invocation.getArgument(0), invocation.getArgument(1));
    });
    doAnswer(invocation -> {
      _consumptionStoppedListener.set(invocation.getArgument(0));
      return null;
    }).when(_llcManager).setConsumptionStoppedListener(any());
    _executor = mock(ReplicateModeRecoveryExecutor.class);
    _manager = new ReplicateModeFailoverManager(new ControllerConf(), _resourceManager, _leadControllerManager,
        mock(ControllerMetrics.class), _llcManager, _completionManager, _clock, _executor);
    _manager.start();
  }

  @AfterMethod
  public void tearDown() {
    _manager.stop();
  }

  @Test
  public void testRecordsFailureWithoutChangingAssignmentAndReturnsImmutableSnapshot() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    IdealState original = HelixHelper.cloneIdealState(_idealState);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B, SERVER_C));

    _manager.run();

    PendingFailure failure = onlyFailure();
    assertEquals(failure.partitionId(), 0);
    assertEquals(failure.segmentName(), segment);
    assertEquals(failure.serverName(), SERVER_A);
    assertEquals(failure.status(), Status.IN_PROGRESS);
    assertEquals(failure.reason(), FailureReason.INSTANCE_NOT_LIVE);
    assertEquals(failure.firstObservedTimeMs(), 1000L);
    assertEquals(failure.lastObservedTimeMs(), 1000L);
    assertEquals(_idealState.getInstanceStateMap(segment), Map.of(SERVER_A, SegmentStateModel.CONSUMING));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_idealState, 0), SERVER_A);
    assertEquals(_idealState, original);
    assertThrows(UnsupportedOperationException.class, () -> _manager.getPendingFailures(TABLE).clear());
    assertEquals(onlyPlan().segments().get(segment).action(), RecoveryAction.REASSIGN_CONSUMING);
    assertEquals(onlyPlan().segments().get(segment).targetServer(), SERVER_B);
    assertThrows(UnsupportedOperationException.class, () -> _manager.getRecoveryPlans(TABLE).clear());
    verify(_executor).execute(eq(TABLE), eq(_manager.getRecoveryPlans(TABLE)), any());
    verify(_resourceManager).getTableConfig(TABLE);
    verify(_completionManager).getRecoveryStatus(segment);
    verifyNoMoreInteractions(_completionManager);
    verify(_llcManager, never()).ensureAllPartitionsConsuming(any(), any(), any());
    verify(_llcManager, never()).repairCompletedReplicateModeSegments(anyString());
    verify(_llcManager, never()).repairSegmentsInErrorState(any(), anyBoolean());
  }

  @DataProvider(name = "explicitFailures")
  public Object[][] explicitFailures() {
    return new Object[][]{
        {SegmentStateModel.OFFLINE, SegmentStateModel.CONSUMING, FailureReason.LEADER_OFFLINE},
        {SegmentStateModel.CONSUMING, SegmentStateModel.ERROR, FailureReason.STATE_TRANSITION_ERROR}
    };
  }

  @Test(dataProvider = "explicitFailures")
  public void testObservesExplicitSegmentFailure(String desiredState, String actualState, FailureReason reason) {
    String segment = addSegment(0, 0, SERVER_A, desiredState, Status.IN_PROGRESS, true);
    _externalView.setState(segment, SERVER_A, actualState);

    _manager.run();

    assertEquals(onlyFailure().reason(), reason);
  }

  @DataProvider(name = "transientExternalViewStates")
  public Object[][] transientExternalViewStates() {
    return new Object[][]{{null}, {SegmentStateModel.OFFLINE}, {SegmentStateModel.CONSUMING}};
  }

  @Test(dataProvider = "transientExternalViewStates")
  public void testDoesNotTreatMissingOrOfflineExternalViewAsFailure(String actualState) {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    if (actualState != null) {
      _externalView.setState(segment, SERVER_A, actualState);
    }

    _manager.run();

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    verify(_resourceManager, never()).getSegmentZKMetadata(anyString(), anyString());
    verify(_resourceManager, never()).getAllHelixInstanceConfigs();
  }

  @Test
  public void testSegmentSnapshotDeterminesWhetherModeApplies() {
    addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, false);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));

    _manager.run();

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertTrue(_manager.getRecoveryPlans(TABLE).isEmpty());
    verify(_resourceManager, never()).getTableConfig(anyString());
    verify(_resourceManager, never()).getAllHelixInstanceConfigs();
  }

  @Test
  public void testPreservesFirstObservationAndResetsItWhenReasonChanges() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();

    when(_clock.millis()).thenReturn(2000L);
    _manager.run();
    assertEquals(onlyFailure().firstObservedTimeMs(), 1000L);
    assertEquals(onlyFailure().lastObservedTimeMs(), 2000L);

    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_A, SERVER_B));
    _idealState.setInstanceStateMap(segment, Map.of(SERVER_A, SegmentStateModel.OFFLINE));
    when(_clock.millis()).thenReturn(3000L);
    _manager.run();
    assertEquals(onlyFailure().reason(), FailureReason.LEADER_OFFLINE);
    assertEquals(onlyFailure().firstObservedTimeMs(), 3000L);
  }

  @Test
  public void testRetainsPreviousObservationWhenMetadataReadFails() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    PendingFailure failure = onlyFailure();
    PartitionRecoveryPlan plan = onlyPlan();

    when(_resourceManager.getSegmentZKMetadata(TABLE, segment)).thenThrow(new IllegalStateException("unavailable"));
    when(_clock.millis()).thenReturn(2000L);
    _manager.run();

    assertEquals(onlyFailure(), failure);
    assertEquals(onlyPlan(), plan);
  }

  @Test
  public void testClearsObservationWhenHostRecoversOrSegmentCompletes() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    assertEquals(_manager.getPendingFailures(TABLE).size(), 1);

    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_A, SERVER_B));
    _externalView.setState(segment, SERVER_A, SegmentStateModel.CONSUMING);
    _manager.run();
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());

    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    assertEquals(_manager.getPendingFailures(TABLE).size(), 1);
    when(_resourceManager.getSegmentZKMetadata(TABLE, segment)).thenReturn(metadata(segment, Status.DONE, true));
    _manager.run();
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
  }

  @Test
  public void testClearsObservationAfterReplacementAndLeadershipLoss() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    assertEquals(_manager.getPendingFailures(TABLE).size(), 1);

    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_B, List.of(SERVER_A, SERVER_B, SERVER_C));
    _idealState.getRecord().setMapField(segment, Map.of(SERVER_B, SegmentStateModel.CONSUMING));
    _externalView.setState(segment, SERVER_B, SegmentStateModel.CONSUMING);
    _manager.run();
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());

    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_C));
    _manager.run();
    assertEquals(onlyFailure().serverName(), SERVER_B);
    when(_leadControllerManager.isLeaderForTable(TABLE)).thenReturn(false);
    _manager.run();
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
  }

  @Test
  public void testClearsObservationWhenTableIsDeleted() {
    addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    assertEquals(_manager.getPendingFailures(TABLE).size(), 1);

    when(_resourceManager.getAllTables()).thenReturn(List.of());
    when(_leadControllerManager.isLeaderForTable(TABLE)).thenReturn(false);
    _manager.run();

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
  }

  @Test
  public void testObservesCommittingHostEvenWhenNextSegmentHasDifferentLeader() {
    String committing = addSegment(0, 0, SERVER_A, SegmentStateModel.ONLINE, Status.COMMITTING, true);
    addSegment(0, 1, SERVER_B, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_llcManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of(committing));
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B, SERVER_C));

    _manager.run();

    assertEquals(onlyFailure().segmentName(), committing);
    assertEquals(onlyFailure().serverName(), SERVER_A);
    assertEquals(onlyFailure().status(), Status.COMMITTING);
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_idealState, 0), SERVER_B);
  }

  @Test
  public void testIgnoresAbandonedOfflineSegmentWhenNewerSegmentExists() {
    String abandoned = addSegment(0, 0, SERVER_A, SegmentStateModel.OFFLINE, Status.IN_PROGRESS, true);
    addSegment(0, 1, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));

    _manager.run();

    assertEquals(onlyFailure().segmentName(), segmentName(0, 1));
    verify(_resourceManager, never()).getSegmentZKMetadata(TABLE, abandoned);
  }

  @Test
  public void testDoesNotScanHistoricalSegmentMetadataAndReadsLivenessOnce() {
    addSegment(0, 10, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    addSegment(1, 10, SERVER_B, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    for (int partition = 0; partition < 2; partition++) {
      for (int sequence = 0; sequence < 10; sequence++) {
        _idealState.getRecord().setMapField(segmentName(partition, sequence),
            Map.of(SERVER_A, SegmentStateModel.ONLINE, SERVER_B, SegmentStateModel.ONLINE,
                SERVER_C, SegmentStateModel.ONLINE));
      }
    }
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_C));

    _manager.run();

    assertEquals(_manager.getPendingFailures(TABLE).size(), 2);
    verify(_resourceManager, times(1)).getAllLiveInstances();
    verify(_resourceManager, times(2)).getSegmentZKMetadata(anyString(), anyString());
    verify(_resourceManager).getAllHelixInstanceConfigs();
  }

  @Test
  public void testTablesWithoutRolesDoNotFetchLivenessOrCommittingSegments() {
    _idealState.setInstanceStateMap(segmentName(0, 0), Map.of(SERVER_A, SegmentStateModel.CONSUMING));

    _manager.run();

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    verify(_resourceManager, never()).getAllLiveInstances();
    verify(_resourceManager, never()).getAllHelixInstanceConfigs();
    verify(_resourceManager, never()).getSegmentZKMetadata(anyString(), anyString());
    verify(_llcManager, never()).getPendingSegmentCompletions(anyString());
  }

  @Test
  public void testNegativeRoleCacheSkipsUnchangedAssignmentAndDetectsAddedRolesOnVersionChange() {
    Stat stat = new Stat();
    stat.setVersion(7);
    stat.setCzxid(50L);
    when(_resourceManager.getZKStat(PropertyPathBuilder.idealState(CLUSTER, TABLE))).thenReturn(stat);
    _manager.run();
    _manager.run();
    verify(_resourceManager, times(1)).getTableIdealState(TABLE);
    verify(_resourceManager, never()).getAllLiveInstances();

    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    stat.setVersion(8);
    _manager.run();

    assertEquals(onlyFailure().segmentName(), segment);
    verify(_resourceManager, times(2)).getTableIdealState(TABLE);
    verify(_resourceManager).getTableConfig(TABLE);
  }

  @Test
  public void testNegativeRoleCacheInvalidatesWhenIdealStateIsRecreatedAtSameVersion() {
    Stat stat = new Stat();
    stat.setVersion(0);
    stat.setCzxid(50L);
    when(_resourceManager.getZKStat(PropertyPathBuilder.idealState(CLUSTER, TABLE))).thenReturn(stat);
    _manager.run();
    _manager.run();
    verify(_resourceManager, times(1)).getTableIdealState(TABLE);

    addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    stat.setCzxid(51L);
    _manager.run();

    assertEquals(onlyFailure().reason(), FailureReason.INSTANCE_NOT_LIVE);
    verify(_resourceManager, times(2)).getTableIdealState(TABLE);
  }

  @Test
  public void testDirtyHintBypassesNegativeRoleCacheEvenWhenStatIsUnchanged() {
    Stat stat = new Stat();
    stat.setVersion(7);
    stat.setCzxid(50L);
    when(_resourceManager.getZKStat(PropertyPathBuilder.idealState(CLUSTER, TABLE))).thenReturn(stat);
    _manager.run();
    addSegment(0, 0, SERVER_A, SegmentStateModel.OFFLINE, Status.IN_PROGRESS, true);

    _manager.requestReconciliation(TABLE);
    _manager.run();

    assertEquals(onlyFailure().reason(), FailureReason.LEADER_OFFLINE);
    verify(_resourceManager, times(2)).getTableIdealState(TABLE);
  }

  @DataProvider(name = "intentionalStops")
  public Object[][] intentionalStops() {
    return new Object[][]{{"disabled"}, {"paused"}, {"topicPaused"}};
  }

  @Test(dataProvider = "intentionalStops")
  public void testDoesNotObserveFailuresDuringIntentionalStops(String stop)
      throws Exception {
    addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    switch (stop) {
      case "disabled" -> _idealState.enable(false);
      case "paused" -> {
        // Exercise backward compatibility with the persisted legacy table-pause field.
        _idealState.getRecord().setSimpleField("isTablePaused", "true");
      }
      case "topicPaused" -> {
        PauseState pauseState = new PauseState(false, PauseState.ReasonCode.ADMINISTRATIVE, null, null, List.of(0));
        _idealState.getRecord().setSimpleField(PinotLLCRealtimeSegmentManager.PAUSE_STATE, pauseState.toJsonString());
      }
      default -> throw new IllegalArgumentException(stop);
    }

    _manager.run();

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    verify(_resourceManager, never()).getSegmentZKMetadata(anyString(), anyString());
  }

  @Test
  public void testConsumptionStoppedSignalOnlyRequestsNextReconciliation() {
    addSegment(0, 0, SERVER_A, SegmentStateModel.OFFLINE, Status.IN_PROGRESS, true);
    Consumer<String> listener = _consumptionStoppedListener.get();
    assertNotNull(listener);

    listener.accept(TABLE);

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    verify(_resourceManager, never()).getTableIdealState(anyString());
    _manager.run();
    assertEquals(onlyFailure().reason(), FailureReason.LEADER_OFFLINE);

    _manager.stop();
    assertNull(_consumptionStoppedListener.get());
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertTrue(_manager.getRecoveryPlans(TABLE).isEmpty());
  }

  @Test
  public void testDirtyTablesArePrioritizedWithoutDiscardingOtherTables() {
    String otherTable = "otherTable_REALTIME";
    when(_resourceManager.getAllTables()).thenReturn(List.of(otherTable, TABLE));

    _manager.requestReconciliation(TABLE);

    assertEquals(_manager.getTablesToProcess(new Properties()), List.of(TABLE, otherTable));
  }

  @Test
  public void testDirtyHintIsDiscardedWhenNoTableIsLedByController() {
    _manager.requestReconciliation(TABLE);
    when(_resourceManager.getAllTables()).thenReturn(List.of());
    when(_leadControllerManager.isLeaderForTable(TABLE)).thenReturn(false);

    _manager.run();

    assertTrue(_manager.getTablesToProcess(new Properties()).isEmpty());
    verify(_resourceManager, never()).getTableIdealState(anyString());
  }

  @Test
  public void testTableScopedRunDoesNotClearObservationsForOtherTablesStillLedByController() {
    String otherTable = "otherTable_REALTIME";
    String otherSegment = new LLCSegmentName("otherTable", 0, 0, 1000L).getSegmentName();
    IdealState otherIdealState = new IdealState(otherTable);
    otherIdealState.enable(true);
    ReplicateModeAssignmentUtils.setAssignment(otherIdealState, 0, SERVER_A, List.of(SERVER_A, SERVER_B, SERVER_C));
    otherIdealState.setInstanceStateMap(otherSegment, Map.of(SERVER_A, SegmentStateModel.CONSUMING));
    when(_resourceManager.getAllTables()).thenReturn(List.of(TABLE, otherTable));
    when(_leadControllerManager.isLeaderForTable(otherTable)).thenReturn(true);
    when(_resourceManager.getTableIdealState(otherTable)).thenReturn(otherIdealState);
    TableConfig otherConfig = tableConfig("otherTable");
    when(_resourceManager.getTableConfig(otherTable)).thenReturn(otherConfig);
    when(_llcManager.getConsumingInstancePartitions(otherConfig)).thenReturn(_instancePartitions);
    when(_resourceManager.getSegmentZKMetadata(otherTable, otherSegment))
        .thenReturn(metadata(otherSegment, Status.IN_PROGRESS, true));
    when(_llcManager.getPendingSegmentCompletions(otherTable)).thenReturn(List.of());
    addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    assertEquals(_manager.getPendingFailures(otherTable).size(), 1);
    assertEquals(_manager.getRecoveryPlans(otherTable).size(), 1);
    verify(_resourceManager).getAllHelixInstanceConfigs();

    Properties properties = new Properties();
    properties.setProperty(PeriodicTask.PROPERTY_KEY_TABLE_NAME, TABLE);
    _manager.run(properties);

    assertEquals(_manager.getPendingFailures(TABLE).size(), 1);
    assertEquals(_manager.getPendingFailures(otherTable).size(), 1);
    assertEquals(_manager.getRecoveryPlans(TABLE).size(), 1);
    assertEquals(_manager.getRecoveryPlans(otherTable).size(), 1);
    verify(_resourceManager, times(2)).getAllHelixInstanceConfigs();
  }

  @Test
  public void testRecomputesPlansFromCandidateChangesWithoutAnIdealStateChange() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    IdealState original = HelixHelper.cloneIdealState(_idealState);
    Stat stat = new Stat();
    stat.setVersion(7);
    when(_resourceManager.getZKStat(PropertyPathBuilder.idealState(CLUSTER, TABLE))).thenReturn(stat);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B, SERVER_C));
    _manager.run();
    assertEquals(onlyPlan().segments().get(segment).targetServer(), SERVER_B);

    _instanceConfigs.get(SERVER_B).setInstanceOperation(InstanceOperation.DISABLE);
    when(_clock.millis()).thenReturn(2000L);
    _manager.run();
    assertEquals(onlyPlan().segments().get(segment).targetServer(), SERVER_C);
    assertEquals(onlyPlan().observedTimeMs(), 2000L);

    _instanceConfigs.get(SERVER_B).setInstanceOperation(InstanceOperation.ENABLE);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    assertEquals(onlyPlan().segments().get(segment).targetServer(), SERVER_B);

    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of());
    _manager.run();
    SegmentRecoveryPlan blocked = onlyPlan().segments().get(segment);
    assertEquals(blocked.action(), RecoveryAction.BLOCKED);
    assertEquals(blocked.blockedReason(), BlockedReason.NO_ELIGIBLE_REPLICA);
    assertNull(blocked.targetServer());
    assertEquals(_idealState, original);
    verify(_resourceManager, times(4)).getAllHelixInstanceConfigs();
    verify(_resourceManager, times(4)).getAllLiveInstances();
  }

  @Test
  public void testRetainsAtomicObservationWhenCandidateConfigurationReadFails() {
    addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    PendingFailure failure = onlyFailure();
    PartitionRecoveryPlan plan = onlyPlan();

    when(_clock.millis()).thenReturn(2000L);
    when(_resourceManager.getAllHelixInstanceConfigs()).thenThrow(new IllegalStateException("unavailable"));
    _manager.run();

    assertEquals(onlyFailure(), failure);
    assertEquals(onlyPlan(), plan);
  }

  @Test
  public void testCompletionObservationUsesMetadataStatWithoutAnotherRead() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.ONLINE, Status.COMMITTING, true);
    when(_llcManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of(segment));

    _manager.run();

    verify(_llcManager).getSegmentZKMetadataIfExists(eq(TABLE), eq(segment), any(Stat.class));
    verify(_llcManager).isExceededMaxSegmentCompletionTime(eq(segment),
        argThat((Stat stat) -> stat.getMtime() == 1L), eq(1000L));
    verify(_llcManager, never()).isExceededMaxSegmentCompletionTime(anyString(), anyString(), anyLong());
    verify(_resourceManager).getSegmentZKMetadata(TABLE, segment);
  }

  @Test
  public void testKnownCommittingPlanWaitsForNativeTimeoutAndUnknownCommitThenReusesHealthyLeader()
      throws Exception {
    String committing = addSegment(0, 0, SERVER_A, SegmentStateModel.ONLINE, Status.COMMITTING, true);
    addSegment(0, 1, SERVER_B, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    IdealState original = HelixHelper.cloneIdealState(_idealState);
    when(_llcManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of(committing));
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B, SERVER_C));
    when(_completionManager.getRecoveryStatus(committing)).thenReturn(RecoveryStatus.COMMIT_IN_PROGRESS);
    _manager.run();
    assertEquals(onlyPlan().segments().get(committing).action(), RecoveryAction.WAIT_FOR_COMPLETION);

    // Observation age alone must not replace the native segment-completion timeout.
    when(_clock.millis()).thenReturn(10_000_000L);
    when(_completionManager.getRecoveryStatus(committing)).thenReturn(RecoveryStatus.NO_ACTIVE_COMMIT);
    _manager.run();
    assertEquals(onlyPlan().segments().get(committing).action(), RecoveryAction.WAIT_FOR_COMPLETION);

    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_A, SERVER_B, SERVER_C));
    _externalView.setState(committing, SERVER_A, SegmentStateModel.ONLINE);
    when(_llcManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of());
    when(_llcManager.isExceededMaxSegmentCompletionTime(eq(committing), any(Stat.class), anyLong())).thenReturn(true);
    when(_completionManager.getRecoveryStatus(committing)).thenReturn(RecoveryStatus.UNKNOWN);
    _manager.run();
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertEquals(onlyPlan().segments().get(committing).action(), RecoveryAction.WAIT_FOR_COMPLETION);

    when(_completionManager.getRecoveryStatus(committing)).thenReturn(RecoveryStatus.NO_ACTIVE_COMMIT);
    _manager.run();
    SegmentRecoveryPlan reingestion = onlyPlan().segments().get(committing);
    assertEquals(reingestion.action(), RecoveryAction.REINGEST_COMMITTING);
    assertEquals(reingestion.targetServer(), SERVER_B);
    assertEquals(reingestion.startOffset(), "0");
    assertEquals(reingestion.endOffset(), "10");
    assertEquals(_idealState, original);
    verify(_llcManager, never()).triggerReingestion(anyString(), anyString());
    verify(_llcManager, never()).uploadToDeepStoreIfMissing(any(), any());
    verify(_llcManager, never()).uploadToDeepStoreIfMissing(any(), any(), anyBoolean());
  }

  @Test
  public void testListedCommittingSegmentIsPlannedOnFirstRunWithoutAHostFailure()
      throws Exception {
    String committing = addSegment(0, 0, SERVER_A, SegmentStateModel.ONLINE, Status.COMMITTING, true);
    addSegment(0, 1, SERVER_B, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    IdealState original = HelixHelper.cloneIdealState(_idealState);
    when(_llcManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of(committing));

    _manager.run();

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertEquals(onlyPlan().segments().get(committing).action(), RecoveryAction.WAIT_FOR_COMPLETION);
    assertNull(onlyPlan().segments().get(committing).targetServer());

    when(_llcManager.isExceededMaxSegmentCompletionTime(eq(committing), any(Stat.class), anyLong())).thenReturn(true);
    _manager.run();

    SegmentRecoveryPlan reingestion = onlyPlan().segments().get(committing);
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertEquals(reingestion.action(), RecoveryAction.REINGEST_COMMITTING);
    assertEquals(reingestion.targetServer(), SERVER_B);
    assertEquals(reingestion.startOffset(), "0");
    assertEquals(reingestion.endOffset(), "10");
    assertEquals(_idealState, original);
    verify(_completionManager, times(2)).getRecoveryStatus(committing);
    verifyNoMoreInteractions(_completionManager);
    verify(_llcManager, never()).triggerReingestion(anyString(), anyString());
    verify(_llcManager, never()).repairCompletedReplicateModeSegments(anyString());
    verify(_llcManager, never()).ensureAllPartitionsConsuming(any(), any(), any());
  }

  @Test
  public void testKnownCompletionWindowFollowsDoneUntilAssignmentIsRepaired() {
    String committing = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.COMMITTING, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B, SERVER_C));
    _manager.run();
    assertEquals(onlyPlan().segments().get(committing).action(), RecoveryAction.WAIT_FOR_COMPLETION);

    when(_resourceManager.getSegmentZKMetadata(TABLE, committing)).thenReturn(metadata(committing, Status.DONE, true));
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_A, SERVER_B, SERVER_C));
    _externalView.setState(committing, SERVER_A, SegmentStateModel.CONSUMING);
    _manager.run();
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertEquals(onlyPlan().segments().get(committing).action(), RecoveryAction.WAIT_FOR_COMPLETION);

    when(_llcManager.isExceededMaxSegmentCompletionTime(eq(committing), any(Stat.class), anyLong())).thenReturn(true);
    _manager.run();
    assertEquals(onlyPlan().segments().get(committing).action(), RecoveryAction.REPAIR_COMPLETION);
    verify(_llcManager, never()).repairCompletedReplicateModeSegments(anyString());

    addSegment(0, 1, SERVER_B, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    _idealState.setInstanceStateMap(committing, Map.of(SERVER_A, SegmentStateModel.ONLINE,
        SERVER_B, SegmentStateModel.ONLINE, SERVER_C, SegmentStateModel.ONLINE));
    _externalView.setState(committing, SERVER_A, SegmentStateModel.ONLINE);
    _manager.run();
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertTrue(_manager.getRecoveryPlans(TABLE).isEmpty());
  }

  @Test
  public void testReconstructsTrackedDoneFanoutWithoutPreviousObservations() {
    String done = addSegment(0, 0, SERVER_A, SegmentStateModel.ONLINE, Status.DONE, true);
    addSegment(0, 1, SERVER_B, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_llcManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of(done));
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_A, SERVER_B, SERVER_C));

    _manager.run();

    assertEquals(onlyPlan().segments().get(done).action(), RecoveryAction.REPAIR_COMPLETION);
    verify(_executor).execute(eq(TABLE), eq(_manager.getRecoveryPlans(TABLE)), any());
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
  }

  @Test
  public void testHistoricalDonePlacementIsNotMistakenForPendingFanout() {
    String done = addSegment(0, 0, SERVER_A, SegmentStateModel.ONLINE, Status.DONE, true);
    _idealState.setInstanceStateMap(done, Map.of(SERVER_A, SegmentStateModel.ONLINE,
        SERVER_B, SegmentStateModel.ONLINE));
    addSegment(0, 1, SERVER_B, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);

    _manager.run();

    assertTrue(_manager.getRecoveryPlans(TABLE).isEmpty());
    verify(_resourceManager, never()).getSegmentZKMetadata(TABLE, done);
  }

  @DataProvider
  public Object[][] observationCleanupTriggers() {
    return new Object[][]{{"disabled"}, {"paused"}, {"topicPaused"}, {"nonLeader"}, {"deleted"}, {"stopped"}};
  }

  @Test
  public void testResumesPersistedManualRepairAfterControllerRestart() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    ReplicateModeAssignmentUtils.setPendingRestart(_idealState, segment, SERVER_A);
    _manager.run();
    verify(_llcManager).restartReplicateModeConsumer(TABLE, segment, SERVER_A);
    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
  }

  @Test
  public void testPersistedManualRepairRespectsPauseAndRetriesAfterFailure() {
    String segment = addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    ReplicateModeAssignmentUtils.setPendingRestart(_idealState, segment, SERVER_A);
    _idealState.getRecord().setSimpleField("isTablePaused", "true");
    _manager.run();
    verify(_llcManager, never()).restartReplicateModeConsumer(anyString(), anyString(), anyString());
    _idealState.getRecord().setSimpleField("isTablePaused", "false");
    when(_llcManager.restartReplicateModeConsumer(TABLE, segment, SERVER_A))
        .thenThrow(new IllegalStateException("Pending Helix transition"));
    _manager.run();
    _manager.run();
    verify(_llcManager, times(2)).restartReplicateModeConsumer(TABLE, segment, SERVER_A);
    assertEquals(ReplicateModeAssignmentUtils.getPendingRestarts(_idealState), Map.of(segment, SERVER_A));
  }

  @Test(dataProvider = "observationCleanupTriggers")
  public void testCleansUpFailuresAndPlansTogether(String trigger)
      throws Exception {
    addSegment(0, 0, SERVER_A, SegmentStateModel.CONSUMING, Status.IN_PROGRESS, true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(SERVER_B));
    _manager.run();
    assertEquals(_manager.getPendingFailures(TABLE).size(), 1);
    assertEquals(_manager.getRecoveryPlans(TABLE).size(), 1);

    switch (trigger) {
      case "disabled" -> _idealState.enable(false);
      case "paused" -> _idealState.getRecord().setSimpleField("isTablePaused", "true");
      case "topicPaused" -> _idealState.getRecord().setSimpleField(PinotLLCRealtimeSegmentManager.PAUSE_STATE,
          new PauseState(false, PauseState.ReasonCode.ADMINISTRATIVE, null, null, List.of(0)).toJsonString());
      case "nonLeader" -> when(_leadControllerManager.isLeaderForTable(TABLE)).thenReturn(false);
      case "deleted" -> {
        when(_resourceManager.getAllTables()).thenReturn(List.of());
        when(_resourceManager.getTableIdealState(TABLE)).thenReturn(null);
      }
      case "stopped" -> _manager.stop();
      default -> throw new IllegalArgumentException(trigger);
    }
    _manager.run();

    assertTrue(_manager.getPendingFailures(TABLE).isEmpty());
    assertTrue(_manager.getRecoveryPlans(TABLE).isEmpty());
  }

  private String addSegment(int partitionId, int sequenceNumber, String server, String state, Status status,
      boolean replicateMode) {
    String segment = segmentName(partitionId, sequenceNumber);
    ReplicateModeAssignmentUtils.setAssignment(_idealState, partitionId, server,
        List.of(SERVER_A, SERVER_B, SERVER_C));
    _idealState.setInstanceStateMap(segment, Map.of(server, state));
    when(_resourceManager.getSegmentZKMetadata(TABLE, segment)).thenReturn(metadata(segment, status, replicateMode));
    return segment;
  }

  private static SegmentZKMetadata metadata(String segment, Status status, boolean replicateMode) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(segment);
    metadata.setStatus(status);
    metadata.setReplicateMode(replicateMode);
    metadata.setNumReplicas(3);
    metadata.setStartOffset("0");
    if (status == Status.COMMITTING || status == Status.DONE) {
      metadata.setEndOffset("10");
    }
    return metadata;
  }

  private static String segmentName(int partitionId, int sequenceNumber) {
    return new LLCSegmentName("testTable", partitionId, sequenceNumber, 1000L).getSegmentName();
  }

  private PendingFailure onlyFailure() {
    Map<String, PendingFailure> failures = _manager.getPendingFailures(TABLE);
    assertEquals(failures.size(), 1);
    return failures.values().iterator().next();
  }

  private PartitionRecoveryPlan onlyPlan() {
    Map<Integer, PartitionRecoveryPlan> plans = _manager.getRecoveryPlans(TABLE);
    assertEquals(plans.size(), 1);
    return plans.values().iterator().next();
  }

  private static TableConfig tableConfig(String rawTableName) {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(rawTableName).setServerTenant("DefaultTenant")
        .setNumReplicas(3).build();
  }
}
