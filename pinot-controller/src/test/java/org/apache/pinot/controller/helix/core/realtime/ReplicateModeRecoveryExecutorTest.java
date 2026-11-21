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

import com.google.common.collect.HashBiMap;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.helix.HelixManager;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.CandidateContext;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.PartitionRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.RecoveryAction;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentSnapshot;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.zookeeper.data.Stat;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/// Exercises real revalidation callbacks, stale-plan races, retry policy and immutable recovery ranges without HTTP.
public class ReplicateModeRecoveryExecutorTest {
  private static final String TABLE = "testTable_REALTIME";
  private static final String SEGMENT = "testTable__0__0__20250228T1903Z";
  private static final String NEXT = "testTable__0__1__20250228T1903Z";
  private static final String A = "Server_a_1234";
  private static final String B = "Server_b_1234";
  private static final String C = "Server_c_1234";
  private static final List<String> SERVERS = List.of(A, B, C);
  private ControllerConf _config;
  private PinotHelixResourceManager _resourceManager;
  private PinotLLCRealtimeSegmentManager _segmentManager;
  private SegmentCompletionManager _completionManager;
  private LeadControllerManager _leadManager;
  private ControllerMetrics _metrics;
  private HelixManager _helix;
  private Clock _clock;
  private AtomicReference<IdealState> _state;
  private SegmentZKMetadata _metadata;
  private ExternalView _externalView;
  private Map<String, InstanceConfig> _configs;
  private MockedStatic<HelixHelper> _helper;
  private Runnable _beforeUpdate;
  private Runnable _beforeRetry;
  private ReplicateModeRecoveryExecutor _executor;

  @BeforeMethod
  public void setUp() throws Exception {
    _config = new ControllerConf();
    _resourceManager = mock(PinotHelixResourceManager.class);
    _segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    _completionManager = mock(SegmentCompletionManager.class);
    _leadManager = mock(LeadControllerManager.class);
    _metrics = mock(ControllerMetrics.class);
    _helix = mock(HelixManager.class);
    _clock = mock(Clock.class);
    when(_clock.millis()).thenReturn(100_000L);
    when(_segmentManager.getSegmentZKMetadataIfExists(anyString(), anyString(), any(Stat.class)))
        .thenAnswer(invocation -> {
          Stat stat = invocation.getArgument(2);
          stat.setMtime(1L);
          return _resourceManager.getSegmentZKMetadata(invocation.getArgument(0), invocation.getArgument(1));
        });
    when(_helix.isConnected()).thenReturn(true);
    when(_resourceManager.getHelixZkManager()).thenReturn(_helix);
    when(_leadManager.isLeaderForTable(TABLE)).thenReturn(true);
    _state = new AtomicReference<>(new IdealState(TABLE));
    _state.get().enable(true);
    _state.get().setInstanceStateMap(SEGMENT, Map.of(A, SegmentStateModel.CONSUMING));
    ReplicateModeAssignmentUtils.setAssignment(_state.get(), 0, A, SERVERS);
    when(_resourceManager.getTableIdealState(TABLE)).thenAnswer(invocation -> _state.get());
    _metadata = new SegmentZKMetadata(SEGMENT);
    _metadata.setReplicateMode(true);
    _metadata.setStatus(Status.IN_PROGRESS);
    _metadata.setNumReplicas(3);
    _metadata.setStartOffset("100");
    when(_resourceManager.getSegmentZKMetadata(TABLE, SEGMENT)).thenAnswer(invocation -> _metadata);
    _externalView = new ExternalView(TABLE);
    _externalView.setState(SEGMENT, A, SegmentStateModel.CONSUMING);
    when(_resourceManager.getTableExternalView(TABLE)).thenReturn(_externalView);
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(B, C));
    _configs = new HashMap<>();
    for (String server : SERVERS) {
      InstanceConfig config = new InstanceConfig(server);
      config.addTag("tenant_REALTIME");
      _configs.put(server, config);
    }
    when(_resourceManager.getAllHelixInstanceConfigs()).thenAnswer(invocation -> List.copyOf(_configs.values()));
    TableConfig table = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setServerTenant("tenant").setNumReplicas(3).build();
    when(_resourceManager.getTableConfig(TABLE)).thenReturn(table);
    InstancePartitions partitions = new InstancePartitions("testTable_CONSUMING");
    partitions.setInstances(0, 0, SERVERS);
    when(_segmentManager.getConsumingInstancePartitions(table)).thenReturn(partitions);
    when(_segmentManager.isExceededMaxSegmentCompletionTime(eq(SEGMENT), any(Stat.class), anyLong())).thenReturn(true);
    when(_completionManager.getRecoveryStatus(SEGMENT)).thenReturn(RecoveryStatus.NO_ACTIVE_COMMIT);
    when(_completionManager.withSegmentCompletionLock(anyString(), any())).thenAnswer(
        invocation -> ((Supplier<?>) invocation.getArgument(1)).get());
    when(_resourceManager.getDataInstanceAdminEndpoints(Set.of(B)))
        .thenReturn(HashBiMap.create(Map.of(B, "b:8097")));
    _beforeUpdate = () -> { };
    _beforeRetry = null;
    _helper = mockStatic(HelixHelper.class);
    _helper.when(() -> HelixHelper.updateIdealState(eq(_helix), eq(TABLE), any())).thenAnswer(invocation -> {
      _beforeUpdate.run();
      Function<IdealState, IdealState> update = invocation.getArgument(2);
      if (_beforeRetry != null) {
        update.apply(new IdealState(new ZNRecord(_state.get().getRecord())));
        _beforeRetry.run();
      }
      IdealState updated = update.apply(
          new IdealState(new ZNRecord(_state.get().getRecord())));
      _state.set(updated);
      return updated;
    });
    _executor = executor();
  }

  @AfterMethod
  public void tearDown() {
    _helper.close();
  }

  @Test
  public void testHandoffChangesRoleAndSameSegmentTogether() {
    execute(plan(RecoveryAction.REASSIGN_CONSUMING));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_state.get(), 0), B);
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(_state.get(), 0), SERVERS);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(B, SegmentStateModel.CONSUMING));
    assertEquals(_state.get().getRecord().getMapFields().keySet(), Set.of(SEGMENT));
    assertEquals(_metadata.getStatus(), Status.IN_PROGRESS);
    assertEquals(_metadata.getStartOffset(), "100");
    assertEquals(_metadata.getNumReplicas(), 3);
    verify(_metrics).addMeteredTableValue(TABLE, ControllerMeter.REPLICATE_MODE_HANDOFFS, 1);
    verify(_segmentManager, never()).commitSegmentMetadata(anyString(), any());
  }

  @Test
  public void testAlreadyAdmittedCommitDoesNotBlockConsumptionTakeover() {
    when(_completionManager.getRecoveryStatus(SEGMENT)).thenReturn(RecoveryStatus.COMMIT_IN_PROGRESS);
    execute(plan(RecoveryAction.REASSIGN_CONSUMING));
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(B, SegmentStateModel.CONSUMING));
  }

  @DataProvider
  public Object[][] stalePlans() {
    return new Object[][]{{"leader"}, {"range"}, {"done"}, {"live"}, {"disabled"}, {"pause"}, {"target"},
        {"leadership"}, {"connection"}, {"newer"}, {"roles"}, {"mode"}, {"state"}};
  }

  @Test(dataProvider = "stalePlans")
  public void testStalePlansCannotAuthorizeAWrite(String change) {
    Map<Integer, PartitionRecoveryPlan> plan = plan(RecoveryAction.REASSIGN_CONSUMING);
    _beforeUpdate = () -> {
      switch (change) {
        case "leader" -> ReplicateModeAssignmentUtils.setAssignment(_state.get(), 0, C, SERVERS);
        case "range" -> _metadata.setStartOffset("101");
        case "done" -> _metadata.setStatus(Status.DONE);
        case "live" -> when(_resourceManager.getAllLiveInstances()).thenReturn(SERVERS);
        case "disabled" -> _state.get().enable(false);
        case "pause" -> {
          PauseState pause = new PauseState();
          pause.setPaused(true);
          _state.get().getRecord().setSimpleField(PinotLLCRealtimeSegmentManager.PAUSE_STATE, pause.toJsonString());
        }
        case "target" -> _configs.get(B).setInstanceOperation(InstanceOperation.DISABLE);
        case "leadership" -> when(_leadManager.isLeaderForTable(TABLE)).thenReturn(false);
        case "connection" -> when(_helix.isConnected()).thenReturn(false);
        case "newer" -> _state.get().setInstanceStateMap(NEXT, Map.of(C, SegmentStateModel.CONSUMING));
        case "roles" -> ReplicateModeAssignmentUtils.setAssignment(_state.get(), 0, A, List.of(A, C));
        case "mode" -> _metadata.setReplicateMode(false);
        case "state" -> _state.get().setInstanceStateMap(SEGMENT, Map.of(A, SegmentStateModel.ERROR));
        default -> throw new IllegalArgumentException(change);
      }
    };
    execute(plan);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT),
        Map.of(A, change.equals("state") ? SegmentStateModel.ERROR : SegmentStateModel.CONSUMING));
    verify(_metrics, never()).addMeteredTableValue(TABLE, ControllerMeter.REPLICATE_MODE_HANDOFFS, 1);
  }

  @Test
  public void testCASRetryRevalidatesCompletionAndPreservesNextSegment() {
    _beforeRetry = () -> {
      _metadata.setStatus(Status.DONE);
      _state.get().setInstanceStateMap(SEGMENT,
          Map.of(A, SegmentStateModel.ONLINE, B, SegmentStateModel.ONLINE, C, SegmentStateModel.ONLINE));
      _state.get().setInstanceStateMap(NEXT, Map.of(A, SegmentStateModel.CONSUMING));
    };
    execute(plan(RecoveryAction.REASSIGN_CONSUMING));
    assertEquals(_state.get().getInstanceStateMap(SEGMENT).size(), 3);
    assertEquals(_state.get().getInstanceStateMap(NEXT), Map.of(A, SegmentStateModel.CONSUMING));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_state.get(), 0), A);
  }

  @Test
  public void testCommittingRecoveryAssignsOnlineAndKeepsRange() throws Exception {
    committing();
    Map<Integer, PartitionRecoveryPlan> plan = plan(RecoveryAction.REINGEST_COMMITTING);
    execute(plan);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(B, SegmentStateModel.ONLINE));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_state.get(), 0), A);
    assertEquals(_metadata.getStartOffset(), "100");
    assertEquals(_metadata.getEndOffset(), "200");
    assertEquals(_metadata.getStatus(), Status.COMMITTING);
    verify(_segmentManager).triggerReingestion("b:8097", SEGMENT, 5000);
    // ERROR is expected until the missing COMMITTING artifact is rebuilt. It must not rotate healthy workers.
    _externalView.setState(SEGMENT, B, SegmentStateModel.ERROR);
    when(_clock.millis()).thenReturn(110_000L);
    execute(plan);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(B, SegmentStateModel.ONLINE));
    verify(_segmentManager, times(2)).triggerReingestion("b:8097", SEGMENT, 5000);
  }

  @Test
  public void testCommittingRecoveryTracksCompletionAfterReplicaReduction() throws Exception {
    committing();
    List<String> currentCandidates = List.of(A, B);
    ReplicateModeAssignmentUtils.setAssignment(_state.get(), 0, A, currentCandidates);
    SegmentRecoveryPlan action = plan(RecoveryAction.REINGEST_COMMITTING).get(0).segments().get(SEGMENT);
    Map<Integer, PartitionRecoveryPlan> plans = Map.of(0, new PartitionRecoveryPlan(0, A, currentCandidates,
        List.of(B), Map.of(), Map.of(SEGMENT, action), 100_000L));

    execute(plans);

    verify(_segmentManager).ensureReplicateModeCompletionTracked(TABLE, SEGMENT);
    verify(_segmentManager).triggerReingestion("b:8097", SEGMENT, 5000);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(B, SegmentStateModel.ONLINE));
    assertEquals(_metadata.getNumReplicas(), 3);
  }

  @Test
  public void testFailedCompletionTrackingDoesNotPublishReplayAssignment() throws Exception {
    committing();
    doThrow(new IllegalStateException("Completion entry could not be persisted")).when(_segmentManager)
        .ensureReplicateModeCompletionTracked(TABLE, SEGMENT);

    execute(plan(RecoveryAction.REINGEST_COMMITTING));

    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(A, SegmentStateModel.ONLINE));
    verify(_segmentManager, never()).triggerReingestion(anyString(), anyString(), anyInt());
  }

  @Test
  public void testIndependentCommittingTargetsSurviveExecutionRevalidation() throws Exception {
    committing();
    SegmentZKMetadata next = new SegmentZKMetadata(NEXT);
    next.setReplicateMode(true);
    next.setStatus(Status.COMMITTING);
    next.setNumReplicas(3);
    next.setStartOffset("200");
    next.setEndOffset("300");
    _state.get().setInstanceStateMap(NEXT, Map.of(A, SegmentStateModel.ONLINE));
    when(_resourceManager.getSegmentZKMetadata(TABLE, NEXT)).thenReturn(next);
    when(_completionManager.getRecoveryStatus(NEXT)).thenReturn(RecoveryStatus.NO_ACTIVE_COMMIT);
    when(_segmentManager.isExceededMaxSegmentCompletionTime(eq(NEXT), any(Stat.class), anyLong())).thenReturn(true);
    when(_resourceManager.getAllLiveInstances()).thenReturn(SERVERS);
    _configs.get(B).setInstanceEnabledForPartition(TABLE, SEGMENT, false);
    _configs.get(C).setInstanceEnabledForPartition(TABLE, SEGMENT, false);
    _configs.get(A).setInstanceEnabledForPartition(TABLE, NEXT, false);
    when(_resourceManager.getDataInstanceAdminEndpoints(Set.of(A)))
        .thenReturn(HashBiMap.create(Map.of(A, "a:8097")));
    TableConfig config = _resourceManager.getTableConfig(TABLE);
    CandidateContext context = new CandidateContext(config, _state.get(),
        _segmentManager.getConsumingInstancePartitions(config), _configs, Set.copyOf(SERVERS));
    Map<Integer, PartitionRecoveryPlan> plans = new ReplicateModeRecoveryPlanner(_config).plan(context,
        List.of(new SegmentSnapshot(_metadata, _state.get().getInstanceStateMap(SEGMENT), null,
                RecoveryStatus.NO_ACTIVE_COMMIT, true, false),
            new SegmentSnapshot(next, _state.get().getInstanceStateMap(NEXT), null,
                RecoveryStatus.NO_ACTIVE_COMMIT, true, false)), _externalView, 100_000L);

    execute(plans);

    verify(_segmentManager).triggerReingestion("a:8097", SEGMENT, 5000);
    verify(_segmentManager).triggerReingestion("b:8097", NEXT, 5000);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(A, SegmentStateModel.ONLINE));
    assertEquals(_state.get().getInstanceStateMap(NEXT), Map.of(B, SegmentStateModel.ONLINE));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_state.get(), 0), A);
    assertEquals(_metadata.getStatus(), Status.COMMITTING);
    assertEquals(_metadata.getStartOffset(), "100");
    assertEquals(_metadata.getEndOffset(), "200");
    assertEquals(next.getStartOffset(), "200");
    assertEquals(next.getEndOffset(), "300");
  }

  @Test
  public void testCommittingWaitsForNativeTimeoutThenCanCompeteWithAcceptedCommit() throws Exception {
    committing();
    Map<Integer, PartitionRecoveryPlan> plan = plan(RecoveryAction.REINGEST_COMMITTING);
    when(_segmentManager.isExceededMaxSegmentCompletionTime(eq(SEGMENT), any(Stat.class), anyLong())).thenReturn(false);
    execute(plan);
    verify(_segmentManager, never()).triggerReingestion(anyString(), anyString(), anyInt());
    when(_segmentManager.isExceededMaxSegmentCompletionTime(eq(SEGMENT), any(Stat.class), anyLong())).thenReturn(true);
    when(_completionManager.getRecoveryStatus(SEGMENT)).thenReturn(RecoveryStatus.UNKNOWN);
    execute(plan);
    verify(_segmentManager, never()).triggerReingestion(anyString(), anyString(), anyInt());
    when(_completionManager.getRecoveryStatus(SEGMENT)).thenReturn(RecoveryStatus.COMMIT_IN_PROGRESS);
    execute(plan);
    verify(_segmentManager).triggerReingestion("b:8097", SEGMENT, 5000);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(B, SegmentStateModel.ONLINE));
  }

  @Test
  public void testFailedTriggerBacksOffWithoutHoldingUpConsumption() throws Exception {
    committing();
    doThrow(new IOException("Server table manager is initializing")).when(_segmentManager)
        .triggerReingestion("b:8097", SEGMENT, 5000);
    Map<Integer, PartitionRecoveryPlan> plan = plan(RecoveryAction.REINGEST_COMMITTING);
    execute(plan);
    when(_clock.millis()).thenReturn(109_999L);
    execute(plan);
    verify(_segmentManager).triggerReingestion("b:8097", SEGMENT, 5000);
    when(_clock.millis()).thenReturn(110_000L);
    execute(plan);
    when(_clock.millis()).thenReturn(129_999L);
    execute(plan);
    verify(_segmentManager, times(2)).triggerReingestion("b:8097", SEGMENT, 5000);
    when(_clock.millis()).thenReturn(130_000L);
    execute(plan);
    verify(_segmentManager, times(3)).triggerReingestion("b:8097", SEGMENT, 5000);
  }

  @Test
  public void testPersistedCooldownSurvivesExecutorRecreation() {
    execute(plan(RecoveryAction.REASSIGN_CONSUMING));
    String firstHandoff = _state.get().getRecord()
        .getSimpleField(ReplicateModeRecoveryExecutor.LAST_HANDOFF_PREFIX + 0);
    assertEquals(firstHandoff, "100000");
    when(_resourceManager.getAllLiveInstances()).thenReturn(List.of(C));
    _executor = executor();
    Map<Integer, PartitionRecoveryPlan> second = Map.of(0,
        new PartitionRecoveryPlan(0, B, SERVERS, List.of(C), Map.of(), Map.of(SEGMENT,
            new SegmentRecoveryPlan(SEGMENT, Status.IN_PROGRESS, RecoveryAction.REASSIGN_CONSUMING, C, null,
                "100", null, RecoveryStatus.NO_ACTIVE_COMMIT, false)), 110_000L));
    when(_clock.millis()).thenReturn(110_000L);
    execute(second);
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_state.get(), 0), B);
    when(_clock.millis()).thenReturn(130_000L);
    execute(second);
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_state.get(), 0), C);
    assertNotEquals(_state.get().getRecord().getSimpleField(ReplicateModeRecoveryExecutor.LAST_HANDOFF_PREFIX + 0),
        firstHandoff);
  }

  @Test
  public void testStoppedTaskAndDisconnectedControllerDoNoRecovery() throws Exception {
    _executor.execute(TABLE, plan(RecoveryAction.REASSIGN_CONSUMING), () -> false);
    when(_helix.isConnected()).thenReturn(false);
    execute(plan(RecoveryAction.REASSIGN_CONSUMING));
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), Map.of(A, SegmentStateModel.CONSUMING));
    verify(_resourceManager, never()).getSegmentZKMetadata(anyString(), anyString());
    verify(_segmentManager, never()).triggerReingestion(anyString(), anyString(), anyInt());
  }

  @Test
  public void testRecoveryBudgetDoesNotStarveAnotherCommittingSegment() throws Exception {
    committing();
    _config = new ControllerConf(Map.of("controller.realtime.replicateMode.recovery.maxActionsPerTable", 1));
    _executor = executor();
    SegmentZKMetadata next = new SegmentZKMetadata(NEXT);
    next.setReplicateMode(true);
    next.setStatus(Status.COMMITTING);
    next.setNumReplicas(3);
    next.setStartOffset("200");
    next.setEndOffset("300");
    when(_resourceManager.getSegmentZKMetadata(TABLE, NEXT)).thenReturn(next);
    when(_completionManager.getRecoveryStatus(NEXT)).thenReturn(RecoveryStatus.NO_ACTIVE_COMMIT);
    when(_segmentManager.isExceededMaxSegmentCompletionTime(eq(NEXT), any(Stat.class), anyLong())).thenReturn(true);
    _state.get().setInstanceStateMap(NEXT, Map.of(A, SegmentStateModel.ONLINE));
    SegmentRecoveryPlan first = plan(RecoveryAction.REINGEST_COMMITTING).get(0).segments().get(SEGMENT);
    SegmentRecoveryPlan second = new SegmentRecoveryPlan(NEXT, Status.COMMITTING, RecoveryAction.REINGEST_COMMITTING,
        B, null, "200", "300", RecoveryStatus.NO_ACTIVE_COMMIT, true);
    Map<Integer, PartitionRecoveryPlan> plans = Map.of(0, new PartitionRecoveryPlan(0, A, SERVERS, List.of(B, C),
        Map.of(), Map.of(SEGMENT, first, NEXT, second), 100_000L));
    execute(plans);
    verify(_segmentManager).triggerReingestion("b:8097", SEGMENT, 5000);
    verify(_segmentManager, never()).triggerReingestion("b:8097", NEXT, 5000);
    execute(plans);
    verify(_segmentManager).triggerReingestion("b:8097", NEXT, 5000);
  }

  @Test
  public void testReplayedDoneFansOutWithoutWaitingForMetadataTimeout() {
    _metadata.setStatus(Status.DONE);
    _state.get().setInstanceStateMap(SEGMENT, Map.of(B, SegmentStateModel.ONLINE));
    when(_segmentManager.getPendingSegmentCompletions(TABLE)).thenReturn(List.of(SEGMENT));
    when(_segmentManager.isExceededMaxSegmentCompletionTime(eq(SEGMENT), any(Stat.class), anyLong())).thenReturn(false);
    when(_resourceManager.getTableConfig(TABLE)).thenReturn(null);
    execute(plan(RecoveryAction.REPAIR_COMPLETION));
    verify(_segmentManager).repairCompletedReplicateModeSegments(TABLE);
  }

  @Test
  public void testStaleDonePlanDoesNotRepairCompletedHistoricalPlacement() {
    _metadata.setStatus(Status.DONE);
    _state.get().setInstanceStateMap(SEGMENT, Map.of(B, SegmentStateModel.ONLINE));

    execute(plan(RecoveryAction.REPAIR_COMPLETION));

    verify(_segmentManager, never()).repairCompletedReplicateModeSegments(anyString());
    verify(_segmentManager, never()).ensureAllPartitionsConsuming(any(), any(), any());
  }

  private void committing() {
    _metadata.setStatus(Status.COMMITTING);
    _metadata.setEndOffset("200");
    _state.get().setInstanceStateMap(SEGMENT, Map.of(A, SegmentStateModel.ONLINE));
  }

  private Map<Integer, PartitionRecoveryPlan> plan(RecoveryAction action) {
    return Map.of(0, new PartitionRecoveryPlan(0, A, SERVERS, List.of(B, C), Map.of(),
        Map.of(SEGMENT, new SegmentRecoveryPlan(SEGMENT, _metadata.getStatus(), action, B, null,
            _metadata.getStartOffset(), _metadata.getEndOffset(), RecoveryStatus.NO_ACTIVE_COMMIT, true)), 100_000L));
  }

  private void execute(Map<Integer, PartitionRecoveryPlan> plans) {
    _executor.execute(TABLE, plans, () -> true);
  }

  private ReplicateModeRecoveryExecutor executor() {
    return new ReplicateModeRecoveryExecutor(_config, _resourceManager, _leadManager, _segmentManager,
        _completionManager, _metrics, _clock);
  }
}
