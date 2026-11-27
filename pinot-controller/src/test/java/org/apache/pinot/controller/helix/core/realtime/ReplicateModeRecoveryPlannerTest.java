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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.CandidateContext;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.BlockedReason;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.FailureReason;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.PartitionRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.RecoveryAction;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentSnapshot;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies advisory recovery choices, native completion boundaries and preservation of segment ranges and inputs.
public class ReplicateModeRecoveryPlannerTest {
  private static final String TABLE = "testTable_REALTIME";
  private static final String SERVER_A = "Server_a_1234";
  private static final String SERVER_B = "Server_b_1234";
  private static final String SERVER_C = "Server_c_1234";
  private static final List<String> SERVERS = List.of(SERVER_A, SERVER_B, SERVER_C);
  private static final long NOW_MS = 1_000_000L;

  private TableConfig _tableConfig;
  private IdealState _idealState;
  private InstancePartitions _instancePartitions;
  private Map<String, InstanceConfig> _instanceConfigs;
  private Set<String> _liveInstances;
  private ReplicateModeRecoveryPlanner _planner;

  @BeforeMethod
  public void setUp() {
    _tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setServerTenant("tenant").setNumReplicas(3).build();
    _idealState = new IdealState(TABLE);
    _idealState.enable(true);
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_A, SERVERS);
    _instancePartitions = new InstancePartitions("testTable_CONSUMING");
    _instancePartitions.setInstances(0, 0, SERVERS);
    _instanceConfigs = new HashMap<>();
    for (String server : SERVERS) {
      InstanceConfig config = new InstanceConfig(server);
      config.setInstanceOperation(InstanceOperation.ENABLE);
      config.addTag("tenant_REALTIME");
      _instanceConfigs.put(server, config);
    }
    _liveInstances = new HashSet<>(List.of(SERVER_B, SERVER_C));
    _planner = new ReplicateModeRecoveryPlanner(new ControllerConf());
  }

  @Test
  public void testPlansSameSegmentTakeoverWithoutChangingInputsOrOffsets() {
    SegmentSnapshot snapshot = failedConsuming(0, 0);
    IdealState originalAssignment = HelixHelper.cloneIdealState(_idealState);
    Map<String, String> originalMetadata = Map.copyOf(snapshot.metadata().getSimpleFields());
    String originalPlacement = _instancePartitions.toJsonString();
    String originalTable = _tableConfig.toJsonString();
    Set<String> originalLiveInstances = Set.copyOf(_liveInstances);

    Map<Integer, PartitionRecoveryPlan> plans = plan(List.of(snapshot));

    PartitionRecoveryPlan partition = plans.get(0);
    SegmentRecoveryPlan action = partition.segments().get(snapshot.metadata().getSegmentName());
    assertEquals(partition.expectedLeader(), SERVER_A);
    assertEquals(partition.replicaCandidates(), SERVERS);
    assertEquals(partition.eligibleServers(), List.of(SERVER_B, SERVER_C));
    assertEquals(partition.observedTimeMs(), NOW_MS);
    assertEquals(action.action(), RecoveryAction.REASSIGN_CONSUMING);
    assertEquals(action.targetServer(), SERVER_B);
    assertEquals(action.segmentName(), snapshot.metadata().getSegmentName());
    assertEquals(action.startOffset(), "0");
    assertNull(action.endOffset());
    assertNull(action.blockedReason());
    assertEquals(_idealState, originalAssignment);
    assertEquals(snapshot.metadata().getSimpleFields(), originalMetadata);
    assertEquals(snapshot.assignment(), Map.of(SERVER_A, SegmentStateModel.CONSUMING));
    assertEquals(_instancePartitions.toJsonString(), originalPlacement);
    assertEquals(_tableConfig.toJsonString(), originalTable);
    assertEquals(_liveInstances, originalLiveInstances);
  }

  @Test
  public void testConsumptionTakeoverDoesNotCancelOldActiveCommit() {
    SegmentSnapshot failed = failedConsuming(0, 0);
    SegmentSnapshot activeCommit = new SegmentSnapshot(failed.metadata(), failed.assignment(),
        failed.failureReason(), RecoveryStatus.COMMIT_IN_PROGRESS, false, false);

    SegmentRecoveryPlan action = onlyAction(activeCommit);

    assertEquals(action.action(), RecoveryAction.REASSIGN_CONSUMING);
    assertEquals(action.targetServer(), SERVER_B);
    assertEquals(action.completionStatus(), RecoveryStatus.COMMIT_IN_PROGRESS);
    assertFalse(action.completionTimedOut());
    assertEquals(_idealState.getInstanceStateMap(action.segmentName()),
        Map.of(SERVER_A, SegmentStateModel.CONSUMING));
  }

  @Test
  public void testVirtualLeadershipCountsBalanceMultipleFailuresInOnePass() {
    SegmentSnapshot first = failedConsuming(0, 0);
    SegmentSnapshot second = failedConsuming(2, 0);
    SegmentSnapshot third = failedConsuming(4, 0);
    IdealState originalAssignment = HelixHelper.cloneIdealState(_idealState);

    Map<Integer, PartitionRecoveryPlan> plans = plan(List.of(third, second, first));

    assertEquals(action(plans, first).targetServer(), SERVER_B);
    assertEquals(action(plans, second).targetServer(), SERVER_C);
    assertEquals(action(plans, third).targetServer(), SERVER_B);
    assertEquals(_idealState, originalAssignment);
  }

  @DataProvider
  public Object[][] committingProgress() {
    return new Object[][]{
        {RecoveryStatus.COMMIT_IN_PROGRESS, true, RecoveryAction.REINGEST_COMMITTING},
        {RecoveryStatus.COMMIT_IN_PROGRESS, false, RecoveryAction.WAIT_FOR_COMPLETION},
        {RecoveryStatus.UNKNOWN, true, RecoveryAction.WAIT_FOR_COMPLETION},
        {RecoveryStatus.NO_ACTIVE_COMMIT, false, RecoveryAction.WAIT_FOR_COMPLETION},
        {RecoveryStatus.COMMIT_EXPIRED, false, RecoveryAction.WAIT_FOR_COMPLETION},
        {RecoveryStatus.NO_ACTIVE_COMMIT, true, RecoveryAction.REINGEST_COMMITTING},
        {RecoveryStatus.COMMIT_EXPIRED, true, RecoveryAction.REINGEST_COMMITTING}
    };
  }

  @Test(dataProvider = "committingProgress")
  public void testCommittingRecoveryRequiresNativeTimeoutAndKnownPublicationContract(RecoveryStatus completionStatus,
      boolean metadataTimedOut, RecoveryAction expectedAction) {
    SegmentSnapshot snapshot = snapshot(0, 0, Status.COMMITTING, SegmentStateModel.ONLINE,
        FailureReason.INSTANCE_NOT_LIVE, completionStatus, metadataTimedOut);

    SegmentRecoveryPlan action = onlyAction(snapshot);

    assertEquals(action.action(), expectedAction);
    assertEquals(action.startOffset(), "0");
    assertEquals(action.endOffset(), "10");
    assertEquals(action.status(), Status.COMMITTING);
    assertEquals(action.completionStatus(), completionStatus);
    assertEquals(action.completionTimedOut(), metadataTimedOut);
    if (expectedAction == RecoveryAction.WAIT_FOR_COMPLETION) {
      assertNull(action.targetServer());
    } else {
      assertEquals(action.targetServer(), SERVER_B);
    }
    assertEquals(snapshot.metadata().getStatus(), Status.COMMITTING);
    assertEquals(snapshot.metadata().getEndOffset(), "10");
    assertEquals(_idealState.getRecord().getMapFields().keySet(), Set.of(action.segmentName()));
  }

  @Test
  public void testOldCommittingSegmentReusesHealthyLeaderOfNextSegment() {
    SegmentSnapshot committing = snapshot(0, 0, Status.COMMITTING, SegmentStateModel.ONLINE,
        FailureReason.INSTANCE_NOT_LIVE, RecoveryStatus.NO_ACTIVE_COMMIT, true);
    String next = segmentName(0, 1);
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_B, SERVERS);
    _idealState.setInstanceStateMap(next, Map.of(SERVER_B, SegmentStateModel.CONSUMING));

    SegmentRecoveryPlan action = onlyAction(committing);

    assertEquals(action.action(), RecoveryAction.REINGEST_COMMITTING);
    assertEquals(action.targetServer(), SERVER_B);
    assertEquals(action.startOffset(), "0");
    assertEquals(action.endOffset(), "10");
    assertEquals(_idealState.getInstanceStateMap(next), Map.of(SERVER_B, SegmentStateModel.CONSUMING));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_idealState, 0), SERVER_B);
  }

  @Test
  public void testCommittingRangesChooseIndependentEligibleRecoveryServers() {
    _liveInstances.add(SERVER_A);
    SegmentSnapshot first = snapshot(0, 0, Status.COMMITTING, SegmentStateModel.ONLINE, null,
        RecoveryStatus.COMMIT_EXPIRED, true);
    SegmentSnapshot second = snapshot(0, 1, Status.COMMITTING, SegmentStateModel.ONLINE, null,
        RecoveryStatus.COMMIT_EXPIRED, true);
    second.metadata().setStartOffset("10");
    second.metadata().setEndOffset("20");
    _instanceConfigs.get(SERVER_B).setInstanceEnabledForPartition(TABLE, first.metadata().getSegmentName(), false);
    _instanceConfigs.get(SERVER_C).setInstanceEnabledForPartition(TABLE, first.metadata().getSegmentName(), false);
    _instanceConfigs.get(SERVER_A).setInstanceEnabledForPartition(TABLE, second.metadata().getSegmentName(), false);
    IdealState original = HelixHelper.cloneIdealState(_idealState);

    Map<Integer, PartitionRecoveryPlan> plans = plan(List.of(second, first));

    assertEquals(action(plans, first).action(), RecoveryAction.REINGEST_COMMITTING);
    assertEquals(action(plans, first).targetServer(), SERVER_A);
    assertEquals(action(plans, second).action(), RecoveryAction.REINGEST_COMMITTING);
    assertEquals(action(plans, second).targetServer(), SERVER_B);
    assertEquals(plans.get(0).eligibleServers(), SERVERS);
    assertTrue(plans.get(0).rejectedServers().isEmpty());
    assertEquals(plan(List.of(first, second)), plans);
    assertEquals(_idealState, original);
  }

  @Test
  public void testBlockedCommittingRangeDoesNotBlockAnotherRange() {
    _liveInstances.add(SERVER_A);
    SegmentSnapshot blocked = snapshot(0, 0, Status.COMMITTING, SegmentStateModel.ONLINE, null,
        RecoveryStatus.NO_ACTIVE_COMMIT, true);
    SegmentSnapshot recoverable = snapshot(0, 1, Status.COMMITTING, SegmentStateModel.ONLINE, null,
        RecoveryStatus.NO_ACTIVE_COMMIT, true);
    recoverable.metadata().setStartOffset("10");
    recoverable.metadata().setEndOffset("20");
    for (InstanceConfig config : _instanceConfigs.values()) {
      config.setInstanceEnabledForPartition(TABLE, blocked.metadata().getSegmentName(), false);
    }
    _instanceConfigs.get(SERVER_A)
        .setInstanceEnabledForPartition(TABLE, recoverable.metadata().getSegmentName(), false);

    Map<Integer, PartitionRecoveryPlan> plans = plan(List.of(blocked, recoverable));

    assertEquals(action(plans, blocked).action(), RecoveryAction.BLOCKED);
    assertEquals(action(plans, blocked).blockedReason(), BlockedReason.NO_ELIGIBLE_REPLICA);
    assertNull(action(plans, blocked).targetServer());
    assertEquals(action(plans, recoverable).action(), RecoveryAction.REINGEST_COMMITTING);
    assertEquals(action(plans, recoverable).targetServer(), SERVER_B);
    assertEquals(plans.get(0).eligibleServers(), List.of(SERVER_B, SERVER_C));
    assertEquals(plans.get(0).rejectedServers().keySet(), Set.of(SERVER_A));
  }

  @Test
  public void testCommittingRestrictionsAndConsumerFailuresRemainSegmentScoped() {
    _liveInstances.add(SERVER_A);
    SegmentSnapshot committing = snapshot(0, 0, Status.COMMITTING, SegmentStateModel.ONLINE,
        FailureReason.STATE_TRANSITION_ERROR, RecoveryStatus.COMMIT_EXPIRED, true);
    SegmentSnapshot consuming = snapshot(0, 1, Status.IN_PROGRESS, SegmentStateModel.OFFLINE,
        FailureReason.LEADER_OFFLINE, RecoveryStatus.NO_ACTIVE_COMMIT, false);
    consuming.metadata().setStartOffset("10");
    String committingSegment = committing.metadata().getSegmentName();
    _instanceConfigs.get(SERVER_B).setInstanceEnabledForPartition(TABLE, committingSegment, false);
    _instanceConfigs.get(SERVER_C).setInstanceEnabledForPartition(TABLE, committingSegment, false);
    ExternalView externalView = new ExternalView(TABLE);
    externalView.setStateMap(committingSegment, Map.of(SERVER_A, SegmentStateModel.ERROR));
    externalView.setStateMap(consuming.metadata().getSegmentName(), Map.of(SERVER_C, SegmentStateModel.ERROR));

    Map<Integer, PartitionRecoveryPlan> plans =
        _planner.plan(context(), List.of(committing, consuming), externalView, NOW_MS);

    assertEquals(action(plans, committing).action(), RecoveryAction.REINGEST_COMMITTING);
    assertEquals(action(plans, committing).targetServer(), SERVER_A);
    assertEquals(action(plans, consuming).action(), RecoveryAction.REASSIGN_CONSUMING);
    assertEquals(action(plans, consuming).targetServer(), SERVER_B);
    assertEquals(plans.get(0).eligibleServers(), List.of(SERVER_A, SERVER_B));
    assertEquals(plans.get(0).rejectedServers().keySet(), Set.of(SERVER_C));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_idealState, 0), SERVER_A);
  }

  @DataProvider
  public Object[][] doneProgress() {
    return new Object[][]{
        {RecoveryStatus.NO_ACTIVE_COMMIT, false, RecoveryAction.WAIT_FOR_COMPLETION},
        {RecoveryStatus.COMMIT_IN_PROGRESS, true, RecoveryAction.WAIT_FOR_COMPLETION},
        {RecoveryStatus.UNKNOWN, true, RecoveryAction.WAIT_FOR_COMPLETION},
        {RecoveryStatus.NO_ACTIVE_COMMIT, true, RecoveryAction.REPAIR_COMPLETION}
    };
  }

  @Test(dataProvider = "doneProgress")
  public void testDoneWithStaleAssignmentRespectsCompletionRepairWindow(RecoveryStatus completionStatus,
      boolean metadataTimedOut, RecoveryAction expectedAction) {
    SegmentSnapshot snapshot = snapshot(0, 0, Status.DONE, SegmentStateModel.CONSUMING,
        FailureReason.INSTANCE_NOT_LIVE, completionStatus, metadataTimedOut);

    SegmentRecoveryPlan action = onlyAction(snapshot);

    assertEquals(action.action(), expectedAction);
    assertNull(action.targetServer());
    assertEquals(action.startOffset(), "0");
    assertEquals(action.endOffset(), "10");
  }

  @Test(dataProvider = "doneProgress")
  public void testOnlineDoneCanExpandFollowersImmediately(RecoveryStatus completionStatus,
      boolean metadataTimedOut, RecoveryAction ignored) {
    SegmentSnapshot snapshot = snapshot(0, 0, Status.DONE, SegmentStateModel.ONLINE,
        FailureReason.INSTANCE_NOT_LIVE, completionStatus, metadataTimedOut);

    SegmentRecoveryPlan action = onlyAction(snapshot);

    assertEquals(action.action(), RecoveryAction.REPAIR_COMPLETION);
    assertNull(action.targetServer());
  }

  @Test
  public void testFullyAssignedDoneSegmentWithNextSegmentNeedsNoRecovery() {
    _liveInstances.add(SERVER_A);
    SegmentSnapshot done = snapshot(0, 0, Status.DONE, SegmentStateModel.ONLINE, null,
        RecoveryStatus.NO_ACTIVE_COMMIT, false);
    Map<String, String> online = Map.of(SERVER_A, SegmentStateModel.ONLINE, SERVER_B, SegmentStateModel.ONLINE,
        SERVER_C, SegmentStateModel.ONLINE);
    _idealState.setInstanceStateMap(done.metadata().getSegmentName(), online);
    done = new SegmentSnapshot(done.metadata(), online, null, RecoveryStatus.NO_ACTIVE_COMMIT, false, false);
    SegmentSnapshot next = snapshot(0, 1, Status.IN_PROGRESS, SegmentStateModel.CONSUMING, null,
        RecoveryStatus.NO_ACTIVE_COMMIT, false);

    assertTrue(plan(List.of(done, next)).isEmpty());
  }

  @DataProvider
  public Object[][] replicationChanges() {
    return new Object[][]{{Status.IN_PROGRESS, 3, 2}, {Status.IN_PROGRESS, 2, 3},
        {Status.COMMITTING, 3, 2}, {Status.COMMITTING, 2, 3}};
  }

  @Test(dataProvider = "replicationChanges")
  public void testRecoveryUsesCurrentCandidatesAfterReplicationChange(Status status, int oldReplicas,
      int currentReplicas) {
    SegmentSnapshot failed = snapshot(0, 0, status,
        status == Status.COMMITTING ? SegmentStateModel.ONLINE : SegmentStateModel.CONSUMING,
        FailureReason.INSTANCE_NOT_LIVE, RecoveryStatus.COMMIT_EXPIRED, true);
    failed.metadata().setNumReplicas(oldReplicas);
    List<String> currentCandidates = SERVERS.subList(0, currentReplicas);
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_A, currentCandidates);

    SegmentRecoveryPlan action = onlyAction(failed);

    assertEquals(action.action(), status == Status.COMMITTING
        ? RecoveryAction.REINGEST_COMMITTING : RecoveryAction.REASSIGN_CONSUMING);
    assertEquals(action.targetServer(), SERVER_B);
    assertNull(action.blockedReason());
    assertEquals(failed.metadata().getNumReplicas(), oldReplicas);
  }

  @Test
  public void testHistoricalDoneAssignmentIsNotRepairedUsingOldReplicaCount() {
    SegmentSnapshot done = snapshot(0, 0, Status.DONE, SegmentStateModel.ONLINE, null,
        RecoveryStatus.NO_ACTIVE_COMMIT, true);
    Map<String, String> rebalanced = Map.of(SERVER_A, SegmentStateModel.ONLINE, SERVER_B, SegmentStateModel.ONLINE);
    _idealState.setInstanceStateMap(done.metadata().getSegmentName(), rebalanced);
    done = new SegmentSnapshot(done.metadata(), rebalanced, null, RecoveryStatus.NO_ACTIVE_COMMIT, true, false);

    assertTrue(plan(List.of(done)).isEmpty());
  }

  @Test
  public void testNoEligibleFollowerProducesBlockedPlan() {
    SegmentSnapshot failed = failedConsuming(0, 0);
    _liveInstances.clear();

    SegmentRecoveryPlan action = onlyAction(failed);

    assertEquals(action.action(), RecoveryAction.BLOCKED);
    assertEquals(action.blockedReason(), BlockedReason.NO_ELIGIBLE_REPLICA);
    assertNull(action.targetServer());
    assertEquals(action.startOffset(), "0");
  }

  @Test
  public void testSingleReplicaCannotInventTakeoverCandidate() {
    SegmentSnapshot failed = failedConsuming(0, 0);
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_A, List.of(SERVER_A));
    failed.metadata().setNumReplicas(1);

    SegmentRecoveryPlan action = onlyAction(failed);

    assertEquals(action.action(), RecoveryAction.BLOCKED);
    assertEquals(action.blockedReason(), BlockedReason.NO_ELIGIBLE_REPLICA);
    assertNull(action.targetServer());
  }

  @DataProvider
  public Object[][] invalidObservations() {
    return new Object[][]{
        {"wrongRole", BlockedReason.INVALID_ASSIGNMENT},
        {"multipleHosts", BlockedReason.INVALID_ASSIGNMENT},
        {"multipleConsuming", BlockedReason.MULTIPLE_CONSUMING_SEGMENTS},
        {"duplicateSequence", BlockedReason.DUPLICATE_SEGMENT_SEQUENCE},
        {"superseded", BlockedReason.SUPERSEDED_SEGMENT},
        {"missingStart", BlockedReason.INVALID_OFFSETS},
        {"missingLeader", BlockedReason.INVALID_ROLE_METADATA},
        {"duplicateCandidates", BlockedReason.INVALID_ROLE_METADATA}
    };
  }

  @Test(dataProvider = "invalidObservations")
  public void testUnsafeConsumptionObservationsAreBlocked(String invalidity, BlockedReason expectedReason) {
    SegmentSnapshot failed = failedConsuming(0, 0);
    String segment = failed.metadata().getSegmentName();
    switch (invalidity) {
      case "wrongRole" -> {
        Map<String, String> assignment = Map.of(SERVER_B, SegmentStateModel.CONSUMING);
        _idealState.setInstanceStateMap(segment, assignment);
        failed = new SegmentSnapshot(failed.metadata(), assignment, FailureReason.STATE_TRANSITION_ERROR,
            RecoveryStatus.NO_ACTIVE_COMMIT, false, false);
      }
      case "multipleHosts" -> {
        Map<String, String> assignment = Map.of(SERVER_A, SegmentStateModel.CONSUMING,
            SERVER_B, SegmentStateModel.CONSUMING);
        _idealState.setInstanceStateMap(segment, assignment);
        failed = new SegmentSnapshot(failed.metadata(), assignment, FailureReason.INSTANCE_NOT_LIVE,
            RecoveryStatus.NO_ACTIVE_COMMIT, false, false);
      }
      case "multipleConsuming" -> _idealState.setInstanceStateMap(segmentName(0, 1),
          Map.of(SERVER_A, SegmentStateModel.CONSUMING));
      case "duplicateSequence" -> _idealState.setInstanceStateMap(
          new LLCSegmentName("testTable", 0, 0, 61_000L).getSegmentName(), Map.of(SERVER_B, SegmentStateModel.ONLINE));
      case "superseded" -> {
        Map<String, String> assignment = Map.of(SERVER_A, SegmentStateModel.OFFLINE);
        _idealState.setInstanceStateMap(segment, assignment);
        _idealState.setInstanceStateMap(segmentName(0, 1), Map.of(SERVER_B, SegmentStateModel.ONLINE));
        failed = new SegmentSnapshot(failed.metadata(), assignment, FailureReason.LEADER_OFFLINE,
            RecoveryStatus.NO_ACTIVE_COMMIT, false, false);
      }
      case "missingStart" -> failed.metadata().setStartOffset(" ");
      case "missingLeader" -> _idealState.getRecord().getSimpleFields().remove("replicateMode.leader.0");
      case "duplicateCandidates" -> _idealState.getRecord().setListField("replicateMode.replicas.0",
          List.of(SERVER_A, SERVER_B, SERVER_B));
      default -> throw new IllegalArgumentException(invalidity);
    }

    SegmentRecoveryPlan action = onlyAction(failed);

    assertEquals(action.action(), RecoveryAction.BLOCKED);
    assertEquals(action.blockedReason(), expectedReason);
    assertNull(action.targetServer());
  }

  @DataProvider
  public Object[][] unsafeCommittingMetadata() {
    return new Object[][]{{"missingEnd", BlockedReason.INVALID_OFFSETS},
        {"artifactPresent", BlockedReason.COMMITTING_ARTIFACT_PRESENT}};
  }

  @Test(dataProvider = "unsafeCommittingMetadata")
  public void testCommittingReingestionPreservesUncertainArtifactsAndRequiresFixedRange(String invalidity,
      BlockedReason expectedReason) {
    SegmentSnapshot committing = snapshot(0, 0, Status.COMMITTING, SegmentStateModel.ONLINE,
        FailureReason.INSTANCE_NOT_LIVE, RecoveryStatus.COMMIT_EXPIRED, true);
    if (invalidity.equals("missingEnd")) {
      committing.metadata().setEndOffset(" ");
    } else {
      committing.metadata().setDownloadUrl("file:///segments/committing.tar.gz");
    }

    SegmentRecoveryPlan action = onlyAction(committing);

    assertEquals(action.action(), RecoveryAction.BLOCKED);
    assertEquals(action.blockedReason(), expectedReason);
    assertNull(action.targetServer());
  }

  @DataProvider
  public Object[][] unsupportedTables() {
    return new Object[][]{{"offline"}, {"fullUpsert"}, {"partialUpsert"}, {"dedup"}};
  }

  @Test(dataProvider = "unsupportedTables")
  public void testUnsupportedTablesAreBlocked(String type) {
    SegmentSnapshot failed = failedConsuming(0, 0);
    switch (type) {
      case "offline" -> _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
          .setServerTenant("tenant").setNumReplicas(3).build();
      case "fullUpsert" -> _tableConfig.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL));
      case "partialUpsert" -> _tableConfig.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.PARTIAL));
      case "dedup" -> _tableConfig.setDedupConfig(new DedupConfig());
      default -> throw new IllegalArgumentException(type);
    }

    SegmentRecoveryPlan action = onlyAction(failed);

    assertEquals(action.action(), RecoveryAction.BLOCKED);
    assertEquals(action.blockedReason(), BlockedReason.UNSUPPORTED_TABLE);
  }

  @DataProvider
  public Object[][] intentionalStops() {
    return new Object[][]{{"disabled"}, {"paused"}, {"topicPaused"}, {"modeDisabled"}};
  }

  @Test(dataProvider = "intentionalStops")
  public void testIntentionalStopsAndOrdinarySegmentsAreIgnored(String stop)
      throws Exception {
    SegmentSnapshot failed = failedConsuming(0, 0);
    switch (stop) {
      case "disabled" -> _idealState.enable(false);
      case "paused", "topicPaused" -> {
        boolean paused = stop.equals("paused");
        PauseState state = new PauseState(paused, PauseState.ReasonCode.ADMINISTRATIVE, null, null,
            paused ? List.of() : List.of(0));
        _idealState.getRecord().setSimpleField(PinotLLCRealtimeSegmentManager.PAUSE_STATE, state.toJsonString());
      }
      case "modeDisabled" -> failed.metadata().setReplicateMode(false);
      default -> throw new IllegalArgumentException(stop);
    }

    assertTrue(plan(List.of(failed)).isEmpty());
  }

  @Test
  public void testFollowerWithExplicitErrorIsExcludedButUnassignedFollowersNeedNoExternalView() {
    SegmentSnapshot failed = failedConsuming(0, 0);
    ExternalView externalView = new ExternalView(TABLE);
    externalView.setStateMap(failed.metadata().getSegmentName(), Map.of(SERVER_B, SegmentStateModel.ERROR));
    Map<String, String> originalStates = Map.copyOf(externalView.getStateMap(failed.metadata().getSegmentName()));

    Map<Integer, PartitionRecoveryPlan> plans = _planner.plan(context(), List.of(failed), externalView, NOW_MS);

    assertEquals(action(plans, failed).targetServer(), SERVER_C);
    assertEquals(plans.get(0).eligibleServers(), List.of(SERVER_C));
    assertTrue(plans.get(0).rejectedServers().containsKey(SERVER_B));
    assertEquals(externalView.getStateMap(failed.metadata().getSegmentName()), originalStates);
    assertEquals(onlyAction(failed).targetServer(), SERVER_B);
  }

  @Test
  public void testPlanAndSnapshotCollectionsAreImmutable() {
    SegmentSnapshot failed = failedConsuming(0, 0);
    Map<Integer, PartitionRecoveryPlan> plans = plan(List.of(failed));
    PartitionRecoveryPlan partition = plans.get(0);

    assertThrows(UnsupportedOperationException.class, plans::clear);
    assertThrows(UnsupportedOperationException.class, () -> partition.replicaCandidates().add("anotherServer"));
    assertThrows(UnsupportedOperationException.class, () -> partition.eligibleServers().add(SERVER_A));
    assertThrows(UnsupportedOperationException.class, () -> partition.rejectedServers().put(SERVER_B, "rejected"));
    assertThrows(UnsupportedOperationException.class, () -> partition.segments().clear());
    assertThrows(UnsupportedOperationException.class, () -> failed.assignment().clear());
  }

  private SegmentSnapshot failedConsuming(int partitionId, int sequence) {
    return snapshot(partitionId, sequence, Status.IN_PROGRESS, SegmentStateModel.CONSUMING,
        FailureReason.INSTANCE_NOT_LIVE, RecoveryStatus.NO_ACTIVE_COMMIT, false);
  }

  private SegmentSnapshot snapshot(int partitionId, int sequence, Status status, String assignmentState,
      FailureReason failureReason, RecoveryStatus completionStatus, boolean completionTimedOut) {
    if (ReplicateModeAssignmentUtils.getLeader(_idealState, partitionId) == null) {
      ReplicateModeAssignmentUtils.setAssignment(_idealState, partitionId, SERVER_A, SERVERS);
    }
    String segment = segmentName(partitionId, sequence);
    SegmentZKMetadata metadata = new SegmentZKMetadata(segment);
    metadata.setReplicateMode(true);
    metadata.setNumReplicas(3);
    metadata.setStatus(status);
    metadata.setStartOffset("0");
    if (status == Status.COMMITTING || status == Status.DONE) {
      metadata.setEndOffset("10");
    }
    Map<String, String> assignment = Map.of(SERVER_A, assignmentState);
    _idealState.setInstanceStateMap(segment, assignment);
    return new SegmentSnapshot(metadata, assignment, failureReason, completionStatus, completionTimedOut,
        status == Status.DONE);
  }

  private CandidateContext context() {
    return new CandidateContext(_tableConfig, _idealState, _instancePartitions, _instanceConfigs, _liveInstances);
  }

  private Map<Integer, PartitionRecoveryPlan> plan(List<SegmentSnapshot> snapshots) {
    return _planner.plan(context(), snapshots, null, NOW_MS);
  }

  private SegmentRecoveryPlan onlyAction(SegmentSnapshot snapshot) {
    Map<Integer, PartitionRecoveryPlan> plans = plan(List.of(snapshot));
    int partitionId = new LLCSegmentName(snapshot.metadata().getSegmentName()).getPartitionGroupId();
    assertEquals(plans.size(), 1);
    assertEquals(plans.get(partitionId).segments().size(), 1);
    return action(plans, snapshot);
  }

  private static SegmentRecoveryPlan action(Map<Integer, PartitionRecoveryPlan> plans, SegmentSnapshot snapshot) {
    int partitionId = new LLCSegmentName(snapshot.metadata().getSegmentName()).getPartitionGroupId();
    return plans.get(partitionId).segments().get(snapshot.metadata().getSegmentName());
  }

  private static String segmentName(int partitionId, int sequence) {
    return new LLCSegmentName("testTable", partitionId, sequence, 1000L).getSegmentName();
  }
}
