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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.CandidateContext;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.Selection;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;


/// Builds immutable, advisory recovery plans from current partition observations, without executing recovery.
/// The planner does not mutate assignments, metadata, FSMs, or input snapshots. It can be used concurrently with
/// stable caller-provided inputs. A plan is not authorization: execution must revalidate metadata, leadership,
/// placement and completion state, including any old commit that legitimately finishes after planning.
final class ReplicateModeRecoveryPlanner {
  private final ReplicateModeCandidateSelector _candidateSelector;

  ReplicateModeRecoveryPlanner(ControllerConf config) {
    _candidateSelector = new ReplicateModeCandidateSelector(config);
  }

  /// Native failure facts only; absence from ExternalView alone is not a confirmed consumption failure.
  enum FailureReason {
    INSTANCE_NOT_LIVE, LEADER_OFFLINE, STATE_TRANSITION_ERROR
  }

  /// Shares native failure classification between observation and execution. The caller supplies liveness lazily;
  /// unsupported assignment states are validated separately by the planner and do not trigger a liveness read.
  @Nullable
  static FailureReason getFailureReason(Map<String, String> assignment, @Nullable ExternalView externalView,
      String segmentName, Supplier<Set<String>> liveInstances) {
    Map<String, String> actualStates = externalView != null ? externalView.getStateMap(segmentName) : null;
    for (Map.Entry<String, String> instance : assignment.entrySet()) {
      String state = instance.getValue();
      if (!SegmentStateModel.CONSUMING.equals(state) && !SegmentStateModel.ONLINE.equals(state)
          && !SegmentStateModel.OFFLINE.equals(state)) {
        continue;
      }
      if (!liveInstances.get().contains(instance.getKey())) {
        return FailureReason.INSTANCE_NOT_LIVE;
      }
      if (SegmentStateModel.OFFLINE.equals(state)) {
        return FailureReason.LEADER_OFFLINE;
      }
      if (actualStates != null && SegmentStateModel.ERROR.equals(actualStates.get(instance.getKey()))) {
        return FailureReason.STATE_TRANSITION_ERROR;
      }
    }
    return null;
  }

  /// Per-segment observations. The timeout must come from native segment-metadata timing, not failure-observation age.
  record SegmentSnapshot(SegmentZKMetadata metadata, Map<String, String> assignment,
                         @Nullable FailureReason failureReason, RecoveryStatus completionStatus,
                         boolean completionTimedOut, boolean completionPending) {
    SegmentSnapshot {
      assignment = Map.copyOf(assignment);
    }
  }

  /// Internal actions only; these are neither new Helix states nor additional persistent replica roles.
  enum RecoveryAction {
    REASSIGN_CONSUMING, WAIT_FOR_COMPLETION, REPAIR_COMPLETION, REINGEST_COMMITTING, BLOCKED
  }

  /// Reasons why a recovery action cannot currently be planned safely.
  enum BlockedReason {
    UNSUPPORTED_TABLE, INVALID_ASSIGNMENT, INVALID_ROLE_METADATA, SUPERSEDED_SEGMENT,
    MULTIPLE_CONSUMING_SEGMENTS, DUPLICATE_SEGMENT_SEQUENCE, INVALID_OFFSETS,
    COMMITTING_ARTIFACT_PRESENT,
    NO_ELIGIBLE_REPLICA
  }

  /// Immutable segment action retaining its original identity and range. A null target means no host action is ready.
  record SegmentRecoveryPlan(String segmentName, Status status, RecoveryAction action,
                             @Nullable String targetServer, @Nullable BlockedReason blockedReason,
                             @Nullable String startOffset, @Nullable String endOffset,
                             RecoveryStatus completionStatus, boolean completionTimedOut) {
  }

  /// Immutable partition plan; candidate placement is recorded for review and is never rewritten by planning.
  /// Eligible servers are the union across pending host actions; rejected servers cannot run any of those actions.
  /// Each segment retains its own target, independently of the targets chosen for other fixed recovery ranges.
  record PartitionRecoveryPlan(int partitionId, @Nullable String expectedLeader, List<String> replicaCandidates,
                               List<String> eligibleServers, Map<String, String> rejectedServers,
                               Map<String, SegmentRecoveryPlan> segments, long observedTimeMs) {
    PartitionRecoveryPlan {
      replicaCandidates = List.copyOf(replicaCandidates);
      eligibleServers = List.copyOf(eligibleServers);
      rejectedServers = Map.copyOf(rejectedServers);
      segments = Map.copyOf(segments);
    }
  }

  Map<Integer, PartitionRecoveryPlan> plan(CandidateContext context, Collection<SegmentSnapshot> snapshots,
      @Nullable ExternalView externalView, long nowMs) {
    IdealState idealState = context.idealState();
    if (!idealState.isEnabled() || PinotLLCRealtimeSegmentManager.isTablePaused(idealState)) {
      return Map.of();
    }
    Map<Integer, List<SegmentSnapshot>> byPartition = new TreeMap<>();
    for (SegmentSnapshot snapshot : snapshots) {
      LLCSegmentName name = LLCSegmentName.of(snapshot.metadata().getSegmentName());
      if (name != null && snapshot.metadata().isReplicateMode()
          && !PinotLLCRealtimeSegmentManager.isTopicPaused(idealState, name.getSegmentName())) {
        byPartition.computeIfAbsent(name.getPartitionGroupId(), ignored -> new ArrayList<>()).add(snapshot);
      }
    }

    Map<String, Integer> leaderCounts = new HashMap<>();
    ReplicateModeAssignmentUtils.getLeaders(idealState).values()
        .forEach(server -> leaderCounts.merge(server, 1, Integer::sum));
    Map<Integer, PartitionRecoveryPlan> plans = new TreeMap<>();
    Map<Integer, PartitionProgress> progress = getPartitionProgress(idealState, byPartition);
    for (Map.Entry<Integer, List<SegmentSnapshot>> entry : byPartition.entrySet()) {
      int partitionId = entry.getKey();
      String leader = ReplicateModeAssignmentUtils.getLeader(idealState, partitionId);
      List<String> replicas = ReplicateModeAssignmentUtils.getReplicas(idealState, partitionId);
      Map<String, SegmentRecoveryPlan> actions = new TreeMap<>();
      Map<String, Set<String>> excludedServersBySegment = new HashMap<>();
      for (SegmentSnapshot snapshot : entry.getValue()) {
        SegmentRecoveryPlan action = classify(context, leader, replicas, snapshot, progress.get(partitionId));
        if (action == null) {
          continue;
        }
        actions.put(action.segmentName(), action);
        if (needsTarget(action.action())) {
          excludedServersBySegment.put(action.segmentName(),
              getExcludedServers(snapshot, action.action(), externalView));
        }
      }
      if (actions.isEmpty()) {
        continue;
      }
      Set<String> eligibleServers = new TreeSet<>();
      Map<String, List<String>> rejectionReasons = new TreeMap<>();
      boolean multipleHostActions = excludedServersBySegment.size() > 1;
      String consumingTarget = null;
      for (Map.Entry<String, SegmentRecoveryPlan> actionEntry : actions.entrySet()) {
        SegmentRecoveryPlan action = actionEntry.getValue();
        if (!needsTarget(action.action())) {
          continue;
        }
        // Fixed ranges can require different workers, and one range's failure must not exclude another's worker.
        Selection selection = _candidateSelector.select(context, partitionId, List.of(action.segmentName()),
            excludedServersBySegment.get(action.segmentName()), leaderCounts, leader, nowMs);
        eligibleServers.addAll(selection.eligibleServers());
        selection.rejectedServers().forEach((server, reason) -> rejectionReasons
            .computeIfAbsent(server, ignored -> new ArrayList<>())
            .add(multipleHostActions ? action.segmentName() + ": " + reason : reason));
        String target = selection.selectedServer();
        if (target == null) {
          actionEntry.setValue(withTarget(action, RecoveryAction.BLOCKED, null, BlockedReason.NO_ELIGIBLE_REPLICA));
        } else {
          if (action.action() == RecoveryAction.REASSIGN_CONSUMING && !target.equals(leader)) {
            consumingTarget = target;
          }
          actionEntry.setValue(withTarget(action, action.action(), target, null));
        }
      }
      if (consumingTarget != null) {
        // Virtual counts balance this pass without changing the persisted role of any partition.
        // Update after selecting every range so a consuming handoff does not affect its sibling replay choices.
        leaderCounts.computeIfPresent(leader, (server, count) -> Math.max(0, count - 1));
        leaderCounts.merge(consumingTarget, 1, Integer::sum);
      }
      Map<String, String> rejectedServers = new TreeMap<>();
      rejectionReasons.forEach((server, reasons) -> {
        if (!eligibleServers.contains(server)) {
          rejectedServers.put(server, String.join("; ", reasons));
        }
      });
      plans.put(partitionId, new PartitionRecoveryPlan(partitionId, leader, replicas, List.copyOf(eligibleServers),
          rejectedServers, actions, nowMs));
    }
    return Map.copyOf(plans);
  }

  private static Set<String> getExcludedServers(SegmentSnapshot snapshot, RecoveryAction action,
      @Nullable ExternalView externalView) {
    Set<String> excluded = new HashSet<>();
    if (snapshot.failureReason() != null && (snapshot.metadata().getStatus() != Status.COMMITTING
        || snapshot.failureReason() == FailureReason.INSTANCE_NOT_LIVE)) {
      excluded.addAll(snapshot.assignment().keySet());
    }
    if (externalView != null && action != RecoveryAction.REINGEST_COMMITTING) {
      Map<String, String> actualStates = externalView.getStateMap(snapshot.metadata().getSegmentName());
      if (actualStates != null) {
        actualStates.forEach((server, state) -> {
          if (SegmentStateModel.ERROR.equals(state)) {
            excluded.add(server);
          }
        });
      }
    }
    return excluded;
  }

  @Nullable
  private static SegmentRecoveryPlan classify(CandidateContext context, @Nullable String leader,
      List<String> replicas, SegmentSnapshot snapshot, PartitionProgress progress) {
    SegmentZKMetadata metadata = snapshot.metadata();
    String segment = metadata.getSegmentName();
    Status status = metadata.getStatus();
    if (context.tableConfig().getTableType() != TableType.REALTIME || context.tableConfig().isUpsertEnabled()
        || context.tableConfig().isDedupEnabled()) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.UNSUPPORTED_TABLE);
    }
    if (leader == null || replicas.isEmpty() || !replicas.contains(leader)
        || new HashSet<>(replicas).size() != replicas.size()) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.INVALID_ROLE_METADATA);
    }
    if (status == Status.DONE) {
      boolean consuming = snapshot.assignment().containsValue(SegmentStateModel.CONSUMING);
      if (!snapshot.completionPending() && !consuming) {
        // Completed historical placement belongs to rebalance, even when its creation-time RF differs.
        return null;
      }
      if (!consuming) {
        // The durable completion entry, not the historical RF, identifies an unfinished fanout or handoff.
        return action(snapshot, RecoveryAction.REPAIR_COMPLETION, null);
      }
      if (!snapshot.completionTimedOut() || isCommitInProgress(snapshot)) {
        return action(snapshot, RecoveryAction.WAIT_FOR_COMPLETION, null);
      }
      return action(snapshot, RecoveryAction.REPAIR_COMPLETION, null);
    }
    if (status != Status.IN_PROGRESS && status != Status.COMMITTING) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.INVALID_ASSIGNMENT);
    }
    if (snapshot.assignment().size() != 1) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.INVALID_ASSIGNMENT);
    }
    String assignedState = snapshot.assignment().values().iterator().next();
    if (!SegmentStateModel.CONSUMING.equals(assignedState) && !SegmentStateModel.OFFLINE.equals(assignedState)
        && !SegmentStateModel.ONLINE.equals(assignedState)) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.INVALID_ASSIGNMENT);
    }
    if (status == Status.COMMITTING && (!snapshot.completionTimedOut()
        || snapshot.completionStatus() == RecoveryStatus.UNKNOWN)) {
      // A built-in attempt still doing I/O after the native timeout may compete with replay. Immutable publication
      // and the metadata CAS arbitrate this race across Controllers already; a local FSM must not veto it forever.
      return action(snapshot, RecoveryAction.WAIT_FOR_COMPLETION, null);
    }
    if (metadata.getStartOffset() == null || metadata.getStartOffset().isBlank()
        || status == Status.COMMITTING && (metadata.getEndOffset() == null || metadata.getEndOffset().isBlank())) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.INVALID_OFFSETS);
    }
    if (status == Status.COMMITTING) {
      if (metadata.getDownloadUrl() != null && !metadata.getDownloadUrl().isBlank()) {
        return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.COMMITTING_ARTIFACT_PRESENT);
      }
      return action(snapshot, RecoveryAction.REINGEST_COMMITTING, null);
    }
    if (snapshot.failureReason() == null) {
      return null;
    }
    String state = snapshot.assignment().get(leader);
    if (!SegmentStateModel.CONSUMING.equals(state) && !SegmentStateModel.OFFLINE.equals(state)) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.INVALID_ASSIGNMENT);
    }
    if (progress._consumingSegments > 1) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.MULTIPLE_CONSUMING_SEGMENTS);
    }
    int sequence = new LLCSegmentName(segment).getSequenceNumber();
    if (progress._observedSequenceCounts.get(sequence) > 1) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.DUPLICATE_SEGMENT_SEQUENCE);
    }
    if (progress._latestSequence > sequence) {
      return action(snapshot, RecoveryAction.BLOCKED, BlockedReason.SUPERSEDED_SEGMENT);
    }
    // A pre-existing committer may still finish; planning consumption takeover does not cancel that attempt.
    return action(snapshot, RecoveryAction.REASSIGN_CONSUMING, null);
  }

  private static boolean isCommitInProgress(SegmentSnapshot snapshot) {
    return snapshot.completionStatus() == RecoveryStatus.COMMIT_IN_PROGRESS
        || snapshot.completionStatus() == RecoveryStatus.UNKNOWN;
  }

  private static Map<Integer, PartitionProgress> getPartitionProgress(IdealState idealState,
      Map<Integer, List<SegmentSnapshot>> snapshots) {
    Map<Integer, PartitionProgress> progress = new HashMap<>();
    snapshots.forEach((partition, segments) -> {
      PartitionProgress state = new PartitionProgress();
      segments.forEach(segment -> state._observedSequenceCounts.put(
          new LLCSegmentName(segment.metadata().getSegmentName()).getSequenceNumber(), 0));
      progress.put(partition, state);
    });
    // Scan assignment names once per table, rather than rescanning all historical names for each failed partition.
    idealState.getRecord().getMapFields().forEach((segment, assignment) -> {
      LLCSegmentName name = LLCSegmentName.of(segment);
      PartitionProgress state = name != null ? progress.get(name.getPartitionGroupId()) : null;
      if (state != null) {
        int sequence = name.getSequenceNumber();
        state._latestSequence = Math.max(state._latestSequence, sequence);
        if (assignment.containsValue(SegmentStateModel.CONSUMING)) {
          state._consumingSegments++;
        }
        state._observedSequenceCounts.computeIfPresent(sequence, (key, count) -> count + 1);
      }
    });
    return progress;
  }

  /// Mutable indexing state confined to one plan invocation; only requested sequence identities are counted.
  private static final class PartitionProgress {
    private int _latestSequence = -1;
    private int _consumingSegments;
    private final Map<Integer, Integer> _observedSequenceCounts = new HashMap<>();
  }

  private static boolean needsTarget(RecoveryAction action) {
    return action == RecoveryAction.REASSIGN_CONSUMING || action == RecoveryAction.REINGEST_COMMITTING;
  }

  private static SegmentRecoveryPlan action(SegmentSnapshot snapshot, RecoveryAction action,
      @Nullable BlockedReason reason) {
    SegmentZKMetadata metadata = snapshot.metadata();
    return new SegmentRecoveryPlan(metadata.getSegmentName(), metadata.getStatus(), action, null, reason,
        metadata.getStartOffset(), metadata.getEndOffset(), snapshot.completionStatus(), snapshot.completionTimedOut());
  }

  private static SegmentRecoveryPlan withTarget(SegmentRecoveryPlan plan, RecoveryAction action,
      @Nullable String target, @Nullable BlockedReason reason) {
    return new SegmentRecoveryPlan(plan.segmentName(), plan.status(), action, target, reason, plan.startOffset(),
        plan.endOffset(), plan.completionStatus(), plan.completionTimedOut());
  }
}
