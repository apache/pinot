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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.CandidateContext;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.FailureReason;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.PartitionRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.RecoveryAction;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentSnapshot;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Executes bounded recovery work after revalidating each plan against current metadata and assignment.
/// The scheduler serializes calls; only the version-checked IdealState callback mutates role and assignment intent.
/// Commit admission and local handoffs share a segment lock. Remote commits remain safe through immutable artifact
/// publication and the native metadata CAS. Retry bookkeeping is reconstructible and is cleared on leadership loss.
/// The persisted handoff timestamp is a cooldown policy, not a new ownership identity or query-readiness barrier.
final class ReplicateModeRecoveryExecutor {
  static final String LAST_HANDOFF_PREFIX = "replicateMode.lastHandoff.";
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateModeRecoveryExecutor.class);
  private static final long RETRY_INITIAL_DELAY_MS = 10_000L;
  private static final int REINGESTION_SUBMISSION_TIMEOUT_MS = 5000;

  private final ControllerConf _config;
  private final PinotHelixResourceManager _resourceManager;
  private final LeadControllerManager _leadControllerManager;
  private final PinotLLCRealtimeSegmentManager _segmentManager;
  private final SegmentCompletionManager _completionManager;
  private final ControllerMetrics _metrics;
  private final Clock _clock;
  private final ReplicateModeRecoveryPlanner _planner;
  private final Map<String, Map<String, Retry>> _retries = new HashMap<>();

  ReplicateModeRecoveryExecutor(ControllerConf config, PinotHelixResourceManager resourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager segmentManager,
      SegmentCompletionManager completionManager, ControllerMetrics metrics, Clock clock) {
    _config = config;
    _resourceManager = resourceManager;
    _leadControllerManager = leadControllerManager;
    _segmentManager = segmentManager;
    _completionManager = completionManager;
    _metrics = metrics;
    _clock = clock;
    _planner = new ReplicateModeRecoveryPlanner(config);
  }

  void clear(String table) {
    _retries.remove(table);
  }

  void clear() {
    _retries.clear();
  }

  void execute(String table, Map<Integer, PartitionRecoveryPlan> plans, BooleanSupplier running) {
    Set<String> pending = new HashSet<>();
    List<Work> work = new ArrayList<>();
    plans.values().forEach(partition -> partition.segments().values().forEach(segment -> {
      pending.add(segment.segmentName());
      if (segment.action() != RecoveryAction.WAIT_FOR_COMPLETION && segment.action() != RecoveryAction.BLOCKED) {
        work.add(new Work(partition, segment));
      }
    }));
    Map<String, Retry> retries = _retries.computeIfAbsent(table, ignored -> new HashMap<>());
    retries.keySet().retainAll(pending);
    // First restore live consumption, then repair completion, then start bounded fixed-range background work.
    work.sort(Comparator.comparingInt((Work entry) -> entry.segment().action().ordinal())
        .thenComparingInt(entry -> entry.partition().partitionId())
        .thenComparing(entry -> entry.segment().segmentName()));
    int remaining = _config.getReplicateModeRecoveryMaxActionsPerTable();
    boolean repairedCompletion = false;
    for (Work entry : work) {
      if (!authorized(table, running) || remaining == 0) {
        break;
      }
      SegmentRecoveryPlan segment = entry.segment();
      long nowMs = _clock.millis();
      Retry retry = retries.get(segment.segmentName());
      if (retry != null && nowMs < retry.nextAttemptMs()) {
        continue;
      }
      remaining--;
      try {
        if (segment.action() == RecoveryAction.REPAIR_COMPLETION) {
          if (!repairedCompletion && validateCompletionRepair(table, segment, running)) {
            _segmentManager.repairCompletedReplicateModeSegments(table);
            TableConfig config = _resourceManager.getTableConfig(table);
            if (config != null && authorized(table, running)) {
              _segmentManager.ensureAllPartitionsConsuming(config, IngestionConfigUtils.getStreamConfigs(config), null);
            }
            repairedCompletion = true;
          }
        } else {
          String target = _completionManager.withSegmentCompletionLock(segment.segmentName(),
              () -> prepare(table, entry, running));
          if (target == null) {
            continue;
          }
          if (segment.action() == RecoveryAction.REASSIGN_CONSUMING) {
            _metrics.addMeteredTableValue(table, ControllerMeter.REPLICATE_MODE_HANDOFFS, 1);
            LOGGER.info("Reassigned replicate-mode segment {} in table {} to {}", segment.segmentName(), table, target);
          } else if (authorized(table, running)) {
            // A fresh target may not have initialized its table manager yet. A 404 or an in-progress 409 is retried.
            String endpoint = _resourceManager.getDataInstanceAdminEndpoints(Set.of(target)).get(target);
            if (endpoint == null) {
              throw new IllegalStateException("No Server admin endpoint for " + target);
            }
            _segmentManager.triggerReingestion(endpoint, segment.segmentName(), REINGESTION_SUBMISSION_TIMEOUT_MS);
            _metrics.addMeteredTableValue(table, ControllerMeter.REPLICATE_MODE_RECOVERY_REQUESTS, 1);
            LOGGER.info("Requested fixed-range recovery of {} in table {} on {}", segment.segmentName(), table, target);
          }
        }
        scheduleRetry(retries, segment.segmentName(), nowMs, retry);
      } catch (Exception e) {
        scheduleRetry(retries, segment.segmentName(), nowMs, retry);
        _metrics.addMeteredTableValue(table, ControllerMeter.REPLICATE_MODE_RECOVERY_FAILURES, 1);
        LOGGER.warn("Replicate-mode recovery will retry segment {} in table {}", segment.segmentName(), table, e);
      }
    }
    if (retries.isEmpty()) {
      _retries.remove(table);
    }
  }

  @Nullable
  private String prepare(String table, Work work, BooleanSupplier running) {
    String[] selected = {null};
    HelixHelper.updateIdealState(_resourceManager.getHelixZkManager(), table, idealState -> {
      selected[0] = null; // The updater can run again after a version conflict.
      PartitionRecoveryPlan expected = work.partition();
      SegmentRecoveryPlan wanted = work.segment();
      if (!authorized(table, running) || idealState == null || !idealState.isEnabled()
          || PinotLLCRealtimeSegmentManager.isTablePaused(idealState)
          || PinotLLCRealtimeSegmentManager.isTopicPaused(idealState, wanted.segmentName())
          || !Objects.equals(expected.expectedLeader(),
              ReplicateModeAssignmentUtils.getLeader(idealState, expected.partitionId()))
          || !expected.replicaCandidates().equals(
              ReplicateModeAssignmentUtils.getReplicas(idealState, expected.partitionId()))) {
        return idealState;
      }
      long nowMs = _clock.millis();
      if (wanted.action() == RecoveryAction.REASSIGN_CONSUMING
          && inCooldown(idealState, expected.partitionId(), nowMs)) {
        return idealState;
      }
      Stat metadataStat = new Stat();
      SegmentZKMetadata metadata =
          _segmentManager.getSegmentZKMetadataIfExists(table, wanted.segmentName(), metadataStat);
      TableConfig tableConfig = _resourceManager.getTableConfig(table);
      if (metadata == null || tableConfig == null || !metadata.isReplicateMode()
          || metadata.getStatus() != wanted.status()
          || !Objects.equals(metadata.getStartOffset(), wanted.startOffset())
          || !Objects.equals(metadata.getEndOffset(), wanted.endOffset())) {
        return idealState;
      }
      Map<String, String> assignment = idealState.getInstanceStateMap(wanted.segmentName());
      if (assignment == null) {
        return idealState;
      }
      Set<String> live = Set.copyOf(_resourceManager.getAllLiveInstances());
      ExternalView externalView = _resourceManager.getTableExternalView(table);
      FailureReason failure = ReplicateModeRecoveryPlanner.getFailureReason(assignment, externalView,
          wanted.segmentName(), () -> live);
      SegmentSnapshot snapshot = new SegmentSnapshot(metadata, assignment, failure,
          _completionManager.getRecoveryStatus(wanted.segmentName()),
          metadata.getStatus() == Status.COMMITTING
              && _segmentManager.isExceededMaxSegmentCompletionTime(wanted.segmentName(), metadataStat, nowMs), false);
      Map<String, InstanceConfig> configs = _resourceManager.getAllHelixInstanceConfigs().stream()
          .collect(Collectors.toMap(InstanceConfig::getInstanceName, config -> config));
      CandidateContext context = new CandidateContext(tableConfig, idealState,
          _segmentManager.getConsumingInstancePartitions(tableConfig), configs, live);
      PartitionRecoveryPlan current = _planner.plan(context, List.of(snapshot), externalView, nowMs)
          .get(expected.partitionId());
      SegmentRecoveryPlan action = current != null ? current.segments().get(wanted.segmentName()) : null;
      if (action == null || action.action() != wanted.action()
          || !Objects.equals(action.targetServer(), wanted.targetServer()) || action.targetServer() == null) {
        return idealState;
      }
      String target = action.targetServer();
      if (action.action() == RecoveryAction.REASSIGN_CONSUMING) {
        ReplicateModeAssignmentUtils.setAssignment(idealState, expected.partitionId(), target,
            expected.replicaCandidates());
        idealState.setInstanceStateMap(wanted.segmentName(),
            new HashMap<>(Map.of(target, SegmentStateModel.CONSUMING)));
        idealState.getRecord().setSimpleField(LAST_HANDOFF_PREFIX + expected.partitionId(), Long.toString(nowMs));
      } else {
        // Track before publishing the replay assignment: a fast worker can publish DONE before the next pass.
        _segmentManager.ensureReplicateModeCompletionTracked(table, wanted.segmentName());
        // ONLINE deliberately does not launch an unbounded consumer. The explicit API rebuilds exactly [start,end).
        idealState.setInstanceStateMap(wanted.segmentName(), new HashMap<>(Map.of(target, SegmentStateModel.ONLINE)));
      }
      selected[0] = target;
      return idealState;
    });
    return selected[0];
  }

  private boolean validateCompletionRepair(String table, SegmentRecoveryPlan segment, BooleanSupplier running) {
    return _completionManager.withSegmentCompletionLock(segment.segmentName(), () -> {
      if (!authorized(table, running)) {
        return false;
      }
      IdealState idealState = _resourceManager.getTableIdealState(table);
      SegmentZKMetadata metadata = _resourceManager.getSegmentZKMetadata(table, segment.segmentName());
      Map<String, String> assignment =
          idealState != null ? idealState.getInstanceStateMap(segment.segmentName()) : null;
      return idealState != null && idealState.isEnabled() && !PinotLLCRealtimeSegmentManager.isTablePaused(idealState)
          && !PinotLLCRealtimeSegmentManager.isTopicPaused(idealState, segment.segmentName())
          && metadata != null && metadata.isReplicateMode() && metadata.getStatus() == Status.DONE
          && (assignment != null && assignment.containsValue(SegmentStateModel.CONSUMING)
              || _segmentManager.getPendingSegmentCompletions(table).contains(segment.segmentName()));
    });
  }

  private boolean authorized(String table, BooleanSupplier running) {
    return running.getAsBoolean() && _leadControllerManager.isLeaderForTable(table)
        && _resourceManager.getHelixZkManager().isConnected();
  }

  private boolean inCooldown(IdealState idealState, int partition, long nowMs) {
    String value = idealState.getRecord().getSimpleField(LAST_HANDOFF_PREFIX + partition);
    if (value == null) {
      return false;
    }
    try {
      long previous = Long.parseLong(value);
      return nowMs < previous || nowMs - previous < _config.getReplicateModeFailoverCooldownMs();
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private void scheduleRetry(Map<String, Retry> retries, String segment, long nowMs, @Nullable Retry previous) {
    long delay = previous == null ? RETRY_INITIAL_DELAY_MS
        : Math.min(_config.getReplicateModeRecoveryRetryMaxDelayMs(), previous.delayMs() * 2);
    retries.put(segment, new Retry(nowMs + delay, delay));
  }

  private record Work(PartitionRecoveryPlan partition, SegmentRecoveryPlan segment) {
  }

  private record Retry(long nextAttemptMs, long delayMs) {
  }
}
