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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.CandidateContext;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.FailureReason;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.PartitionRecoveryPlan;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeRecoveryPlanner.SegmentSnapshot;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Observes replicate-mode failures and reconciles partition recovery on the table's lead Controller.
///
/// Planning selects eligible recovery hosts; the executor revalidates each action before changing assignment intent.
/// Segment metadata is authoritative for mode and completion. Only current consumption candidates and the native
/// pending-completion list are checked. Discovery does not probe Servers over HTTP;
/// explicit fixed-range recovery submissions are bounded separately by the executor.
///
/// Periodic and on-demand executions are serialized by the parent task. Consumption-stop notifications only mark a
/// table dirty for priority processing on the next periodic pass. Pending failures are immutable, reconstructible
/// observations, not recovery authorization; the executor validates current state again before every action.
/// Concurrent notifications and snapshot reads are supported. The existing scheduler owns the task lifecycle.
public class ReplicateModeFailoverManager extends ControllerPeriodicTask<ReplicateModeFailoverManager.Context> {
  public static final String TASK_NAME = "ReplicateModeFailoverManager";
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateModeFailoverManager.class);

  private final PinotLLCRealtimeSegmentManager _segmentManager;
  private final SegmentCompletionManager _completionManager;
  private final ReplicateModeRecoveryPlanner _recoveryPlanner;
  private final ReplicateModeRecoveryExecutor _recoveryExecutor;
  private final int _maxRepairActionsPerTable;
  private final Clock _clock;
  private final Set<String> _dirtyTables = ConcurrentHashMap.newKeySet();
  private final Map<String, TableObservation> _tableObservations = new ConcurrentHashMap<>();
  private final Map<String, IdealStateVersion> _tablesWithoutRoles = new ConcurrentHashMap<>();

  public ReplicateModeFailoverManager(ControllerConf config, PinotHelixResourceManager resourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      PinotLLCRealtimeSegmentManager segmentManager, SegmentCompletionManager completionManager) {
    this(config, resourceManager, leadControllerManager, controllerMetrics, segmentManager, completionManager,
        Clock.systemUTC());
  }

  @VisibleForTesting
  ReplicateModeFailoverManager(ControllerConf config, PinotHelixResourceManager resourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      PinotLLCRealtimeSegmentManager segmentManager, SegmentCompletionManager completionManager, Clock clock) {
    this(config, resourceManager, leadControllerManager, controllerMetrics, segmentManager, completionManager, clock,
        new ReplicateModeRecoveryExecutor(config, resourceManager, leadControllerManager, segmentManager,
            completionManager, controllerMetrics, clock));
  }

  @VisibleForTesting
  ReplicateModeFailoverManager(ControllerConf config, PinotHelixResourceManager resourceManager,
      LeadControllerManager leadControllerManager, ControllerMetrics controllerMetrics,
      PinotLLCRealtimeSegmentManager segmentManager, SegmentCompletionManager completionManager, Clock clock,
      ReplicateModeRecoveryExecutor recoveryExecutor) {
    super(TASK_NAME, config.getReplicateModeFailoverFrequencyInSeconds(),
        config.getReplicateModeFailoverInitialDelayInSeconds(), config.getReplicateModeFailoverCronExpression(),
        resourceManager, leadControllerManager, controllerMetrics);
    _segmentManager = segmentManager;
    _completionManager = completionManager;
    _recoveryPlanner = new ReplicateModeRecoveryPlanner(config);
    _recoveryExecutor = recoveryExecutor;
    _maxRepairActionsPerTable = config.getReplicateModeRecoveryMaxActionsPerTable();
    _clock = clock;
  }

  /// Marks a table for priority inspection. This callback performs no metadata reads or recovery work.
  public void requestReconciliation(String tableNameWithType) {
    if (isStarted() && TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      _dirtyTables.add(tableNameWithType);
    }
  }

  /// Returns an immutable observation snapshot. These observations must not be used without revalidation to recover.
  @VisibleForTesting
  Map<String, PendingFailure> getPendingFailures(String tableNameWithType) {
    TableObservation observation = _tableObservations.get(tableNameWithType);
    return observation != null ? observation.failures() : Map.of();
  }

  /// Returns the latest observed plans. Execution may already have advanced; plans are not recovery authorization.
  @VisibleForTesting
  Map<Integer, PartitionRecoveryPlan> getRecoveryPlans(String tableNameWithType) {
    TableObservation observation = _tableObservations.get(tableNameWithType);
    return observation != null ? observation.plans() : Map.of();
  }

  @Override
  protected void setUpTask() {
    _segmentManager.setConsumptionStoppedListener(this::requestReconciliation);
  }

  @Override
  protected void cleanUpTask() {
    _segmentManager.setConsumptionStoppedListener(null);
    _dirtyTables.clear();
    _tableObservations.clear();
    _tablesWithoutRoles.clear();
    _recoveryExecutor.clear();
  }

  @Override
  protected List<String> getTablesToProcess(Properties properties) {
    // The parent does not call postprocess when this Controller leads no tables, including before the first run.
    clearNonLeaderObservations();
    List<String> tables = super.getTablesToProcess(properties);
    if (properties.getProperty(PeriodicTask.PROPERTY_KEY_TABLE_NAME) != null) {
      return tables;
    }
    Set<String> orderedTables = new LinkedHashSet<>(new TreeSet<>(_dirtyTables));
    orderedTables.addAll(tables);
    // Revisit observations of deleted tables so their state is removed even if they left the table catalog.
    orderedTables.addAll(new TreeSet<>(_tableObservations.keySet()));
    orderedTables.addAll(new TreeSet<>(_tablesWithoutRoles.keySet()));
    return List.copyOf(orderedTables);
  }

  @Override
  protected boolean shouldProcessTable(String tableNameWithType) {
    return TableNameBuilder.isRealtimeTableResource(tableNameWithType) && super.shouldProcessTable(tableNameWithType);
  }

  @Override
  protected Context preprocess(Properties properties) {
    // Most clusters have no replicate-mode tables. Read liveness at most once, and only when a candidate needs it.
    Supplier<Set<String>> liveInstances = Suppliers.memoize(
        () -> Set.copyOf(_pinotHelixResourceManager.getAllLiveInstances()));
    Supplier<Map<String, InstanceConfig>> instanceConfigs = Suppliers.memoize(
        () -> _pinotHelixResourceManager.getAllHelixInstanceConfigs().stream()
            .collect(Collectors.toUnmodifiableMap(InstanceConfig::getInstanceName, config -> config)));
    return new Context(_clock.millis(), liveInstances, instanceConfigs);
  }

  @Override
  protected void processTables(List<String> tableNamesWithType, Properties properties) {
    // The parent filters the catalog through a Set, so prioritize after leadership filtering as well.
    Set<String> dirtyTables = Set.copyOf(_dirtyTables);
    List<String> orderedTables = new ArrayList<>(tableNamesWithType);
    orderedTables.sort(Comparator.comparing((String table) -> !dirtyTables.contains(table))
        .thenComparing(Comparator.naturalOrder()));
    super.processTables(orderedTables, properties);
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    // Notifications arriving during inspection remain dirty for the next pass.
    boolean dirty = _dirtyTables.remove(tableNameWithType);
    // Ordinary tables can have very large assignments. Probe only the znode stat while a negative role observation
    // is unchanged. Read the stat BEFORE the full IS, so a concurrent role addition cannot be cached as absent.
    String path = PropertyPathBuilder.idealState(_pinotHelixResourceManager.getHelixClusterName(), tableNameWithType);
    Stat stat = _pinotHelixResourceManager.getZKStat(path);
    IdealStateVersion version = stat != null ? new IdealStateVersion(stat.getCzxid(), stat.getVersion()) : null;
    if (!dirty && version != null && version.equals(_tablesWithoutRoles.get(tableNameWithType))) {
      return;
    }
    _tablesWithoutRoles.remove(tableNameWithType);
    IdealState idealState = _pinotHelixResourceManager.getTableIdealState(tableNameWithType);
    if (idealState == null || !idealState.isEnabled() || PinotLLCRealtimeSegmentManager.isTablePaused(idealState)) {
      clearTableRecovery(tableNameWithType);
      return;
    }
    int repairs = 0;
    for (Map.Entry<String, String> restart : ReplicateModeAssignmentUtils.getPendingRestarts(idealState).entrySet()) {
      if (!isStarted() || !_leadControllerManager.isLeaderForTable(tableNameWithType)
          || repairs++ >= _maxRepairActionsPerTable) {
        break;
      }
      try {
        _segmentManager.restartReplicateModeConsumer(tableNameWithType, restart.getKey(), restart.getValue());
      } catch (Exception e) {
        LOGGER.warn("Will retry manual-rebalance consumer repair for {} in table {}", restart.getKey(),
            tableNameWithType, e);
      }
    }
    Map<Integer, String> leaders = ReplicateModeAssignmentUtils.getLeaders(idealState);
    if (leaders.isEmpty()) {
      clearTableRecovery(tableNameWithType);
      if (version != null && isStarted() && _leadControllerManager.isLeaderForTable(tableNameWithType)) {
        _tablesWithoutRoles.put(tableNameWithType, version);
      }
      return;
    }

    Set<String> candidates = getSegmentsToInspect(idealState, leaders);
    Set<String> pendingCompletions = new HashSet<>(_segmentManager.getPendingSegmentCompletions(tableNameWithType));
    candidates.addAll(pendingCompletions);
    Map<Integer, PartitionRecoveryPlan> previousPlans = getRecoveryPlans(tableNameWithType);
    // Keep following a known completion window even if its Server becomes healthy again in the meantime.
    previousPlans.values().forEach(plan -> candidates.addAll(plan.segments().keySet()));
    ExternalView externalView = _pinotHelixResourceManager.getTableExternalView(tableNameWithType);
    Map<String, PendingFailure> previous = getPendingFailures(tableNameWithType);
    Set<String> plannedSegments = new HashSet<>();
    previousPlans.values().forEach(plan -> plannedSegments.addAll(plan.segments().keySet()));
    Map<String, PendingFailure> observed = new HashMap<>();
    List<SegmentSnapshot> snapshots = new ArrayList<>();
    for (String segmentName : candidates) {
      if (!isStarted()) {
        return;
      }
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName == null || !leaders.containsKey(llcSegmentName.getPartitionGroupId())
          || PinotLLCRealtimeSegmentManager.isTopicPaused(idealState, segmentName)) {
        continue;
      }
      Map<String, String> assignment = idealState.getInstanceStateMap(segmentName);
      Map<String, String> states = assignment != null ? assignment : Map.of();
      FailureReason reason = ReplicateModeRecoveryPlanner.getFailureReason(states, externalView, segmentName,
          context._liveInstances);
      if (reason == null && !plannedSegments.contains(segmentName) && !pendingCompletions.contains(segmentName)) {
        continue;
      }

      Stat metadataStat = new Stat();
      SegmentZKMetadata metadata =
          _segmentManager.getSegmentZKMetadataIfExists(tableNameWithType, segmentName, metadataStat);
      if (metadata == null || !metadata.isReplicateMode()) {
        continue;
      }
      Status status = metadata.getStatus();
      int partitionId = llcSegmentName.getPartitionGroupId();
      RecoveryStatus completionStatus = _completionManager.getRecoveryStatus(segmentName);
      boolean completionTimedOut = (status == Status.COMMITTING || status == Status.DONE)
          && _segmentManager.isExceededMaxSegmentCompletionTime(segmentName, metadataStat, context._nowMs);
      snapshots.add(new SegmentSnapshot(metadata, states, reason, completionStatus, completionTimedOut,
          pendingCompletions.contains(segmentName)));
      // Preserve the original failure-observation API: plans may also cover completed or inconsistent assignments.
      if (reason == null || states.size() != 1 || status != Status.IN_PROGRESS && status != Status.COMMITTING) {
        continue;
      }
      Map.Entry<String, String> assignedInstance = states.entrySet().iterator().next();
      String server = assignedInstance.getKey();
      if (status == Status.IN_PROGRESS && (!server.equals(leaders.get(partitionId))
          || SegmentStateModel.ONLINE.equals(assignedInstance.getValue()))) {
        continue;
      }
      // An older COMMITTING segment can still belong to A after B leads the newer consuming segment.
      PendingFailure oldFailure = previous.get(segmentName);
      boolean sameFailure = oldFailure != null && oldFailure.serverName().equals(server)
          && oldFailure.status() == status && oldFailure.reason() == reason;
      long firstObserved = sameFailure ? oldFailure.firstObservedTimeMs() : context._nowMs;
      PendingFailure failure = new PendingFailure(partitionId, segmentName, server, status, reason, firstObserved,
          context._nowMs);
      observed.put(segmentName, failure);
      if (!sameFailure) {
        LOGGER.warn("Observed unfinished replicate-mode segment failure in table {}: {}", tableNameWithType, failure);
      }
    }

    Map<Integer, PartitionRecoveryPlan> plans = Map.of();
    if (!snapshots.isEmpty()) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig == null) {
        clearTableRecovery(tableNameWithType);
        return;
      }
      CandidateContext candidatesContext = new CandidateContext(tableConfig, idealState,
          _segmentManager.getConsumingInstancePartitions(tableConfig), context._instanceConfigs.get(),
          context._liveInstances.get());
      plans = _recoveryPlanner.plan(candidatesContext, snapshots, externalView, context._nowMs);
    }
    if (!isStarted() || !_leadControllerManager.isLeaderForTable(tableNameWithType)
        || observed.isEmpty() && plans.isEmpty()) {
      _tableObservations.remove(tableNameWithType);
    } else {
      plans.forEach((partition, plan) -> {
        PartitionRecoveryPlan oldPlan = previousPlans.get(partition);
        if (oldPlan == null || !oldPlan.segments().equals(plan.segments())
            || !oldPlan.eligibleServers().equals(plan.eligibleServers())
            || !oldPlan.rejectedServers().equals(plan.rejectedServers())) {
          LOGGER.info("Replicate-mode recovery plan for table {}: {}", tableNameWithType, plan);
        }
      });
      _tableObservations.put(tableNameWithType, new TableObservation(observed, plans));
    }
    updateRecoveryGauges(tableNameWithType, plans);
    if (isStarted() && _leadControllerManager.isLeaderForTable(tableNameWithType)) {
      _recoveryExecutor.execute(tableNameWithType, plans, this::isStarted);
    }
  }

  private void clearTableRecovery(String table) {
    _tableObservations.remove(table);
    _recoveryExecutor.clear(table);
    updateRecoveryGauges(table, Map.of());
  }

  private void updateRecoveryGauges(String table, Map<Integer, PartitionRecoveryPlan> plans) {
    long pending = plans.values().stream().mapToLong(plan -> plan.segments().size()).sum();
    long blocked = plans.values().stream().flatMap(plan -> plan.segments().values().stream())
        .filter(plan -> plan.action() == ReplicateModeRecoveryPlanner.RecoveryAction.BLOCKED).count();
    _controllerMetrics.setOrUpdateTableGauge(table, ControllerGauge.REPLICATE_MODE_PENDING_RECOVERIES, pending);
    _controllerMetrics.setOrUpdateTableGauge(table, ControllerGauge.REPLICATE_MODE_BLOCKED_RECOVERIES, blocked);
  }

  private static Set<String> getSegmentsToInspect(IdealState idealState, Map<Integer, String> leaders) {
    Set<String> segments = new HashSet<>();
    Map<Integer, LLCSegmentName> latestSegments = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      LLCSegmentName segment = LLCSegmentName.of(entry.getKey());
      if (segment == null || !leaders.containsKey(segment.getPartitionGroupId())) {
        continue;
      }
      if (entry.getValue().containsValue(SegmentStateModel.CONSUMING)) {
        segments.add(entry.getKey());
      }
      latestSegments.merge(segment.getPartitionGroupId(), segment,
          (first, second) -> first.getSequenceNumber() >= second.getSequenceNumber() ? first : second);
    }
    latestSegments.values().forEach(segment -> segments.add(segment.getSegmentName()));
    return segments;
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    // A table-scoped on-demand run can omit other tables whose leadership has not actually changed.
    clearNonLeaderObservations();
  }

  @Override
  protected void postprocess(Context context) {
    clearNonLeaderObservations();
  }

  private void clearNonLeaderObservations() {
    _tableObservations.keySet().stream().filter(table -> !_leadControllerManager.isLeaderForTable(table))
        .forEach(_recoveryExecutor::clear);
    _tableObservations.keySet().removeIf(table -> !_leadControllerManager.isLeaderForTable(table));
    _dirtyTables.removeIf(table -> !_leadControllerManager.isLeaderForTable(table));
    _tablesWithoutRoles.keySet().removeIf(table -> !_leadControllerManager.isLeaderForTable(table));
  }

  /// Includes the znode incarnation so deleting and recreating a table at version zero invalidates the cache.
  private record IdealStateVersion(long creationZxid, int version) {
  }

  /// Atomically published, immutable table observations. Neither failures nor plans authorize recovery execution.
  private record TableObservation(Map<String, PendingFailure> failures, Map<Integer, PartitionRecoveryPlan> plans) {
    private TableObservation {
      failures = Map.copyOf(failures);
      plans = Map.copyOf(plans);
    }
  }

  /// Immutable, in-memory failure observation. Timestamps describe observations, not persisted failover attempts.
  record PendingFailure(int partitionId, String segmentName, String serverName, Status status,
                        FailureReason reason, long firstObservedTimeMs, long lastObservedTimeMs) {
  }

  /// Per-pass state, confined to the task's serialized execution thread.
  protected static final class Context {
    private final long _nowMs;
    private final Supplier<Set<String>> _liveInstances;
    private final Supplier<Map<String, InstanceConfig>> _instanceConfigs;

    private Context(long nowMs, Supplier<Set<String>> liveInstances,
        Supplier<Map<String, InstanceConfig>> instanceConfigs) {
      _nowMs = nowMs;
      _liveInstances = liveInstances;
      _instanceConfigs = instanceConfigs;
    }
  }
}
