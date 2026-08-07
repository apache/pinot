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
package org.apache.pinot.query.planner.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.core.routing.LogicalTableRouteInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;


/// The `DispatchablePlanMetadata` info contains the information for dispatching a particular plan fragment.
///
/// It contains information
///
/// - extracted from [org.apache.pinot.query.planner.physical.DispatchablePlanVisitor]
/// - extracted from [org.apache.pinot.query.planner.physical.PinotDispatchPlanner]
public class DispatchablePlanMetadata implements Serializable {

  // --------------------------------------------------------------------------
  // Fields extracted with {@link DispatchablePlanVisitor}
  // --------------------------------------------------------------------------

  // Info from TableNode
  private final List<String> _scannedTables = new ArrayList<>();
  private Map<String, String> _tableOptions;

  // Info from MailboxSendNode - whether a stage is pre-partitioned by the same way the sending exchange desires
  private boolean _isPrePartitioned;

  // Info from PlanNode that requires singleton (e.g. SortNode/AggregateNode)
  private boolean _requiresSingletonInstance;

  // TODO: Change the following maps to lists

  // --------------------------------------------------------------------------
  // Fields extracted with {@link PinotDispatchPlanner}
  // --------------------------------------------------------------------------

  // The following fields are calculated in {@link WorkerManager}
  // Available for both leaf and intermediate stage
  private Map<Integer, QueryServerInstance> _workerIdToServerInstanceMap;
  private String _partitionFunction;
  // Available for leaf stage only
  // Map from workerId -> {tableType -> segments}
  private Map<Integer, Map<String, List<String>>> _workerIdToSegmentsMap;
  // Map from tableType -> segments, available when 'is_replicated' hint is set to true
  private Map<String, List<String>> _replicatedSegments;
  private TimeBoundaryInfo _timeBoundaryInfo;
  private int _partitionParallelism = 1;
  private final Map<String, Set<String>> _tableToUnavailableSegmentsMap = new HashMap<>();
  // Broker-local, never serialized: see getPartitionClassIds()
  private transient int[] _partitionClassIds;
  // Broker-local, never serialized: see getPaddedClassCandidates()
  private transient Map<Integer, Set<String>> _paddedClassCandidates;

  // Calculated in {@link MailboxAssignmentVisitor}
  // Map from workerId -> {planFragmentId -> mailboxes}
  private final Map<Integer, Map<Integer, MailboxInfos>> _workerIdToMailboxesMap = new HashMap<>();

  /// Map from workerId -> {physicalTableName -> segments} is required for logical tables.
  private Map<Integer, Map<String, List<String>>> _workerIdToTableSegmentsMap;
  private LogicalTableRouteInfo _logicalTableRouteInfo;

  public List<String> getScannedTables() {
    return _scannedTables;
  }

  public void addScannedTable(String tableName) {
    _scannedTables.add(tableName);
  }

  @Nullable
  public Map<String, String> getTableOptions() {
    return _tableOptions;
  }

  public void setTableOptions(Map<String, String> tableOptions) {
    _tableOptions = tableOptions;
  }

  // -----------------------------------------------
  // attached physical plan context.
  // -----------------------------------------------

  public Map<Integer, QueryServerInstance> getWorkerIdToServerInstanceMap() {
    return _workerIdToServerInstanceMap;
  }

  public void setWorkerIdToServerInstanceMap(Map<Integer, QueryServerInstance> workerIdToServerInstanceMap) {
    _workerIdToServerInstanceMap = workerIdToServerInstanceMap;
  }

  @Nullable
  public Map<Integer, Map<String, List<String>>> getWorkerIdToSegmentsMap() {
    return _workerIdToSegmentsMap;
  }

  public void setWorkerIdToSegmentsMap(Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    _workerIdToSegmentsMap = workerIdToSegmentsMap;
  }

  @Nullable
  public Map<String, List<String>> getReplicatedSegments() {
    return _replicatedSegments;
  }

  public void setReplicatedSegments(Map<String, List<String>> replicatedSegments) {
    _replicatedSegments = replicatedSegments;
  }

  public Map<Integer, Map<Integer, MailboxInfos>> getWorkerIdToMailboxesMap() {
    return _workerIdToMailboxesMap;
  }

  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public void setTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo) {
    _timeBoundaryInfo = timeBoundaryInfo;
  }

  public boolean isRequiresSingletonInstance() {
    return _requiresSingletonInstance;
  }

  public void setRequireSingleton(boolean newRequireInstance) {
    _requiresSingletonInstance = _requiresSingletonInstance || newRequireInstance;
  }

  public boolean isPrePartitioned() {
    return _isPrePartitioned;
  }

  public void setPrePartitioned(boolean isPrePartitioned) {
    _isPrePartitioned = isPrePartitioned;
  }

  public String getPartitionFunction() {
    return _partitionFunction;
  }

  public void setPartitionFunction(String partitionFunction) {
    _partitionFunction = partitionFunction;
  }

  public int getPartitionParallelism() {
    return _partitionParallelism;
  }

  public void setPartitionParallelism(int partitionParallelism) {
    _partitionParallelism = partitionParallelism;
  }

  /// Returns the partition classes this stage's worker ids stand for, in worker-id order, or `null` when the worker ids
  /// are not in partition-class space.
  ///
  /// A partition class is the set of partitions that share one worker: with a hinted partition size of `w`, class `j`
  /// holds every partition `p` where `p % w == j`. Across a direct (1-to-1) exchange the worker id is the only carrier
  /// of partition identity -- the wiring pairs sender worker `k` with receiver worker `k` and checks nothing about the
  /// data behind them -- so equal worker counts are no evidence that two stages agree: had one dropped its empty class
  /// 1 and the other its empty class 2, both would still have `w - 1` workers, and worker 1 would pair class 2 with
  /// class 1, losing rows with no error. `WorkerManager` therefore computes one class list per colocated group,
  /// dropping only the classes no member of the group holds data in, and shares that same array with every stage of it.
  /// A leaf stage gets one worker per entry, i.e. worker `k` handles class `[k]`; an intermediate stage with a
  /// partition parallelism of `p` gets `p` workers per entry, i.e. worker `k` handles class `[k / p]`, the same fan-out
  /// the exchange performs.
  ///
  /// `null` means the worker ids are not partition classes (e.g. a stage assigned over candidate servers, or a
  /// singleton reducer), or that the stage's group was not reduced, in which case worker `k` maps to class `k` as
  /// before.
  ///
  /// Broker-local planning state: not serialized to the servers, and must not be mutated (the same array instance is
  /// shared by every stage of the group).
  @Nullable
  public int[] getPartitionClassIds() {
    return _partitionClassIds;
  }

  public void setPartitionClassIds(@Nullable int[] partitionClassIds) {
    _partitionClassIds = partitionClassIds;
  }

  /// Returns the partition classes of [#getPartitionClassIds()] that this stage holds no data in, mapped to the servers
  /// its colocated group expects the (empty) worker of that class to be picked from, or `null` when this stage has
  /// nothing to pad. Only ever set together with [#getPartitionClassIds()], by the same producer; see
  /// `WorkerManager#assignPaddedWorker`, which is where such a worker and its candidate servers are used.
  ///
  /// Broker-local planning state, like [#getPartitionClassIds()]: neither the map nor the server sets in it must be
  /// mutated (the sets may be the ones the broker publishes its partition metadata with).
  @Nullable
  public Map<Integer, Set<String>> getPaddedClassCandidates() {
    return _paddedClassCandidates;
  }

  public void setPaddedClassCandidates(@Nullable Map<Integer, Set<String>> paddedClassCandidates) {
    _paddedClassCandidates = paddedClassCandidates;
  }

  public Map<String, Set<String>> getTableToUnavailableSegmentsMap() {
    return _tableToUnavailableSegmentsMap;
  }

  public void addUnavailableSegments(String tableName, Collection<String> unavailableSegments) {
    _tableToUnavailableSegmentsMap.computeIfAbsent(tableName, k -> new HashSet<>()).addAll(unavailableSegments);
  }

  @Nullable
  public LogicalTableRouteInfo getLogicalTableRouteInfo() {
    return _logicalTableRouteInfo;
  }

  public void setLogicalTableRouteInfo(LogicalTableRouteInfo logicalTableRouteInfo) {
    _logicalTableRouteInfo = logicalTableRouteInfo;
  }

  @Nullable
  public Map<Integer, Map<String, List<String>>> getWorkerIdToTableSegmentsMap() {
    return _workerIdToTableSegmentsMap;
  }

  public void setWorkerIdToTableSegmentsMap(
      Map<Integer, Map<String, List<String>>> workerIdToTableSegmentsMap) {
    _workerIdToTableSegmentsMap = workerIdToTableSegmentsMap;
  }
}
