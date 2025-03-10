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
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * The {@code DispatchablePlanMetadata} info contains the information for dispatching a particular plan fragment.
 *
 * <p>It contains information
 * <ul>
 *   <li>extracted from {@link org.apache.pinot.query.planner.physical.DispatchablePlanVisitor}</li>
 *   <li>extracted from {@link org.apache.pinot.query.planner.physical.PinotDispatchPlanner}</li>
 * </ul>
 */
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

  private boolean _isLogicalTable = false;
  private List<String> _physicalTableNames;
  private Map<Integer, Map<String, Map<String, List<String>>>> _workerIdToLogicalTableSegmentsMap;

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

  // Calculated in {@link MailboxAssignmentVisitor}
  // Map from workerId -> {planFragmentId -> mailboxes}
  private final Map<Integer, Map<Integer, MailboxInfos>> _workerIdToMailboxesMap = new HashMap<>();

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

  public Map<String, Set<String>> getTableToUnavailableSegmentsMap() {
    return _tableToUnavailableSegmentsMap;
  }

  public void addUnavailableSegments(String tableName, Collection<String> unavailableSegments) {
    _tableToUnavailableSegmentsMap.computeIfAbsent(tableName, k -> new HashSet<>()).addAll(unavailableSegments);
  }

  public boolean isLogicalTable() {
    return _isLogicalTable;
  }

  public void setLogicalTable(boolean logicalTable) {
    _isLogicalTable = logicalTable;
  }

  public List<String> getPhysicalTableNames() {
    return _physicalTableNames;
  }

  public void setPhysicalTableNames(List<String> physicalTableNames) {
    _physicalTableNames = physicalTableNames;
  }

  public void setWorkerIdToLogicalTableSegmentsMap(Map<Integer, Map<String, Map<String, List<String>>>>
      workerIdToLogicalTableSegmentsMap) {
    _workerIdToLogicalTableSegmentsMap = workerIdToLogicalTableSegmentsMap;
  }

  public Map<Integer, Map<String, Map<String, List<String>>>> getWorkerIdToLogicalTableSegmentsMap() {
    return _workerIdToLogicalTableSegmentsMap;
  }
}
