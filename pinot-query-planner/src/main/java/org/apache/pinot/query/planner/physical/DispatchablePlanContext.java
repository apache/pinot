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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DispatchablePlanContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(DispatchablePlanContext.class);

  private final WorkerManager _workerManager;
  private final long _requestId;
  private final PlannerContext _plannerContext;
  private final PairList<Integer, String> _resultFields;
  private final Set<String> _tableNames;

  private final Set<String> _nonLookupTables;
  private final Set<QueryServerInstance> _leafServerInstances;

  private final Map<Integer, DispatchablePlanMetadata> _dispatchablePlanMetadataMap = new HashMap<>();
  private final Map<Integer, PlanNode> _dispatchablePlanStageRootMap = new HashMap<>();


  public DispatchablePlanContext(WorkerManager workerManager, long requestId, PlannerContext plannerContext,
      PairList<Integer, String> resultFields, Set<String> tableNames) {
    _workerManager = workerManager;
    _requestId = requestId;
    _plannerContext = plannerContext;
    _resultFields = resultFields;
    _tableNames = tableNames;

    if (QueryOptionsUtils.isUseLeafServerForIntermediateStage(plannerContext.getOptions(),
        plannerContext.getEnvConfig().defaultUseLeafServerForIntermediateStage())) {
      // Use only leaf servers for intermediate stages
      _leafServerInstances = new HashSet<>();
      _nonLookupTables = null;
    } else {
      // Use all servers (excluding lookup tables) for intermediate stages
      _leafServerInstances = null;
      _nonLookupTables = Sets.newHashSetWithExpectedSize(tableNames.size());
    }
  }

  public WorkerManager getWorkerManager() {
    return _workerManager;
  }

  public long getRequestId() {
    return _requestId;
  }

  public PlannerContext getPlannerContext() {
    return _plannerContext;
  }

  public PairList<Integer, String> getResultFields() {
    return _resultFields;
  }

  // Returns all the table names.
  public Set<String> getTableNames() {
    return _tableNames;
  }

  /// Returns `true` if we want to use servers for leaf stages as the workers for the intermediate stages.
  public boolean isUseLeafServerForIntermediateStage() {
    return _nonLookupTables == null;
  }

  /// Tracks non-lookup tables queried in leaf stages, which are used to determine the servers to use for intermediate
  /// stages. Should be used only when leaf servers are NOT directly used for intermediate stages.
  public Set<String> getNonLookupTables() {
    assert !isUseLeafServerForIntermediateStage();
    return _nonLookupTables;
  }

  /// Tracks servers that are used for leaf stages, which are used to determine the servers to use for intermediate
  /// stages. Should be used only when leaf servers are directly used for intermediate stages.
  public Set<QueryServerInstance> getLeafServerInstances() {
    assert isUseLeafServerForIntermediateStage();
    return _leafServerInstances;
  }

  public Map<Integer, DispatchablePlanMetadata> getDispatchablePlanMetadataMap() {
    return _dispatchablePlanMetadataMap;
  }

  public Map<Integer, PlanNode> getDispatchablePlanStageRootMap() {
    return _dispatchablePlanStageRootMap;
  }

  public Map<Integer, DispatchablePlanFragment> constructDispatchablePlanFragmentMap(PlanFragment subPlanRoot) {
    Map<Integer, DispatchablePlanFragment> dispatchablePlanFragmentMap = createDispatchablePlanFragmentMap(subPlanRoot);
    for (Map.Entry<Integer, DispatchablePlanMetadata> planMetadataEntry : _dispatchablePlanMetadataMap.entrySet()) {
      int stageId = planMetadataEntry.getKey();
      DispatchablePlanMetadata dispatchablePlanMetadata = planMetadataEntry.getValue();

      // construct each worker metadata
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap =
          dispatchablePlanMetadata.getWorkerIdToServerInstanceMap();
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap =
          dispatchablePlanMetadata.getWorkerIdToSegmentsMap();
      Map<Integer, Map<String, List<String>>> workerIdToTableNameSegmentsMap =
          dispatchablePlanMetadata.getWorkerIdToTableSegmentsMap();
      Map<Integer, Map<Integer, MailboxInfos>> workerIdToMailboxesMap =
          dispatchablePlanMetadata.getWorkerIdToMailboxesMap();
      Preconditions.checkArgument(workerIdToSegmentsMap == null || workerIdToTableNameSegmentsMap == null,
          "Both workerIdToSegmentsMap and workerIdToTableNameSegmentsMap cannot be set at the same time");
      Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdsMap = new HashMap<>();
      WorkerMetadata[] workerMetadataArray = new WorkerMetadata[workerIdToServerInstanceMap.size()];
      for (Map.Entry<Integer, QueryServerInstance> serverEntry : workerIdToServerInstanceMap.entrySet()) {
        int workerId = serverEntry.getKey();
        QueryServerInstance queryServerInstance = serverEntry.getValue();
        serverInstanceToWorkerIdsMap.computeIfAbsent(queryServerInstance, k -> new ArrayList<>()).add(workerId);
        WorkerMetadata workerMetadata = new WorkerMetadata(workerId, workerIdToMailboxesMap.get(workerId));
        if (workerIdToSegmentsMap != null) {
          workerMetadata.setTableSegmentsMap(workerIdToSegmentsMap.get(workerId));
        }
        if (workerIdToTableNameSegmentsMap != null) {
          workerMetadata.setLogicalTableSegmentsMap(workerIdToTableNameSegmentsMap.get(workerId));
        }
        workerMetadataArray[workerId] = workerMetadata;
      }

      // set the stageMetadata
      DispatchablePlanFragment dispatchablePlanFragment = dispatchablePlanFragmentMap.get(stageId);
      dispatchablePlanFragment.setWorkerMetadataList(Arrays.asList(workerMetadataArray));
      if (workerIdToSegmentsMap != null) {
        dispatchablePlanFragment.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
      }
      dispatchablePlanFragment.setServerInstanceToWorkerIdMap(serverInstanceToWorkerIdsMap);
      Preconditions.checkState(dispatchablePlanMetadata.getScannedTables().size() <= 1,
          "More than one table is not supported yet");
      if (dispatchablePlanMetadata.getScannedTables().size() == 1) {
        dispatchablePlanFragment.setTableName(dispatchablePlanMetadata.getScannedTables().get(0));
      }
      if (dispatchablePlanMetadata.getTimeBoundaryInfo() != null) {
        dispatchablePlanFragment.setTimeBoundaryInfo(dispatchablePlanMetadata.getTimeBoundaryInfo());
      }
    }
    return dispatchablePlanFragmentMap;
  }

  private Map<Integer, DispatchablePlanFragment> createDispatchablePlanFragmentMap(PlanFragment planFragmentRoot) {
    HashMap<Integer, DispatchablePlanFragment> result =
        Maps.newHashMapWithExpectedSize(_dispatchablePlanMetadataMap.size());
    Queue<PlanFragment> pendingPlanFragmentIds = new ArrayDeque<>();
    pendingPlanFragmentIds.add(planFragmentRoot);
    while (!pendingPlanFragmentIds.isEmpty()) {
      PlanFragment planFragment = pendingPlanFragmentIds.poll();
      int planFragmentId = planFragment.getFragmentId();

      if (result.containsKey(planFragmentId)) { // this can happen if some stage is spooled.
        LOGGER.debug("Skipping already visited stage {}", planFragmentId);
        continue;
      }
      result.put(planFragmentId, new DispatchablePlanFragment(planFragment));

      pendingPlanFragmentIds.addAll(planFragment.getChildren());
    }
    return result;
  }
}
