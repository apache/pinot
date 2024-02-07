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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.util.Pair;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.routing.WorkerMetadata;


public class DispatchablePlanContext {
  private final WorkerManager _workerManager;

  private final long _requestId;
  private final Set<String> _tableNames;
  private final List<Pair<Integer, String>> _resultFields;

  private final PlannerContext _plannerContext;
  private final Map<Integer, DispatchablePlanMetadata> _dispatchablePlanMetadataMap;
  private final Map<Integer, PlanNode> _dispatchablePlanStageRootMap;

  public DispatchablePlanContext(WorkerManager workerManager, long requestId, PlannerContext plannerContext,
      List<Pair<Integer, String>> resultFields, Set<String> tableNames) {
    _workerManager = workerManager;
    _requestId = requestId;
    _plannerContext = plannerContext;
    _dispatchablePlanMetadataMap = new HashMap<>();
    _dispatchablePlanStageRootMap = new HashMap<>();
    _resultFields = resultFields;
    _tableNames = tableNames;
  }

  public WorkerManager getWorkerManager() {
    return _workerManager;
  }

  public long getRequestId() {
    return _requestId;
  }

  // Returns all the table names.
  public Set<String> getTableNames() {
    return _tableNames;
  }

  public List<Pair<Integer, String>> getResultFields() {
    return _resultFields;
  }

  public PlannerContext getPlannerContext() {
    return _plannerContext;
  }

  public Map<Integer, DispatchablePlanMetadata> getDispatchablePlanMetadataMap() {
    return _dispatchablePlanMetadataMap;
  }

  public Map<Integer, PlanNode> getDispatchablePlanStageRootMap() {
    return _dispatchablePlanStageRootMap;
  }

  public List<DispatchablePlanFragment> constructDispatchablePlanFragmentList(PlanFragment subPlanRoot) {
    DispatchablePlanFragment[] dispatchablePlanFragmentArray =
        new DispatchablePlanFragment[_dispatchablePlanStageRootMap.size()];
    createDispatchablePlanFragmentList(dispatchablePlanFragmentArray, subPlanRoot);
    for (Map.Entry<Integer, DispatchablePlanMetadata> planMetadataEntry : _dispatchablePlanMetadataMap.entrySet()) {
      int stageId = planMetadataEntry.getKey();
      DispatchablePlanMetadata dispatchablePlanMetadata = planMetadataEntry.getValue();

      // construct each worker metadata
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap =
          dispatchablePlanMetadata.getWorkerIdToServerInstanceMap();
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap =
          dispatchablePlanMetadata.getWorkerIdToSegmentsMap();
      Map<Integer, Map<Integer, MailboxMetadata>> workerIdToMailboxesMap =
          dispatchablePlanMetadata.getWorkerIdToMailboxesMap();
      Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdsMap = new HashMap<>();
      WorkerMetadata[] workerMetadataArray = new WorkerMetadata[workerIdToServerInstanceMap.size()];
      for (Map.Entry<Integer, QueryServerInstance> serverEntry : workerIdToServerInstanceMap.entrySet()) {
        int workerId = serverEntry.getKey();
        QueryServerInstance queryServerInstance = serverEntry.getValue();
        serverInstanceToWorkerIdsMap.computeIfAbsent(queryServerInstance, k -> new ArrayList<>()).add(workerId);
        WorkerMetadata workerMetadata = new WorkerMetadata(new VirtualServerAddress(queryServerInstance, workerId),
            workerIdToMailboxesMap.get(workerId));
        if (workerIdToSegmentsMap != null) {
          workerMetadata.setTableSegmentsMap(workerIdToSegmentsMap.get(workerId));
        }
        workerMetadataArray[workerId] = workerMetadata;
      }

      // set the stageMetadata
      DispatchablePlanFragment dispatchablePlanFragment = dispatchablePlanFragmentArray[stageId];
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
    return Arrays.asList(dispatchablePlanFragmentArray);
  }

  private void createDispatchablePlanFragmentList(DispatchablePlanFragment[] dispatchablePlanFragmentArray,
      PlanFragment planFragmentRoot) {
    dispatchablePlanFragmentArray[planFragmentRoot.getFragmentId()] = new DispatchablePlanFragment(planFragmentRoot);
    for (PlanFragment childPlanFragment : planFragmentRoot.getChildren()) {
      createDispatchablePlanFragmentList(dispatchablePlanFragmentArray, childPlanFragment);
    }
  }
}
