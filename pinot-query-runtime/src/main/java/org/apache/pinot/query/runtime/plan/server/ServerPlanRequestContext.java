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
package org.apache.pinot.query.runtime.plan.server;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;


/// Context class for converting a [StagePlan] into
/// [PinotQuery] to execute on server.
///
/// On leaf-stage server node, [PlanNode] are split into [PinotQuery] part and
///     [org.apache.pinot.query.runtime.operator.OpChain] part.
public class ServerPlanRequestContext {
  private final StagePlan _stagePlan;
  @Nullable
  private final WorkerMetadata _workerMetadata;
  private final QueryExecutor _leafQueryExecutor;
  private final ExecutorService _executorService;
  @Nullable
  private final PipelineBreakerResult _pipelineBreakerResult;

  private final PinotQuery _pinotQuery;
  private PlanNode _leafStageBoundaryNode;
  private List<ServerQueryRequest> _serverQueryRequests;

  public ServerPlanRequestContext(StagePlan stagePlan, QueryExecutor leafQueryExecutor,
      ExecutorService executorService, @Nullable PipelineBreakerResult pipelineBreakerResult) {
    this(stagePlan, leafQueryExecutor, executorService, pipelineBreakerResult, null);
  }

  public ServerPlanRequestContext(StagePlan stagePlan, QueryExecutor leafQueryExecutor,
      ExecutorService executorService, @Nullable PipelineBreakerResult pipelineBreakerResult,
      @Nullable WorkerMetadata workerMetadata) {
    _stagePlan = stagePlan;
    _workerMetadata = workerMetadata;
    _leafQueryExecutor = leafQueryExecutor;
    _executorService = executorService;
    _pipelineBreakerResult = pipelineBreakerResult;
    _pinotQuery = new PinotQuery();
  }

  /// Proves that this worker reads one physical table, without logical-table expansion or interleaved hybrid runs.
  /// Only this worker's segment map is read: unrelated workers' legacy maps remain lazily decoded.
  public boolean isSinglePhysicalTable() {
    if (_workerMetadata == null || _workerMetadata.getLogicalTableSegmentsMap() != null) {
      return false;
    }
    Map<String, List<String>> tables = _workerMetadata.getTableSegmentsMap();
    return tables != null && tables.size() == 1;
  }

  public StagePlan getStagePlan() {
    return _stagePlan;
  }

  public QueryExecutor getLeafQueryExecutor() {
    return _leafQueryExecutor;
  }

  public ExecutorService getExecutorService() {
    return _executorService;
  }

  @Nullable
  public PipelineBreakerResult getPipelineBreakerResult() {
    return _pipelineBreakerResult;
  }

  public PinotQuery getPinotQuery() {
    return _pinotQuery;
  }

  public PlanNode getLeafStageBoundaryNode() {
    return _leafStageBoundaryNode;
  }

  public void setLeafStageBoundaryNode(PlanNode leafStageBoundaryNode) {
    _leafStageBoundaryNode = leafStageBoundaryNode;
  }

  public List<ServerQueryRequest> getServerQueryRequests() {
    return _serverQueryRequests;
  }

  public void setServerQueryRequests(List<ServerQueryRequest> serverQueryRequests) {
    _serverQueryRequests = serverQueryRequests;
  }
}
