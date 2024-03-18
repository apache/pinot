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
import java.util.concurrent.ExecutorService;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;


/**
 * Context class for converting a {@link StagePlan} into
 * {@link PinotQuery} to execute on server.
 *
 * On leaf-stage server node, {@link PlanNode} are split into {@link PinotQuery} part and
 *     {@link org.apache.pinot.query.runtime.operator.OpChain} part.
 */
public class ServerPlanRequestContext {
  private final StagePlan _stagePlan;
  private final QueryExecutor _leafQueryExecutor;
  private final ExecutorService _executorService;
  private final PipelineBreakerResult _pipelineBreakerResult;

  private final PinotQuery _pinotQuery;
  private PlanNode _leafStageBoundaryNode;
  private List<ServerQueryRequest> _serverQueryRequests;

  public ServerPlanRequestContext(StagePlan stagePlan, QueryExecutor leafQueryExecutor,
      ExecutorService executorService, PipelineBreakerResult pipelineBreakerResult) {
    _stagePlan = stagePlan;
    _leafQueryExecutor = leafQueryExecutor;
    _executorService = executorService;
    _pipelineBreakerResult = pipelineBreakerResult;
    _pinotQuery = new PinotQuery();
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
