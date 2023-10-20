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
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Extension of the {@link OpChainExecutionContext} for {@link DistributedStagePlan} runs on leaf-stage.
 *
 * The difference is that: on a leaf-stage server node, {@link PlanNode} are split into {@link PinotQuery} part and
 * {@link org.apache.pinot.query.runtime.operator.OpChain} part and are connected together in this context.
 */
public class ServerOpChainExecutionContext extends OpChainExecutionContext {
  private final DistributedStagePlan _stagePlan;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;

  private final PinotQuery _pinotQuery;
  private PlanNode _leafStageBoundaryNode;
  private List<ServerQueryRequest> _serverQueryRequests;

  public ServerOpChainExecutionContext(OpChainExecutionContext executionContext, DistributedStagePlan stagePlan,
      QueryExecutor leafQueryExecutor, ExecutorService executorService) {
    super(executionContext);
    _stagePlan = stagePlan;
    _queryExecutor = leafQueryExecutor;
    _executorService = executorService;
    _pinotQuery = new PinotQuery();
  }

  public DistributedStagePlan getStagePlan() {
    return _stagePlan;
  }

  public QueryExecutor getQueryExecutor() {
    return _queryExecutor;
  }

  public ExecutorService getExecutorService() {
    return _executorService;
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
