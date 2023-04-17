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

import java.util.HashMap;
import java.util.List;
import org.apache.calcite.util.Pair;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.routing.WorkerManager;


public class DispatchablePlanContext {
  private final WorkerManager _workerManager;
  private final long _requestId;
  private final PlannerContext _plannerContext;
  private final QueryPlan _queryPlan;

  public DispatchablePlanContext(WorkerManager workerManager, long requestId, PlannerContext plannerContext,
      List<Pair<Integer, String>> resultFields) {
    _workerManager = workerManager;
    _requestId = requestId;
    _plannerContext = plannerContext;
    _queryPlan = new QueryPlan(resultFields, new HashMap<>(), new HashMap<>());
  }

  public QueryPlan getQueryPlan() {
    return _queryPlan;
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
}
