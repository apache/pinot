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

import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.SubPlan;
import org.apache.pinot.query.routing.WorkerManager;


public class PinotDispatchPlanner {

  private final WorkerManager _workerManager;
  private final long _requestId;
  private final PlannerContext _plannerContext;

  private final TableCache _tableCache;

  public PinotDispatchPlanner(PlannerContext plannerContext, WorkerManager workerManager, long requestId,
      TableCache tableCache) {
    _plannerContext = plannerContext;
    _workerManager = workerManager;
    _requestId = requestId;
    _tableCache = tableCache;
  }

  public DispatchableSubPlan createDispatchTasks(SubPlan subPlan) {
    // perform physical plan conversion and assign workers to each stage.
    DispatchablePlanContext dispatchablePlanContext = new DispatchablePlanContext(_workerManager, _requestId,
        _plannerContext, subPlan.getSubPlanMetadata().getFields(), subPlan.getSubPlanMetadata().getTableNames());
    DispatchableSubPlan dispatchableSubPlan =
        DispatchablePlanVisitor.INSTANCE.constructDispatchableSubPlan(subPlan, dispatchablePlanContext, _tableCache);
    return dispatchableSubPlan;
  }
}
