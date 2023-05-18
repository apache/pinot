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
package org.apache.pinot.query.planner;

import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * The {@code QueryPlan} is the logical query plan from the result of
 * {@link org.apache.pinot.query.planner.logical.PinotLogicalQueryPlanner}.
 *
 */
public class QueryPlan {
  private final PlanNode _planRoot;
  private final QueryPlanMetadata _queryPlanMetadata;

  public QueryPlan(PlanNode queryPlanRoot, QueryPlanMetadata queryPlanMetadata) {
    _planRoot = queryPlanRoot;
    _queryPlanMetadata = queryPlanMetadata;
  }

  /**
   * Get the root node of the query plan.
   */
  public PlanNode getPlanRoot() {
    return _planRoot;
  }

  /**
   * Get the metadata of the query plan.
   */
  public QueryPlanMetadata getPlanMetadata() {
    return _queryPlanMetadata;
  }
}
