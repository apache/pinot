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
package org.apache.pinot.query.runtime.operator.factory;

import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Factory for aggregation related operators.
 */
public interface AggregateOperatorFactory {

  /// Creates the aggregate operator. Grouping-set aggregates are normalized before this is called: the plan
  /// visitor wraps {@code inputOperator} in a RepeatOperator that expands each input row across the grouping
  /// sets, and passes the equivalent plain GROUP BY {@code aggregateNode} (group keys pointing at the appended
  /// key-copy columns plus $groupingId, grouping sets cleared), so implementations need no grouping-set awareness.
  ///
  /// IMPORTANT: for a grouping-set aggregate, {@code inputOperator} is the RepeatOperator, whose output schema is
  /// {@code [input columns..., one NULLable group-key copy per union group-by column..., $groupingId INT]} — this
  /// does NOT match {@code inputPlanNode.getDataSchema()}, which is the original PRE-expansion input schema.
  /// Implementations that need the operator's actual input schema must read it from {@code inputOperator} (or
  /// derive it from {@code aggregateNode}'s group keys), not from {@code inputPlanNode}. For a plain aggregate the
  /// two agree as usual.
  MultiStageOperator createAggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator,
      PlanNode inputPlanNode, AggregateNode aggregateNode);

  MultiStageOperator createWindowAggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator,
      PlanNode inputPlanNode, WindowNode windowNode);
}
