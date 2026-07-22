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

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.operator.AsofJoinOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.LookupJoinOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.NonEquiJoinOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Default implementation that constructs the built-in join operators.
 */
public class DefaultJoinOperatorFactory implements JoinOperatorFactory {
  @Override
  public MultiStageOperator createJoinOperator(OpChainExecutionContext context, MultiStageOperator leftOperator,
      PlanNode leftPlanNode, MultiStageOperator rightOperator, PlanNode rightPlanNode, JoinNode joinNode) {
    JoinNode.JoinStrategy joinStrategy = joinNode.getJoinStrategy();
    DataSchema leftSchema = leftPlanNode.getDataSchema();
    switch (joinStrategy) {
      case HASH:
        if (joinNode.getLeftKeys().isEmpty()) {
          // TODO: Consider adding non-equi as a separate join strategy.
          return new NonEquiJoinOperator(context, leftOperator, leftSchema, rightOperator, joinNode);
        } else {
          return new HashJoinOperator(context, leftOperator, leftSchema, rightOperator, joinNode);
        }
      case LOOKUP:
        return new LookupJoinOperator(context, leftOperator, leftSchema, rightOperator, joinNode);
      case ASOF:
        return new AsofJoinOperator(context, leftOperator, leftSchema, rightOperator, joinNode);
      default:
        throw new IllegalStateException("Unsupported JoinStrategy: " + joinStrategy);
    }
  }

  /// Enriched joins have been removed. This method is retained only for backward compatibility of the
  /// {@link JoinOperatorFactory} interface and always throws. A current broker never produces an
  /// {@link EnrichedJoinNode}, so this is only reachable if a plan from an older-version broker is executed.
  ///
  /// @deprecated enriched joins are no longer supported; this method always throws.
  @Deprecated(forRemoval = true, since = "1.6.0")
  @Override
  public MultiStageOperator createEnrichedJoinOperator(OpChainExecutionContext context,
      MultiStageOperator leftOperator, PlanNode leftPlanNode, MultiStageOperator rightOperator, PlanNode rightPlanNode,
      EnrichedJoinNode joinNode) {
    throw new UnsupportedOperationException("Enriched joins are no longer supported");
  }
}
