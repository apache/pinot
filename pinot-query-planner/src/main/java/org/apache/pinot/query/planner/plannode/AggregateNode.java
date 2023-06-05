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
package org.apache.pinot.query.planner.plannode;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class AggregateNode extends AbstractPlanNode {
  private List<RelHint> _relHints;
  @ProtoProperties
  private List<RexExpression> _aggCalls;
  @ProtoProperties
  private List<RexExpression> _groupSet;

  public AggregateNode(int planFragmentId) {
    super(planFragmentId);
  }

  public AggregateNode(int planFragmentId, DataSchema dataSchema, List<AggregateCall> aggCalls,
      List<RexExpression> groupSet,
      List<RelHint> relHints) {
    super(planFragmentId, dataSchema);


    _groupSet = groupSet;
    _relHints = relHints;
    _aggCalls = aggCalls.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    Preconditions.checkState(!(isFinalStage(this) && isIntermediateStage(this)),
        "Unable to compile aggregation with both hints for final and intermediate agg type.");
  }

  public static boolean isFinalStage(AggregateNode aggNode) {
    return PinotHintStrategyTable.containsHint(aggNode.getRelHints(),
        PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE);
  }

  public static boolean isIntermediateStage(AggregateNode aggNode) {
    return PinotHintStrategyTable.containsHint(aggNode.getRelHints(),
        PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE);
  }

  public static boolean isSingleStageAggregation(AggregateNode aggNode) {
    return PinotHintStrategyTable.containsHint(aggNode.getRelHints(),
        PinotHintStrategyTable.INTERNAL_IS_SINGLE_STAGE_AGG);
  }

  public List<RexExpression> getAggCalls() {
    return _aggCalls;
  }

  public List<RexExpression> getGroupSet() {
    return _groupSet;
  }

  public List<RelHint> getRelHints() {
    return _relHints;
  }

  @Override
  public String explain() {
    return "AGGREGATE";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitAggregate(this, context);
  }
}
