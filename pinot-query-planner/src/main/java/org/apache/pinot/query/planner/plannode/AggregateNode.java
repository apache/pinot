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
  @ProtoProperties
  private AggregationStage _aggregationStage;
  @ProtoProperties
  private boolean _treatIntermediateStageAsLeaf;

  /**
   * Enum to denote the aggregation stage being performed. Hints are used to populate these values
   * LEAF - leaf aggregation level which performs the aggregations on the raw values directly
   * INTERMEDIATE - intermediate aggregation level which merges results from lower aggregation levels
   * FINAL - final aggregation level which extracts the final result
   */
  public enum AggregationStage {
    LEAF,
    INTERMEDIATE,
    FINAL
  }

  public AggregateNode(int planFragmentId) {
    super(planFragmentId);
  }

  public AggregateNode(int planFragmentId, DataSchema dataSchema, List<AggregateCall> aggCalls,
      List<RexExpression> groupSet, List<RelHint> relHints) {
    super(planFragmentId, dataSchema);
    _aggCalls = aggCalls.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    _groupSet = groupSet;
    _relHints = relHints;
    Preconditions.checkState(areHintsValid(relHints),
        "Unable to compile aggregation with combination of hints for final, intermediate and leaf agg type");
    _aggregationStage = getAggregationStage(relHints);
    _treatIntermediateStageAsLeaf = isIntermediateStage(this) && PinotHintStrategyTable
        .containsHint(relHints, PinotHintStrategyTable.INTERNAL_AGG_IS_LEAF_STAGE_SKIPPED);
  }

  private boolean areHintsValid(List<RelHint> relHints) {
    int hasFinalHint = PinotHintStrategyTable.containsHint(relHints,
        PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE) ? 1 : 0;
    int hasIntermediateHint = PinotHintStrategyTable.containsHint(relHints,
        PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE) ? 1 : 0;
    int hasLeafHint = PinotHintStrategyTable.containsHint(relHints,
        PinotHintStrategyTable.INTERNAL_AGG_LEAF_STAGE) ? 1 : 0;
    return (hasFinalHint + hasIntermediateHint + hasLeafHint) == 1;
  }

  private AggregationStage getAggregationStage(List<RelHint> relHints) {
    if (PinotHintStrategyTable.containsHint(relHints,
        PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE)) {
      return AggregationStage.FINAL;
    }
    if (PinotHintStrategyTable.containsHint(relHints,
        PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE)) {
      return AggregationStage.INTERMEDIATE;
    }
    return AggregationStage.LEAF;
  }

  public static boolean isFinalStage(AggregateNode aggNode) {
    return aggNode.getAggregationStage() == AggregationStage.FINAL;
  }

  public static boolean isIntermediateStage(AggregateNode aggNode) {
    return aggNode.getAggregationStage() == AggregationStage.INTERMEDIATE;
  }

  public static boolean isLeafStage(AggregateNode aggNode) {
    return aggNode.getAggregationStage() == AggregationStage.LEAF;
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

  public AggregationStage getAggregationStage() {
    return _aggregationStage;
  }

  public boolean isTreatIntermediateStageAsLeaf() {
    return _treatIntermediateStageAsLeaf;
  }

  public boolean isFinalStage() {
    return _aggregationStage == AggregationStage.FINAL;
  }

  public boolean isIntermediateStage() {
    return _aggregationStage == AggregationStage.INTERMEDIATE;
  }

  public boolean isLeafStage() {
    return _aggregationStage == AggregationStage.LEAF;
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
