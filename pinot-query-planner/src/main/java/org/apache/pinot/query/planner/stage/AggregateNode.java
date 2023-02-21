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
package org.apache.pinot.query.planner.stage;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class AggregateNode extends AbstractStageNode {
  public static final RelHint FINAL_STAGE_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE).build();
  public static final RelHint INTERMEDIATE_STAGE_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE).build();

  private List<RelHint> _relHints;
  @ProtoProperties
  private List<RexExpression> _aggCalls;
  @ProtoProperties
  private List<RexExpression> _groupSet;

  public AggregateNode(int stageId) {
    super(stageId);
  }

  public AggregateNode(int stageId, DataSchema dataSchema, List<AggregateCall> aggCalls, List<RexExpression> groupSet,
      List<RelHint> relHints) {
    super(stageId, dataSchema);
    _aggCalls = aggCalls.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    _groupSet = groupSet;
    _relHints = relHints;
  }

  public static boolean isFinalStage(AggregateNode aggNode) {
    return aggNode.getRelHints().contains(FINAL_STAGE_HINT);
  }

  public static boolean isIntermediateStage(AggregateNode aggNode) {
    return aggNode.getRelHints().contains(INTERMEDIATE_STAGE_HINT);
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
  public <T, C> T visit(StageNodeVisitor<T, C> visitor, C context) {
    return visitor.visitAggregate(this, context);
  }
}
