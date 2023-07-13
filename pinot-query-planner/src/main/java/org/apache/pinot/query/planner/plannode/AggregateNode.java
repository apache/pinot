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
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class AggregateNode extends AbstractPlanNode {
  @ProtoProperties
  private NodeHint _nodeHint;
  @ProtoProperties
  private List<RexExpression> _aggCalls;
  @ProtoProperties
  private List<RexExpression> _groupSet;
  @ProtoProperties
  private AggType _aggType;

  public AggregateNode(int planFragmentId) {
    super(planFragmentId);
  }

  public AggregateNode(int planFragmentId, DataSchema dataSchema, List<AggregateCall> aggCalls,
      List<RexExpression> groupSet, List<RelHint> relHints) {
    super(planFragmentId, dataSchema);
    Preconditions.checkState(areHintsValid(relHints), "invalid sql hint for agg node: {}", relHints);
    _aggCalls = aggCalls.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
    _groupSet = groupSet;
    _nodeHint = new NodeHint(relHints);
    _aggType = AggType.valueOf(PinotHintStrategyTable.getHintOption(relHints, PinotHintOptions.INTERNAL_AGG_OPTIONS, PinotHintOptions.InternalAggregateOptions.AGG_TYPE));
  }

  private boolean areHintsValid(List<RelHint> relHints) {
    return PinotHintStrategyTable.containsHint(relHints, PinotHintOptions.INTERNAL_AGG_OPTIONS);
  }

  public List<RexExpression> getAggCalls() {
    return _aggCalls;
  }

  public List<RexExpression> getGroupSet() {
    return _groupSet;
  }

  public NodeHint getNodeHint() {
    return _nodeHint;
  }

  public AggType getAggType() {
    return _aggType;
  }

  @Override
  public String explain() {
    return "AGGREGATE_" + _aggType;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitAggregate(this, context);
  }

  /**
   * Aggregation Types: Pinot aggregation functions can perform operation on input data which
   *   (1) directly accumulate from raw input, or
   *   (2) merging multiple intermediate data format;
   * in terms of output format, it can also
   *   (1) produce a mergeable intermediate data format, or
   *   (2) extract result as final result format.
   */
  public enum AggType {
    DIRECT(false, false),
    LEAF(false, true),
    INTERMEDIATE(true, true),
    FINAL(true, false);

    private final boolean _isInputIntermediateFormat;
    private final boolean _isOutputIntermediateFormat;

    AggType(boolean isInputIntermediateFormat, boolean isOutputIntermediateFormat) {
      _isInputIntermediateFormat = isInputIntermediateFormat;
      _isOutputIntermediateFormat = isOutputIntermediateFormat;
    }

    public boolean isInputIntermediateFormat() {
      return _isInputIntermediateFormat;
    }

    public boolean isOutputIntermediateFormat() {
      return _isOutputIntermediateFormat;
    }
  }
}
