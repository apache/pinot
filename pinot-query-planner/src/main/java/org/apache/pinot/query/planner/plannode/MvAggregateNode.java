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

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


/// An aggregate that groups each input row by every element of its multi-value (MV) group keys, with the
/// single-stage engine semantics: a row contributes once to the group of each element, a repeated element counts
/// once per copy, an empty array contributes to no group, and several MV keys give the cartesian product.
///
/// The keys listed in [#getMvKeyIndexes()] (positions in [#getGroupKeys()]) are array-typed in the input and
/// scalar (the component type) in the output [DataSchema]. Every other key is a plain single-value key.
///
/// This node does not extend [AggregateNode] on purpose: a visitor that only handles
/// [PlanNodeVisitor#visitAggregate] must not treat an MV aggregate as a plain one. Grouping sets are not supported.
///
/// Instances are immutable and thread-safe.
public class MvAggregateNode extends BasePlanNode {
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<Integer> _filterArgs;
  private final List<Integer> _groupKeys;
  private final AggregateNode.AggType _aggType;
  private final boolean _leafReturnFinalResult;
  // Set when group trim is enabled, extracted from the Sort on top of this aggregate (same as AggregateNode).
  private final List<RelFieldCollation> _collations;
  private final int _limit;
  /// Positions in _groupKeys of the MV keys, ascending.
  private final List<Integer> _mvKeyIndexes;

  public MvAggregateNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression.FunctionCall> aggCalls, List<Integer> filterArgs, List<Integer> groupKeys,
      AggregateNode.AggType aggType, boolean leafReturnFinalResult, @Nullable List<RelFieldCollation> collations,
      int limit, List<Integer> mvKeyIndexes) {
    super(stageId, dataSchema, nodeHint, inputs);
    _aggCalls = aggCalls;
    _filterArgs = filterArgs;
    _groupKeys = groupKeys;
    _aggType = aggType;
    _leafReturnFinalResult = leafReturnFinalResult;
    _collations = collations != null ? collations : List.of();
    _limit = limit;
    _mvKeyIndexes = mvKeyIndexes;
  }

  public List<RexExpression.FunctionCall> getAggCalls() {
    return _aggCalls;
  }

  public List<Integer> getFilterArgs() {
    return _filterArgs;
  }

  public List<Integer> getGroupKeys() {
    return _groupKeys;
  }

  public AggregateNode.AggType getAggType() {
    return _aggType;
  }

  public boolean isLeafReturnFinalResult() {
    return _leafReturnFinalResult;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public int getLimit() {
    return _limit;
  }

  /// The positions in [#getGroupKeys()] of the MV keys, in ascending order.
  public List<Integer> getMvKeyIndexes() {
    return _mvKeyIndexes;
  }

  @Override
  public String explain() {
    return "MV_AGGREGATE_" + _aggType;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMvAggregate(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new MvAggregateNode(_stageId, _dataSchema, _nodeHint, inputs, _aggCalls, _filterArgs, _groupKeys,
        _aggType, _leafReturnFinalResult, _collations, _limit, _mvKeyIndexes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MvAggregateNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MvAggregateNode that = (MvAggregateNode) o;
    return _leafReturnFinalResult == that._leafReturnFinalResult && _limit == that._limit && Objects.equals(_aggCalls,
        that._aggCalls) && Objects.equals(_filterArgs, that._filterArgs) && Objects.equals(_groupKeys, that._groupKeys)
        && _aggType == that._aggType && Objects.equals(_collations, that._collations) && Objects.equals(
        _mvKeyIndexes, that._mvKeyIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _aggCalls, _filterArgs, _groupKeys, _aggType, _leafReturnFinalResult,
        _collations, _limit, _mvKeyIndexes);
  }
}
