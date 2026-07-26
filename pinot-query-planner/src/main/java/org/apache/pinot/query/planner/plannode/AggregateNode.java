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


public class AggregateNode extends BasePlanNode {
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<Integer> _filterArgs;
  private final List<Integer> _groupKeys;
  private final AggType _aggType;
  private final boolean _leafReturnFinalResult;

  /// GROUP BY GROUPING SETS / ROLLUP / CUBE: one entry per grouping set, each the list of indexes into _groupKeys
  /// (the union of all grouping columns) participating in (grouped by) that set; an empty inner list is the
  /// grand-total set (). A set's position in this list is its ordinal, carried as the synthetic $groupingId
  /// discriminator, so the number of grouping columns is unlimited. Empty for a plain GROUP BY. Mirrors the
  /// single-stage engine's PinotQuery.groupingSets so the per-set row expansion can be pushed down to the
  /// single-stage (leaf) engine.
  private final List<List<Integer>> _groupingSets;

  // The following fields are set when group trim is enabled, and are extracted from the Sort on top of this Aggregate.
  // The group trim behavior at leaf stage is shared with single-stage engine.
  private final List<RelFieldCollation> _collations;
  private final int _limit;

  public AggregateNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression.FunctionCall> aggCalls, List<Integer> filterArgs, List<Integer> groupKeys, AggType aggType,
      boolean leafReturnFinalResult, @Nullable List<RelFieldCollation> collations, int limit) {
    this(stageId, dataSchema, nodeHint, inputs, aggCalls, filterArgs, groupKeys, aggType, leafReturnFinalResult,
        collations, limit, List.of());
  }

  public AggregateNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression.FunctionCall> aggCalls, List<Integer> filterArgs, List<Integer> groupKeys, AggType aggType,
      boolean leafReturnFinalResult, @Nullable List<RelFieldCollation> collations, int limit,
      List<List<Integer>> groupingSets) {
    super(stageId, dataSchema, nodeHint, inputs);
    _aggCalls = aggCalls;
    _filterArgs = filterArgs;
    _groupKeys = groupKeys;
    _aggType = aggType;
    _leafReturnFinalResult = leafReturnFinalResult;
    _collations = collations != null ? collations : List.of();
    _limit = limit;
    _groupingSets = groupingSets;
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

  public AggType getAggType() {
    return _aggType;
  }

  public boolean isLeafReturnFinalResult() {
    return _leafReturnFinalResult;
  }

  /// Per grouping set (in ordinal order), the indexes into {@link #getGroupKeys()} participating in it, or
  /// empty for a plain GROUP BY.
  public List<List<Integer>> getGroupingSets() {
    return _groupingSets;
  }

  /// Whether this is a GROUP BY GROUPING SETS / ROLLUP / CUBE aggregate.
  public boolean isGroupingSets() {
    return !_groupingSets.isEmpty();
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public int getLimit() {
    return _limit;
  }

  @Override
  public String explain() {
    return "AGGREGATE_" + _aggType;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitAggregate(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new AggregateNode(_stageId, _dataSchema, _nodeHint, inputs, _aggCalls, _filterArgs, _groupKeys, _aggType,
        _leafReturnFinalResult, _collations, _limit, _groupingSets);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AggregateNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AggregateNode that = (AggregateNode) o;
    return _leafReturnFinalResult == that._leafReturnFinalResult && _limit == that._limit && Objects.equals(_aggCalls,
        that._aggCalls) && Objects.equals(_filterArgs, that._filterArgs) && Objects.equals(_groupKeys, that._groupKeys)
        && _aggType == that._aggType && Objects.equals(_collations, that._collations) && Objects.equals(_groupingSets,
        that._groupingSets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _aggCalls, _filterArgs, _groupKeys, _aggType, _leafReturnFinalResult,
        _collations, _limit, _groupingSets);
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
    //@formatter:off
    DIRECT(false, false),
    LEAF(false, true),
    INTERMEDIATE(true, true),
    FINAL(true, false);
    //@formatter:on

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
