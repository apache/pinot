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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class WindowNode extends BasePlanNode {
  private final List<Integer> _keys;
  private final List<RelFieldCollation> _collations;
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final WindowFrameType _windowFrameType;
  // Both these bounds are relative to current row; 0 means current row, -1 means previous row, 1 means next row, etc.
  // Integer.MIN_VALUE represents UNBOUNDED PRECEDING which is only allowed for the lower bound (ensured by Calcite).
  // Integer.MAX_VALUE represents UNBOUNDED FOLLOWING which is only allowed for the upper bound (ensured by Calcite).
  private final int _lowerBound;
  private final int _upperBound;
  private final List<RexExpression.Literal> _constants;

  public WindowNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs, List<Integer> keys,
      List<RelFieldCollation> collations, List<RexExpression.FunctionCall> aggCalls, WindowFrameType windowFrameType,
      int lowerBound, int upperBound, List<RexExpression.Literal> constants) {
    super(stageId, dataSchema, nodeHint, inputs);
    _keys = keys;
    _collations = collations;
    _aggCalls = aggCalls;
    _windowFrameType = windowFrameType;
    _lowerBound = lowerBound;
    _upperBound = upperBound;
    _constants = constants;
  }

  public List<Integer> getKeys() {
    return _keys;
  }

  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public List<RexExpression.FunctionCall> getAggCalls() {
    return _aggCalls;
  }

  public WindowFrameType getWindowFrameType() {
    return _windowFrameType;
  }

  public int getLowerBound() {
    return _lowerBound;
  }

  public int getUpperBound() {
    return _upperBound;
  }

  public List<RexExpression.Literal> getConstants() {
    return _constants;
  }

  @Override
  public String explain() {
    return "WINDOW";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitWindow(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new WindowNode(_stageId, _dataSchema, _nodeHint, inputs, _keys, _collations, _aggCalls, _windowFrameType,
        _lowerBound, _upperBound, _constants);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WindowNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    WindowNode that = (WindowNode) o;
    return _lowerBound == that._lowerBound && _upperBound == that._upperBound && Objects.equals(_aggCalls,
        that._aggCalls) && Objects.equals(_keys, that._keys) && Objects.equals(_collations, that._collations)
        && _windowFrameType == that._windowFrameType && Objects.equals(_constants, that._constants);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _aggCalls, _keys, _collations, _windowFrameType, _lowerBound, _upperBound,
        _constants);
  }

  /**
   * Enum to denote the type of window frame
   * ROWS - ROWS type window frame
   * RANGE - RANGE type window frame
   */
  public enum WindowFrameType {
    ROWS, RANGE
  }
}
