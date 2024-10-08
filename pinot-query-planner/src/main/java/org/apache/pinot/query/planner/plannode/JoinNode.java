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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class JoinNode extends BasePlanNode {
  private final JoinRelType _joinType;
  private final List<Integer> _leftKeys;
  private final List<Integer> _rightKeys;
  private final List<RexExpression> _nonEquiConditions;
  private final JoinStrategy _joinStrategy;

  public JoinNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      JoinStrategy joinStrategy) {
    super(stageId, dataSchema, nodeHint, inputs);
    _joinType = joinType;
    _leftKeys = leftKeys;
    _rightKeys = rightKeys;
    _nonEquiConditions = nonEquiConditions;
    _joinStrategy = joinStrategy;
  }

  public JoinRelType getJoinType() {
    return _joinType;
  }

  public List<Integer> getLeftKeys() {
    return _leftKeys;
  }

  public List<Integer> getRightKeys() {
    return _rightKeys;
  }

  public List<RexExpression> getNonEquiConditions() {
    return _nonEquiConditions;
  }

  public JoinStrategy getJoinStrategy() {
    return _joinStrategy;
  }

  @Override
  public String explain() {
    return "JOIN";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new JoinNode(_stageId, _dataSchema, _nodeHint, inputs, _joinType, _leftKeys, _rightKeys, _nonEquiConditions,
        _joinStrategy);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JoinNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    JoinNode joinNode = (JoinNode) o;
    return _joinType == joinNode._joinType && Objects.equals(_leftKeys, joinNode._leftKeys) && Objects.equals(
        _rightKeys, joinNode._rightKeys) && Objects.equals(_nonEquiConditions, joinNode._nonEquiConditions)
        && _joinStrategy == joinNode._joinStrategy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _joinType, _leftKeys, _rightKeys, _nonEquiConditions, _joinStrategy);
  }

  public enum JoinStrategy {
    HASH, LOOKUP
  }
}
