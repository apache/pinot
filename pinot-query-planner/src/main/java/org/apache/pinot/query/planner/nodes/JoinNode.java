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
package org.apache.pinot.query.planner.nodes;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;


public class JoinNode extends AbstractStageNode {
  private final JoinRelType _joinType;
  private final int _leftOperandIndex;
  private final int _rightOperandIndex;
  private final FieldSelectionKeySelector _leftFieldSelectionKeySelector;
  private final FieldSelectionKeySelector _rightFieldSelectionKeySelector;

  private transient final RelDataType _leftRowType;
  private transient final RelDataType _rightRowType;

  public JoinNode(LogicalJoin node, String currentStageId) {
    super(currentStageId);
    _joinType = node.getJoinType();
    RexCall joinCondition = (RexCall) node.getCondition();
    Preconditions.checkState(
        joinCondition.getOperator().getKind().equals(SqlKind.EQUALS) && joinCondition.getOperands().size() == 2,
        "only equality JOIN is supported");
    Preconditions.checkState(joinCondition.getOperands().get(0) instanceof RexInputRef, "only reference supported");
    Preconditions.checkState(joinCondition.getOperands().get(1) instanceof RexInputRef, "only reference supported");
    _leftRowType = node.getLeft().getRowType();
    _rightRowType = node.getRight().getRowType();
    _leftOperandIndex = ((RexInputRef) joinCondition.getOperands().get(0)).getIndex();
    _rightOperandIndex = ((RexInputRef) joinCondition.getOperands().get(1)).getIndex();
    _leftFieldSelectionKeySelector = new FieldSelectionKeySelector(_leftOperandIndex);
    _rightFieldSelectionKeySelector =
        new FieldSelectionKeySelector(_rightOperandIndex - _leftRowType.getFieldNames().size());
  }

  public JoinRelType getJoinType() {
    return _joinType;
  }

  public RelDataType getLeftRowType() {
    return _leftRowType;
  }

  public RelDataType getRightRowType() {
    return _rightRowType;
  }

  public int getLeftOperandIndex() {
    return _leftOperandIndex;
  }

  public int getRightOperandIndex() {
    return _rightOperandIndex;
  }

  public FieldSelectionKeySelector getLeftJoinKeySelector() {
    return _leftFieldSelectionKeySelector;
  }

  public FieldSelectionKeySelector getRightJoinKeySelector() {
    return _rightFieldSelectionKeySelector;
  }
}
