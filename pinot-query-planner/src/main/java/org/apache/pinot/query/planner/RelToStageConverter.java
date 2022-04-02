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
package org.apache.pinot.query.planner;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.query.planner.nodes.CalcNode;
import org.apache.pinot.query.planner.nodes.JoinNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.planner.nodes.TableScanNode;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;


/**
 * The {@code StageNodeConverter} converts a logical {@link RelNode} to a {@link StageNode}.
 */
public final class RelToStageConverter {

  private RelToStageConverter() {
    // do not instantiate.
  }

  /**
   * convert a normal relation node into stage node with just the expression piece.
   *
   * TODO: we should convert this to a more structured pattern once we determine the serialization format used.
   *
   * @param node relational node
   * @return stage node.
   */
  public static StageNode toStageNode(RelNode node, int currentStageId) {
    if (node instanceof LogicalCalc) {
      return convertLogicalCal((LogicalCalc) node, currentStageId);
    } else if (node instanceof LogicalTableScan) {
      return convertLogicalTableScan((LogicalTableScan) node, currentStageId);
    } else if (node instanceof LogicalJoin) {
      return convertLogicalJoin((LogicalJoin) node, currentStageId);
    } else {
      throw new UnsupportedOperationException("Unsupported logical plan node: " + node);
    }
  }

  private static StageNode convertLogicalTableScan(LogicalTableScan node, int currentStageId) {
    String tableName = node.getTable().getQualifiedName().get(0);
    List<String> columnNames = node.getRowType().getFieldList().stream()
        .map(RelDataTypeField::getName).collect(Collectors.toList());
    return new TableScanNode(currentStageId, tableName, columnNames);
  }

  private static StageNode convertLogicalCal(LogicalCalc node, int currentStageId) {
    // TODO: support actual calcNode
    return new CalcNode(currentStageId, node.getDigest());
  }

  private static StageNode convertLogicalJoin(LogicalJoin node, int currentStageId) {
    JoinRelType joinType = node.getJoinType();
    RexCall joinCondition = (RexCall) node.getCondition();
    Preconditions.checkState(
        joinCondition.getOperator().getKind().equals(SqlKind.EQUALS) && joinCondition.getOperands().size() == 2,
        "only equality JOIN is supported");
    Preconditions.checkState(joinCondition.getOperands().get(0) instanceof RexInputRef, "only reference supported");
    Preconditions.checkState(joinCondition.getOperands().get(1) instanceof RexInputRef, "only reference supported");
    RelDataType leftRowType = node.getLeft().getRowType();
    RelDataType rightRowType = node.getRight().getRowType();
    int leftOperandIndex = ((RexInputRef) joinCondition.getOperands().get(0)).getIndex();
    int rightOperandIndex = ((RexInputRef) joinCondition.getOperands().get(1)).getIndex();
    FieldSelectionKeySelector leftFieldSelectionKeySelector = new FieldSelectionKeySelector(leftOperandIndex);
    FieldSelectionKeySelector rightFieldSelectionKeySelector =
          new FieldSelectionKeySelector(rightOperandIndex - leftRowType.getFieldNames().size());
    return new JoinNode(currentStageId, joinType, Collections.singletonList(new JoinNode.JoinClause(
        leftFieldSelectionKeySelector, rightFieldSelectionKeySelector)));
  }
}
