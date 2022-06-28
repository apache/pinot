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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.TableScanNode;


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
    if (node instanceof LogicalTableScan) {
      return convertLogicalTableScan((LogicalTableScan) node, currentStageId);
    } else if (node instanceof LogicalJoin) {
      return convertLogicalJoin((LogicalJoin) node, currentStageId);
    } else if (node instanceof LogicalProject) {
      return convertLogicalProject((LogicalProject) node, currentStageId);
    } else if (node instanceof LogicalFilter) {
      return convertLogicalFilter((LogicalFilter) node, currentStageId);
    } else {
      throw new UnsupportedOperationException("Unsupported logical plan node: " + node);
    }
  }

  private static StageNode convertLogicalProject(LogicalProject node, int currentStageId) {
    return new ProjectNode(currentStageId, node.getProjects());
  }

  private static StageNode convertLogicalFilter(LogicalFilter node, int currentStageId) {
    return new FilterNode(currentStageId, node.getCondition());
  }

  private static StageNode convertLogicalTableScan(LogicalTableScan node, int currentStageId) {
    String tableName = node.getTable().getQualifiedName().get(0);
    List<String> columnNames = node.getRowType().getFieldList().stream()
        .map(RelDataTypeField::getName).collect(Collectors.toList());
    return new TableScanNode(currentStageId, tableName, columnNames);
  }

  private static StageNode convertLogicalJoin(LogicalJoin node, int currentStageId) {
    JoinRelType joinType = node.getJoinType();
    Preconditions.checkState(node.getCondition() instanceof RexCall);
    RexCall joinCondition = (RexCall) node.getCondition();

    // Parse out all equality JOIN conditions
    int leftNodeOffset = node.getLeft().getRowType().getFieldList().size();
    List<List<Integer>> predicateColumns = PlannerUtils.parseJoinConditions(joinCondition, leftNodeOffset);

    FieldSelectionKeySelector leftFieldSelectionKeySelector = new FieldSelectionKeySelector(predicateColumns.get(0));
    FieldSelectionKeySelector rightFieldSelectionKeySelector = new FieldSelectionKeySelector(predicateColumns.get(1));
    return new JoinNode(currentStageId, joinType, Collections.singletonList(new JoinNode.JoinClause(
        leftFieldSelectionKeySelector, rightFieldSelectionKeySelector)));
  }
}
