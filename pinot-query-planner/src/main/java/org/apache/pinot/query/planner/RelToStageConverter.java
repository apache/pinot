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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.pinot.query.planner.nodes.CalcNode;
import org.apache.pinot.query.planner.nodes.JoinNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.planner.nodes.TableScanNode;


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
  public static StageNode toStageNode(RelNode node, String currentStageId) {
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

  private static StageNode convertLogicalTableScan(LogicalTableScan node, String currentStageId) {
    return new TableScanNode(node, currentStageId);
  }

  private static StageNode convertLogicalCal(LogicalCalc node, String currentStageId) {
    return new CalcNode(node, currentStageId);
  }

  private static StageNode convertLogicalJoin(LogicalJoin node, String currentStageId) {
    return new JoinNode(node, currentStageId);
  }
}
