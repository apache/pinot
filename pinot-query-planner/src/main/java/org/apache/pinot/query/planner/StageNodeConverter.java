package org.apache.pinot.query.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.pinot.query.planner.nodes.CalcNode;
import org.apache.pinot.query.planner.nodes.JoinNode;
import org.apache.pinot.query.planner.nodes.StageNode;
import org.apache.pinot.query.planner.nodes.TableScanNode;


public final class StageNodeConverter {


  /**
   * convert a normal relation node into stage node with just the expression piece.
   *
   * @param node
   * @return
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
