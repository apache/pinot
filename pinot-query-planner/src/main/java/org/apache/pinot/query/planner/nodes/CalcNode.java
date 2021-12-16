package org.apache.pinot.query.planner.nodes;

import org.apache.calcite.rel.logical.LogicalCalc;


public class CalcNode extends AbstractStageNode {
  private final String _expression;

  public CalcNode(LogicalCalc node, String currentStageId) {
    super(currentStageId);
    _expression = toExpression(node);
  }

  public String getExpression() {
    return _expression;
  }

  private String toExpression(LogicalCalc node) {
    // TODO: make it real.
    return node.getDigest();
  }
}
