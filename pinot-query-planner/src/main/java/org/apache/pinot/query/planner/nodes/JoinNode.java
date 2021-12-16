package org.apache.pinot.query.planner.nodes;

import org.apache.calcite.rel.logical.LogicalJoin;


public class JoinNode extends AbstractStageNode {
  private final String _expression;

  public JoinNode(LogicalJoin node, String currentStageId) {
    super(currentStageId);
    _expression = toExpression(node);
  }

  private String toExpression(LogicalJoin node) {
    // TODO: make it real.
    return node.getDigest();
  }

  public String getExpression() {
    return _expression;
  }
}
