package com.linkedin.pinot.core.plan;

import org.apache.log4j.Logger;

import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.UResultOperator;


/**
 * The root node of an instance query plan.
 *
 * @author xiafu
 *
 */
public class InstanceResponsePlanNode implements PlanNode {
  private static final Logger _logger = Logger.getLogger("QueryPlanLog");
  private CombinePlanNode _planNode;

  public void setPlanNode(CombinePlanNode combinePlanNode) {
    _planNode = combinePlanNode;
  }

  public PlanNode getPlanNode() {
    return _planNode;
  }

  @Override
  public Operator run() {
    return new UResultOperator(_planNode.run());
  }

  @Override
  public void showTree(String prefix) {
    _logger.debug(prefix + "Instance Level Inter-Segments Query Plan Node: ");
    _logger.debug(prefix + "Operator: UResultOperator");
    _logger.debug(prefix + "Argument 0:");
    _planNode.showTree(prefix + "    ");
  }

}
