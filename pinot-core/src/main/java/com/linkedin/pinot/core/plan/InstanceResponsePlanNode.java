package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.UResultOperator;


/**
 * The root node of an instance query plan.
 *
 * @author xiafu
 *
 */
public class InstanceResponsePlanNode implements PlanNode {

  private CombinePlanNode _planNode;

  public void setPlanNode(CombinePlanNode combinePlanNode) {
    _planNode = combinePlanNode;
  }

  public PlanNode getPlanNode() {
    return _planNode;
  }

  @Override
  public Operator run() throws Exception {
    return new UResultOperator(_planNode.run());
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Instance Level Inter-Segments Query Plan Node: ");
    System.out.println(prefix + "Operator: UResultOperator");
    System.out.println(prefix + "Argument 0:");
    _planNode.showTree(prefix + "    ");
  }

}
