package com.linkedin.pinot.core.query.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.plan.operator.MResultOperator;


public class ResultPlanNode implements PlanNode {

  private List<PlanNode> _planNodeList = new ArrayList<PlanNode>();
  private InstanceResponse _segmentResponse = new InstanceResponse();

  public void addPlanNode(PlanNode planNode) {
    _planNodeList.add(planNode);
  }

  public List<PlanNode> getPlanNodeList() {
    return _planNodeList;
  }

  @Override
  public Operator run() {
    List<Operator> retOperators = new ArrayList<Operator>();
    for (PlanNode planNode : _planNodeList) {
      retOperators.add(planNode.run());
    }
    return new MResultOperator(retOperators);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println("The Whole Result Plan : ");
    for (PlanNode planNode : _planNodeList) {
      planNode.showTree(prefix + "    ");
    }
  }

}
