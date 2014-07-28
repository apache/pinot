package com.linkedin.pinot.query.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.index.common.Operator;
import com.linkedin.pinot.index.plan.PlanNode;
import com.linkedin.pinot.query.plan.operator.MResultOperator;
import com.linkedin.pinot.query.response.InstanceResponse;


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
