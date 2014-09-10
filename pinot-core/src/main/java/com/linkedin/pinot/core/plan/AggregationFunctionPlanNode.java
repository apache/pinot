package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.BAggregationFunctionOperator;


public class AggregationFunctionPlanNode implements PlanNode {

  private final AggregationInfo _aggregationInfo;
  private final IndexSegmentProjectionPlanNode _projectionPlanNode;

  public AggregationFunctionPlanNode(AggregationInfo aggregationInfo, IndexSegmentProjectionPlanNode projectionPlanNode) {
    _aggregationInfo = aggregationInfo;
    _projectionPlanNode = projectionPlanNode;
  }

  @Override
  public Operator run() {
    return new BAggregationFunctionOperator(_aggregationInfo, _projectionPlanNode.run());
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Operator: BAggregationFunctionOperator");
    System.out.println(prefix + "Argument 0: Aggregation  - " + _aggregationInfo);
    System.out.println(prefix + "Argument 1: Replicated Projection Opearator: shown above");
  }

}
