package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.BAggregationFunctionOperator;


/**
 * AggregationFunctionPlanNode takes care of how to apply one aggregation
 * function for given data sources.
 *
 * @author xiafu
 *
 */
public class AggregationFunctionPlanNode implements PlanNode {

  private final AggregationInfo _aggregationInfo;
  private final ProjectionPlanNode _projectionPlanNode;

  public AggregationFunctionPlanNode(AggregationInfo aggregationInfo, ProjectionPlanNode projectionPlanNode) {
    _aggregationInfo = aggregationInfo;
    _projectionPlanNode = projectionPlanNode;
  }

  @Override
  public Operator run()  {
    return new BAggregationFunctionOperator(_aggregationInfo, new UReplicatedProjectionOperator(
        (MProjectionOperator) _projectionPlanNode.run()));
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Operator: BAggregationFunctionOperator");
    System.out.println(prefix + "Argument 0: Aggregation  - " + _aggregationInfo);
    System.out.println(prefix + "Argument 1: Projection - Shown Above");
  }

}
