package com.linkedin.pinot.core.plan;

import org.apache.log4j.Logger;

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
  private static final Logger _logger = Logger.getLogger("QueryPlanLog");
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
    _logger.debug(prefix + "Operator: BAggregationFunctionOperator");
    _logger.debug(prefix + "Argument 0: Aggregation  - " + _aggregationInfo);
    _logger.debug(prefix + "Argument 1: Projection - Shown Above");
  }

}
