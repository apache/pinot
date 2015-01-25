package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByWithDictionaryOperator;
import com.linkedin.pinot.core.plan.AggregationGroupByOperatorPlanNode.AggregationGroupByImplementationType;


/**
 * AggregationFunctionGroupByPlanNode takes care of how to apply one aggregation
 * function and the groupby query to an IndexSegment.
 *
 * @author xiafu
 *
 */
public class AggregationFunctionGroupByPlanNode implements PlanNode {

  private final AggregationInfo _aggregationInfo;
  private final GroupBy _groupBy;
  private final AggregationGroupByImplementationType _aggregationGroupByImplementationType;
  private final ProjectionPlanNode _projectionPlanNode;

  public AggregationFunctionGroupByPlanNode(AggregationInfo aggregationInfo, GroupBy groupBy,
      ProjectionPlanNode projectionPlanNode, AggregationGroupByImplementationType aggregationGroupByImplementationType) {
    _aggregationInfo = aggregationInfo;
    _groupBy = groupBy;
    _aggregationGroupByImplementationType = aggregationGroupByImplementationType;
    _projectionPlanNode = projectionPlanNode;
  }

  @Override
  public Operator run() throws Exception {
    switch (_aggregationGroupByImplementationType) {
      case NoDictionary:
        return new MAggregationFunctionGroupByOperator(_aggregationInfo, _groupBy, new UReplicatedProjectionOperator(
            (MProjectionOperator) _projectionPlanNode.run()));
      case Dictionary:
        return new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfo, _groupBy,
            new UReplicatedProjectionOperator((MProjectionOperator) _projectionPlanNode.run()));
      case DictionaryAndTrie:
        return new MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(_aggregationInfo, _groupBy,
            new UReplicatedProjectionOperator((MProjectionOperator) _projectionPlanNode.run()));
      default:
        throw new UnsupportedOperationException("Not Support AggregationGroupBy implmentation: "
            + _aggregationGroupByImplementationType);
    }
  }

  @Override
  public void showTree(String prefix) {
    switch (_aggregationGroupByImplementationType) {
      case NoDictionary:
        System.out.println(prefix + "Operator: MAggregationFunctionGroupByOperator");
        break;
      case Dictionary:
        System.out.println(prefix + "Operator: MAggregationFunctionGroupByWithDictionaryOperator");
        break;
      case DictionaryAndTrie:
        System.out.println(prefix + "Operator: MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator");
        break;
      default:
        throw new UnsupportedOperationException("Not Support AggregationGroupBy implmentation: "
            + _aggregationGroupByImplementationType);
    }

    System.out.println(prefix + "Argument 0: Aggregation  - " + _aggregationInfo);
    System.out.println(prefix + "Argument 1: GroupBy  - " + _groupBy);
    System.out.println(prefix + "Argument 2: Projection - Shown Above");
  }

}
