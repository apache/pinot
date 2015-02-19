package com.linkedin.pinot.core.plan;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.MDefaultAggregationFunctionGroupByOperator;
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

  private static final Logger _logger = Logger.getLogger("QueryPlanLog");
  private final AggregationInfo _aggregationInfo;
  private final GroupBy _groupBy;
  private final AggregationGroupByImplementationType _aggregationGroupByImplementationType;
  private final ProjectionPlanNode _projectionPlanNode;
  private final boolean _hasDictionary;

  public AggregationFunctionGroupByPlanNode(AggregationInfo aggregationInfo, GroupBy groupBy,
      ProjectionPlanNode projectionPlanNode, AggregationGroupByImplementationType aggregationGroupByImplementationType, boolean hasDictionary) {
    _aggregationInfo = aggregationInfo;
    _groupBy = groupBy;
    _aggregationGroupByImplementationType = aggregationGroupByImplementationType;
    _projectionPlanNode = projectionPlanNode;
    _hasDictionary = hasDictionary;
  }

  @Override
  public Operator run() {
    switch (_aggregationGroupByImplementationType) {
      case NoDictionary:
        return new MDefaultAggregationFunctionGroupByOperator(_aggregationInfo, _groupBy, new UReplicatedProjectionOperator(
            (MProjectionOperator) _projectionPlanNode.run()), _hasDictionary);
      case Dictionary:
        return new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfo, _groupBy,
            new UReplicatedProjectionOperator((MProjectionOperator) _projectionPlanNode.run()), _hasDictionary);
      case DictionaryAndTrie:
        return new MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(_aggregationInfo, _groupBy,
            new UReplicatedProjectionOperator((MProjectionOperator) _projectionPlanNode.run()), _hasDictionary);
      default:
        throw new UnsupportedOperationException("Not Support AggregationGroupBy implmentation: "
            + _aggregationGroupByImplementationType);
    }
  }

  @Override
  public void showTree(String prefix) {
    switch (_aggregationGroupByImplementationType) {
      case NoDictionary:
        _logger.debug(prefix + "Operator: MAggregationFunctionGroupByOperator");
        break;
      case Dictionary:
        _logger.debug(prefix + "Operator: MAggregationFunctionGroupByWithDictionaryOperator");
        break;
      case DictionaryAndTrie:
        _logger.debug(prefix + "Operator: MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator");
        break;
      default:
        throw new UnsupportedOperationException("Not Support AggregationGroupBy implmentation: "
            + _aggregationGroupByImplementationType);
    }

    _logger.debug(prefix + "Argument 0: Aggregation  - " + _aggregationInfo);
    _logger.debug(prefix + "Argument 1: GroupBy  - " + _groupBy);
    _logger.debug(prefix + "Argument 2: Projection - Shown Above");
  }

}
