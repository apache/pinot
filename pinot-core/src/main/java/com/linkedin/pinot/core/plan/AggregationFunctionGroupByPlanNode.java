package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
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
  private final List<ColumnarDataSourcePlanNode> _dataSourcesList = new ArrayList<ColumnarDataSourcePlanNode>();
  private final AggregationGroupByImplementationType _aggregationGroupByImplementationType;

  public AggregationFunctionGroupByPlanNode(AggregationInfo aggregationInfo, GroupBy groupBy,
      IndexSegment indexSegment, DocIdSetPlanNode docIdSetPlanNode,
      AggregationGroupByImplementationType aggregationGroupByImplementationType) {
    _aggregationInfo = aggregationInfo;
    _groupBy = groupBy;
    _aggregationGroupByImplementationType = aggregationGroupByImplementationType;
    // Adding groupBy related columns.
    for (String column : _groupBy.getColumns()) {
      _dataSourcesList.add(new ColumnarDataSourcePlanNode(indexSegment, column, docIdSetPlanNode));
    }

    // Adding AggregationFunction related columns.
    if (!_aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
      String columns = _aggregationInfo.getAggregationParams().get("column").trim();
      for (String column : columns.split(",")) {
        _dataSourcesList.add(new ColumnarDataSourcePlanNode(indexSegment, column, docIdSetPlanNode));
      }
    }
  }

  @Override
  public Operator run() {
    List<Operator> dataSourceOpsList = new ArrayList<Operator>();
    for (PlanNode dataSource : _dataSourcesList) {
      dataSourceOpsList.add(dataSource.run());
    }
    switch (_aggregationGroupByImplementationType) {
      case NoDictionary:
        return new MAggregationFunctionGroupByOperator(_aggregationInfo, _groupBy, dataSourceOpsList);
      case Dictionary:
        return new MAggregationFunctionGroupByWithDictionaryOperator(_aggregationInfo, _groupBy, dataSourceOpsList);
      case DictionaryAndTrie:
        return new MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(_aggregationInfo, _groupBy,
            dataSourceOpsList);
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
    System.out.println(prefix + "Argument 2: Replicated DocIdSet Opearator: shown above");
    for (int i = 0; i < _dataSourcesList.size(); ++i) {
      System.out.println(prefix + "Argument " + (2 + 1) + ": DataSourceOperator - " + _dataSourcesList.get(i));
      _dataSourcesList.get(i).showTree(prefix + "    ");
    }
  }

}
