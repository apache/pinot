package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.query.AggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;


/**
 * AggregationGroupByOperatorPlanNode takes care of how to apply multiple aggregation
 * functions and groupBy query to an IndexSegment.
 *
 * @author xiafu
 *
 */
public class AggregationGroupByOperatorPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final ProjectionPlanNode _projectionPlanNode;
  private final List<AggregationFunctionGroupByPlanNode> _aggregationFunctionGroupByPlanNodes =
      new ArrayList<AggregationFunctionGroupByPlanNode>();
  private final AggregationGroupByImplementationType _aggregationGroupByImplementationType;

  public AggregationGroupByOperatorPlanNode(IndexSegment indexSegment, BrokerRequest query,
      AggregationGroupByImplementationType aggregationGroupByImplementationType) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _aggregationGroupByImplementationType = aggregationGroupByImplementationType;
    _projectionPlanNode =
        new ProjectionPlanNode(_indexSegment, getAggregationGroupByRelatedColumns(), new DocIdSetPlanNode(
            _indexSegment, _brokerRequest, 10000));
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      _aggregationFunctionGroupByPlanNodes.add(new AggregationFunctionGroupByPlanNode(_brokerRequest
          .getAggregationsInfo().get(i), _brokerRequest.getGroupBy(), _projectionPlanNode,
          _aggregationGroupByImplementationType));
    }
  }

  private String[] getAggregationGroupByRelatedColumns() {
    Set<String> aggregationGroupByRelatedColumns = new HashSet<String>();
    for (AggregationInfo aggregationInfo : _brokerRequest.getAggregationsInfo()) {
      if (aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        continue;
      }
      String columns = aggregationInfo.getAggregationParams().get("column").trim();
      aggregationGroupByRelatedColumns.addAll(Arrays.asList(columns.split(",")));
    }
    aggregationGroupByRelatedColumns.addAll(_brokerRequest.getGroupBy().getColumns());
    return aggregationGroupByRelatedColumns.toArray(new String[0]);
  }

  @Override
  public Operator run() {
    List<AggregationFunctionGroupByOperator> aggregationFunctionOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    for (AggregationFunctionGroupByPlanNode aggregationFunctionGroupByPlanNode : _aggregationFunctionGroupByPlanNodes) {
      aggregationFunctionOperatorList
          .add((AggregationFunctionGroupByOperator) aggregationFunctionGroupByPlanNode.run());
    }
    return new MAggregationGroupByOperator(_indexSegment, _brokerRequest.getAggregationsInfo(),
        _brokerRequest.getGroupBy(), _projectionPlanNode.run(), aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MAggregationGroupByOperator");
    System.out.println(prefix + "Argument 0: Projection - ");
    _projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      System.out.println(prefix + "Argument " + (i + 1) + ": AggregationGroupBy  - ");
      _aggregationFunctionGroupByPlanNodes.get(i).showTree(prefix + "    ");
    }
  }

  public enum AggregationGroupByImplementationType {
    NoDictionary,
    Dictionary,
    DictionaryAndTrie
  }
}
