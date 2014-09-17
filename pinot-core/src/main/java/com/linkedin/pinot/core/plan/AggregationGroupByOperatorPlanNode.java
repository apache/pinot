package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BDocIdSetOperator;
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
  private final DocIdSetPlanNode _docIdSetPlanNode;
  private final List<AggregationFunctionGroupByPlanNode> _aggregationFunctionGroupByPlanNodes =
      new ArrayList<AggregationFunctionGroupByPlanNode>();
  private final AggregationGroupByImplementationType _aggregationGroupByImplementationType;

  public AggregationGroupByOperatorPlanNode(IndexSegment indexSegment, BrokerRequest query,
      AggregationGroupByImplementationType aggregationGroupByImplementationType) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _aggregationGroupByImplementationType = aggregationGroupByImplementationType;
    _docIdSetPlanNode = new DocIdSetPlanNode(_indexSegment, _brokerRequest, 10000);
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      _aggregationFunctionGroupByPlanNodes.add(new AggregationFunctionGroupByPlanNode(_brokerRequest
          .getAggregationsInfo().get(i), _brokerRequest.getGroupBy(), indexSegment, _docIdSetPlanNode,
          _aggregationGroupByImplementationType));
    }
  }

  @Override
  public Operator run() {
    List<AggregationFunctionGroupByOperator> aggregationFunctionOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    BDocIdSetOperator docIdSetOperator = (BDocIdSetOperator) _docIdSetPlanNode.run();
    for (AggregationFunctionGroupByPlanNode aggregationFunctionGroupByPlanNode : _aggregationFunctionGroupByPlanNodes) {
      aggregationFunctionOperatorList
          .add((AggregationFunctionGroupByOperator) aggregationFunctionGroupByPlanNode.run());
    }
    return new MAggregationGroupByOperator(_indexSegment, _brokerRequest.getAggregationsInfo(),
        _brokerRequest.getGroupBy(), docIdSetOperator, aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MAggregationGroupByOperator");
    System.out.println(prefix + "Argument 0: DocIdSet - ");
    _docIdSetPlanNode.showTree(prefix + "    ");
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
