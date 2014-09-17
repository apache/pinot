package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BDocIdSetOperator;
import com.linkedin.pinot.core.operator.query.BAggregationFunctionOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;


/**
 * AggregationPlanNode takes care of how to apply an aggregation query to an IndexSegment.
 * 
 * @author xiafu
 *
 */
public class AggregationPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final DocIdSetPlanNode _docIdSetPlanNode;
  private final List<AggregationFunctionPlanNode> _aggregationFunctionPlanNodes =
      new ArrayList<AggregationFunctionPlanNode>();

  public AggregationPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _docIdSetPlanNode = new DocIdSetPlanNode(_indexSegment, _brokerRequest, 5000);
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = _brokerRequest.getAggregationsInfo().get(i);
      _aggregationFunctionPlanNodes.add(new AggregationFunctionPlanNode(aggregationInfo, _indexSegment,
          _docIdSetPlanNode));
    }
  }

  @Override
  public Operator run() {
    List<BAggregationFunctionOperator> aggregationFunctionOperatorList = new ArrayList<BAggregationFunctionOperator>();
    BDocIdSetOperator docIdSetOperator = (BDocIdSetOperator) _docIdSetPlanNode.run();
    for (AggregationFunctionPlanNode aggregationFunctionPlanNode : _aggregationFunctionPlanNodes) {
      aggregationFunctionOperatorList.add((BAggregationFunctionOperator) aggregationFunctionPlanNode.run());
    }
    return new MAggregationOperator(_indexSegment, _brokerRequest.getAggregationsInfo(), docIdSetOperator,
        aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MAggregationOperator");
    System.out.println(prefix + "Argument 0: DocIdSet - ");
    _docIdSetPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      System.out.println(prefix + "Argument " + (i + 1) + ": Aggregation  - ");
      _aggregationFunctionPlanNodes.get(i).showTree(prefix + "    ");
    }

  }

}
