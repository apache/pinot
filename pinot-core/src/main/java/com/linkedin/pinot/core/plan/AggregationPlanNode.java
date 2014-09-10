package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.UAggregationOperator;


public class AggregationPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final PlanNode _filterNode;

  public AggregationPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    if (_brokerRequest.isSetFilterQuery()) {
      _filterNode = new FilterPlanNode(_indexSegment, _brokerRequest);
    } else {
      _filterNode = null;
    }
  }

  @Override
  public Operator run() {
    if (_filterNode != null) {
      return new UAggregationOperator(_indexSegment, _brokerRequest.getAggregationsInfo(), _filterNode.run());
    } else {
      return new UAggregationOperator(_indexSegment, _brokerRequest.getAggregationsInfo(), null);
    }

  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: UAggregationOperator");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Aggregations - " + _brokerRequest.getAggregationsInfo());
    if (_filterNode != null) {
      System.out.println(prefix + "Argument 2: FilterPlanNode :(see below)");
      _filterNode.showTree(prefix + "    ");
    }
  }

}
