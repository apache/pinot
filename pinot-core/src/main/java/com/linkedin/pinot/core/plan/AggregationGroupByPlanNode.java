package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.UAggregationGroupByOperator;


public class AggregationGroupByPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final PlanNode _filterNode;

  public AggregationGroupByPlanNode(IndexSegment indexSegment, BrokerRequest query) {
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
      return new UAggregationGroupByOperator(_indexSegment, _brokerRequest, _filterNode.run());
    } else {
      return new UAggregationGroupByOperator(_indexSegment, _brokerRequest);
    }

  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: UAggregationGroupByOperator");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Aggregations - " + _brokerRequest.getAggregationsInfo());
    System.out.println(prefix + "Argument 2: GroupBys - " + _brokerRequest.getGroupBy());
    if (_filterNode != null) {
      System.out.println(prefix + "Argument 3: FilterPlanNode :(see below)");
      _filterNode.showTree(prefix + "    ");
    }
  }

}
