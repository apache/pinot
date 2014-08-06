package com.linkedin.pinot.core.query.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.plan.operator.UAggregationAndSelectionOperator;


public class SelectionPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final PlanNode _filterNode;

  public SelectionPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _filterNode = new FilterPlanNode(_indexSegment, _brokerRequest);
  }

  @Override
  public Operator run() {
    if (_filterNode != null) {
      return new UAggregationAndSelectionOperator(_indexSegment, _brokerRequest, _filterNode.run());
    } else {
      return new UAggregationAndSelectionOperator(_indexSegment, _brokerRequest);
    }

  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Global Plan Node");
    System.out.println(prefix + "Operator: AggregationAndSelectionOperator");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Selections - " + _brokerRequest.getSelections());
    if (_filterNode != null) {
      System.out.println(prefix + "Argument 2: FilterPlanNode :(see below)");
      _filterNode.showTree(prefix + "    ");
    }
  }

}
