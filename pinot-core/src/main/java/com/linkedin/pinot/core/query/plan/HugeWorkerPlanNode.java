package com.linkedin.pinot.core.query.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.plan.operator.UAggregationAndSelectionOperator;


public class HugeWorkerPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final PlanNode _filterNode;

  public HugeWorkerPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _filterNode = new FilterPlanNode(_indexSegment, _brokerRequest);
  }

  @Override
  public Operator run() {
    return new UAggregationAndSelectionOperator(_indexSegment, _brokerRequest, _filterNode.run());
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Global Plan Node");
    System.out.println(prefix + "Operator: AggregationAndSelectionOperator");
    System.out.println(prefix + "Argument 1: Aggregations - " + _brokerRequest.getAggregationsInfo());
    System.out.println(prefix + "Argument 2: Selections - " + _brokerRequest.getSelections());
    System.out.println(prefix + "Argument 3: FilterPlanNode :(see below)");
    _filterNode.showTree(prefix + "    ");
  }

}
