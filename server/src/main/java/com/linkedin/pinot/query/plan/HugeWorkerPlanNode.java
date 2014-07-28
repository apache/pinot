package com.linkedin.pinot.query.plan;

import com.linkedin.pinot.index.common.Operator;
import com.linkedin.pinot.index.plan.FilterPlanNode;
import com.linkedin.pinot.index.plan.PlanNode;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.aggregation.AggregationFunction;
import com.linkedin.pinot.query.aggregation.AggregationService;
import com.linkedin.pinot.query.plan.operator.UAggregationAndSelectionOperator;
import com.linkedin.pinot.query.request.Query;


public class HugeWorkerPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final Query _query;
  private final PlanNode _filterNode;

  public HugeWorkerPlanNode(IndexSegment indexSegment, Query query) {
    _indexSegment = indexSegment;
    _query = query;
    _filterNode = new FilterPlanNode(_indexSegment, _query.getFilterQuery());
  }

  @Override
  public Operator run() {
    return new UAggregationAndSelectionOperator(_indexSegment, _query, _filterNode.run());
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Global Plan Node");
    System.out.println(prefix + "Operator: AggregationAndSelectionOperator");
    System.out.println(prefix + "Argument 1: Aggregations - " + _query.getAggregationsInfo());
    System.out.println(prefix + "Argument 2: Selections - " + _query.getSelections());
    System.out.println(prefix + "Argument 3: FilterPlanNode :(see below)");
    _filterNode.showTree(prefix + "    ");
  }

}
