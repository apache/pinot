package com.linkedin.pinot.core.query.plan;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationService;
import com.linkedin.pinot.core.query.plan.operator.UAggregationAndSelectionOperator;


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
