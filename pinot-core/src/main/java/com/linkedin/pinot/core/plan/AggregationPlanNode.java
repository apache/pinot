package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BAggregationFunctionOperator;
import com.linkedin.pinot.core.operator.BIndexSegmentProjectionOperator;
import com.linkedin.pinot.core.operator.MAggregationOperator;


public class AggregationPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;
  private final IndexSegmentProjectionPlanNode _projectionPlanNode;
  private final List<AggregationFunctionPlanNode> _aggregationFunctionPlanNodes =
      new ArrayList<AggregationFunctionPlanNode>();

  public AggregationPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _projectionPlanNode = new IndexSegmentProjectionPlanNode(_indexSegment, _brokerRequest, 5000);
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = _brokerRequest.getAggregationsInfo().get(i);
      _aggregationFunctionPlanNodes.add(new AggregationFunctionPlanNode(aggregationInfo, _indexSegment,
          _projectionPlanNode));
    }
  }

  @Override
  public Operator run() {
    List<BAggregationFunctionOperator> aggregationFunctionOperatorList = new ArrayList<BAggregationFunctionOperator>();
    BIndexSegmentProjectionOperator projectionOperator = (BIndexSegmentProjectionOperator) _projectionPlanNode.run();
    for (AggregationFunctionPlanNode aggregationFunctionPlanNode : _aggregationFunctionPlanNodes) {
      aggregationFunctionOperatorList.add((BAggregationFunctionOperator) aggregationFunctionPlanNode.run());
    }
    return new MAggregationOperator(_indexSegment, _brokerRequest.getAggregationsInfo(), projectionOperator,
        aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MAggregationOperator");
    System.out.println(prefix + "Argument 0: Projections - ");
    _projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      System.out.println(prefix + "Argument " + (i + 1) + ": Aggregation  - ");
      _aggregationFunctionPlanNodes.get(i).showTree(prefix + "    ");
    }

  }

}
