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
import com.linkedin.pinot.core.operator.MProjectionOperator;
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
  private final List<AggregationFunctionPlanNode> _aggregationFunctionPlanNodes =
      new ArrayList<AggregationFunctionPlanNode>();
  private final ProjectionPlanNode _projectionPlanNode;

  public AggregationPlanNode(IndexSegment indexSegment, BrokerRequest query) {
    _indexSegment = indexSegment;
    _brokerRequest = query;
    _projectionPlanNode =
        new ProjectionPlanNode(_indexSegment, getAggregationRelatedColumns(), new DocIdSetPlanNode(_indexSegment,
            _brokerRequest, 5000));
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      AggregationInfo aggregationInfo = _brokerRequest.getAggregationsInfo().get(i);
      _aggregationFunctionPlanNodes.add(new AggregationFunctionPlanNode(aggregationInfo, _projectionPlanNode));
    }
  }

  private String[] getAggregationRelatedColumns() {
    Set<String> aggregationRelatedColumns = new HashSet<String>();
    for (AggregationInfo aggregationInfo : _brokerRequest.getAggregationsInfo()) {
      if (!aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
        String columns = aggregationInfo.getAggregationParams().get("column").trim();
        aggregationRelatedColumns.addAll(Arrays.asList(columns.split(",")));
      }
    }
    return aggregationRelatedColumns.toArray(new String[0]);
  }

  @Override
  public Operator run() {
    List<BAggregationFunctionOperator> aggregationFunctionOperatorList = new ArrayList<BAggregationFunctionOperator>();
    for (AggregationFunctionPlanNode aggregationFunctionPlanNode : _aggregationFunctionPlanNodes) {
      aggregationFunctionOperatorList.add((BAggregationFunctionOperator) aggregationFunctionPlanNode.run());
    }
    return new MAggregationOperator(_indexSegment, _brokerRequest.getAggregationsInfo(),
        (MProjectionOperator) _projectionPlanNode.run(), aggregationFunctionOperatorList);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Inner-Segment Plan Node :");
    System.out.println(prefix + "Operator: MAggregationOperator");
    System.out.println(prefix + "Argument 0: Projection - ");
    _projectionPlanNode.showTree(prefix + "    ");
    for (int i = 0; i < _brokerRequest.getAggregationsInfo().size(); ++i) {
      System.out.println(prefix + "Argument " + (i + 1) + ": Aggregation  - ");
      _aggregationFunctionPlanNodes.get(i).showTree(prefix + "    ");
    }

  }

}
