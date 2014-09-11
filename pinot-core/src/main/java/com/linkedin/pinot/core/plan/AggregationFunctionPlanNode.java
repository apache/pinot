package com.linkedin.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BAggregationFunctionOperator;


public class AggregationFunctionPlanNode implements PlanNode {

  private final AggregationInfo _aggregationInfo;
  private final String[] _columns;
  private final List<ColumnarDataSourcePlanNode> _dataSources = new ArrayList<ColumnarDataSourcePlanNode>();

  public AggregationFunctionPlanNode(AggregationInfo aggregationInfo, IndexSegment indexSegment,
      IndexSegmentProjectionPlanNode projectionPlanNode) {
    _aggregationInfo = aggregationInfo;
    String columns = _aggregationInfo.getAggregationParams().get("column").trim();
    _columns = columns.split(",");
    for (int i = 0; i < _columns.length; ++i) {
      _dataSources.add(new ColumnarDataSourcePlanNode(indexSegment, _columns[i], projectionPlanNode));
    }

  }

  @Override
  public Operator run() {
    List<Operator> dataSourceOps = new ArrayList<Operator>();
    for (int i = 0; i < _dataSources.size(); ++i) {
      dataSourceOps.add(_dataSources.get(i).run());
    }
    return new BAggregationFunctionOperator(_aggregationInfo, dataSourceOps);
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Operator: BAggregationFunctionOperator");
    System.out.println(prefix + "Argument 0: Aggregation  - " + _aggregationInfo);
    for (int i = 0; i < _columns.length; ++i) {
      System.out.println(prefix + "Argument " + (i + 1) + ": DataSourceOperator - " + _columns[i]);
      _dataSources.get(i).showTree(prefix + "    ");
    }
  }

}
