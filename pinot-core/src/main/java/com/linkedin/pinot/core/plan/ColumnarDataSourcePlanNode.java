package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class ColumnarDataSourcePlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final String _columnName;
  private final IndexSegmentProjectionPlanNode _projectionPlanNode;

  public ColumnarDataSourcePlanNode(IndexSegment indexSegment, String columnName,
      IndexSegmentProjectionPlanNode projectionPlanNode) {
    _indexSegment = indexSegment;
    _columnName = columnName;
    _projectionPlanNode = projectionPlanNode;
  }

  @Override
  public Operator run() {
    return _indexSegment.getDataSource(_columnName, _projectionPlanNode.run());
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Columnar Reader Data Source:");
    System.out.println(prefix + "Operator: ColumnarReaderDataSource");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Column Name - " + _columnName);
    System.out.println(prefix + "Argument 2: Projection Operator - shown above");
  }

}
