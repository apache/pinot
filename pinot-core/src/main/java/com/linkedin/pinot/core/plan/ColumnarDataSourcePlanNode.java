package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.UReplicatedDocIdSetOperator;


/**
 * ColumnarDataSourcePlanNode will take docIdSetPlanNode as input and replicate
 * BDocIdSetOperator as an input for ColumnarReaderDataSource.
 * 
 * @author xiafu
 *
 */
public class ColumnarDataSourcePlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final String _columnName;
  private final DocIdSetPlanNode _docIdSetPlanNode;

  public ColumnarDataSourcePlanNode(IndexSegment indexSegment, String columnName, DocIdSetPlanNode docIdSetPlanNode) {
    _indexSegment = indexSegment;
    _columnName = columnName;
    _docIdSetPlanNode = docIdSetPlanNode;
  }

  @Override
  public Operator run() {
    return _indexSegment.getDataSource(_columnName, new UReplicatedDocIdSetOperator(_docIdSetPlanNode.run()));
  }

  @Override
  public void showTree(String prefix) {
    System.out.println(prefix + "Columnar Reader Data Source:");
    System.out.println(prefix + "Operator: ColumnarReaderDataSource");
    System.out.println(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    System.out.println(prefix + "Argument 1: Column Name - " + _columnName);
    System.out.println(prefix + "Argument 2: Replicated DocIdSet Operator - shown above");
  }

}
