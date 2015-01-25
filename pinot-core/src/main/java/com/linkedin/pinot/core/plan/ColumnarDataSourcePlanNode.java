package com.linkedin.pinot.core.plan;

import org.apache.log4j.Logger;

import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * ColumnarDataSourcePlanNode will take docIdSetPlanNode as input and replicate
 * BDocIdSetOperator as an input for ColumnarReaderDataSource.
 * 
 * @author xiafu
 *
 */
public class ColumnarDataSourcePlanNode implements PlanNode {
  private static final Logger _logger = Logger.getLogger("QueryPlanLog");
  private final IndexSegment _indexSegment;
  private final String _columnName;

  public ColumnarDataSourcePlanNode(IndexSegment indexSegment, String columnName) {
    _indexSegment = indexSegment;
    _columnName = columnName;
  }

  public ColumnarDataSourcePlanNode(IndexSegment indexSegment, String columnName, DocIdSetPlanNode docIdSetPlanNode) {
    _indexSegment = indexSegment;
    _columnName = columnName;
  }

  @Override
  public Operator run() {
    return _indexSegment.getDataSource(_columnName);
  }

  @Override
  public void showTree(String prefix) {
    _logger.debug(prefix + "Columnar Reader Data Source:");
    _logger.debug(prefix + "Operator: ColumnarReaderDataSource");
    _logger.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    _logger.debug(prefix + "Argument 1: Column Name - " + _columnName);
  }

}
