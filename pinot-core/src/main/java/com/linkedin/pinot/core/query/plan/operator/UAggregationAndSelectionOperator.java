package com.linkedin.pinot.core.query.plan.operator;

import java.util.List;

import com.linkedin.pinot.common.query.request.Query;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class UAggregationAndSelectionOperator implements Operator {

  private final Operator _filterOperators;
  private final IndexSegment _indexSegment;
  private final Query _query;

  public UAggregationAndSelectionOperator(IndexSegment indexSegment, Query query, Operator filterOperator) {
    _query = query;
    _indexSegment = indexSegment;
    _filterOperators = filterOperator;
  }

  @Override
  public boolean open() {
    _filterOperators.open();
    return true;
  }

  @Override
  public Block nextBlock() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean close() {
    _filterOperators.close();
    return true;
  }

}
