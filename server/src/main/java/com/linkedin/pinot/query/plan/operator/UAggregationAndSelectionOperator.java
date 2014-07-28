package com.linkedin.pinot.query.plan.operator;

import java.util.List;

import com.linkedin.pinot.index.common.Block;
import com.linkedin.pinot.index.common.BlockId;
import com.linkedin.pinot.index.common.Operator;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.query.request.Query;


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
