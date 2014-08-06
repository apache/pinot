package com.linkedin.pinot.core.query.plan.operator;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class UAggregationAndSelectionOperator implements Operator {

  private final Operator _filterOperators;
  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;

  public UAggregationAndSelectionOperator(IndexSegment indexSegment, BrokerRequest brokerRequest,
      Operator filterOperator) {
    _brokerRequest = brokerRequest;
    _indexSegment = indexSegment;
    _filterOperators = filterOperator;
  }

  public UAggregationAndSelectionOperator(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _brokerRequest = brokerRequest;
    _filterOperators = null;
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
