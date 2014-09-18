package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;


/**
 * UReplicatedProjectionOperator is used by AggregationFunctionOperator and
 * AggregationFunctionGroupByOperator as a copy of MProjectionOperator.
 * nextBlock() here returns currentBlock in MProjectionOperator.
 * 
 * @author xiafu
 *
 */
public class UReplicatedProjectionOperator implements DataSource {

  private final MProjectionOperator _projectionOperator;

  public UReplicatedProjectionOperator(MProjectionOperator projectionOperator) {
    _projectionOperator = projectionOperator;
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public Block nextBlock() {
    return _projectionOperator.getCurrentBlock();
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException("Not supported in MProjectionOperator!");
  }

  public MProjectionOperator getProjectionOperator() {
    return _projectionOperator;
  }
}
