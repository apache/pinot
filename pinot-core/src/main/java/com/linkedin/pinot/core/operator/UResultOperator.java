package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.block.query.InstanceResponseBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;


/**
 * UResultOperator now only take one argument, wrap the operator to InstanceResponseBlock.
 * For now it's always MCombineOperator.
 * 
 * @author xiafu
 *
 */
public class UResultOperator implements Operator {

  private final Operator _operator;

  public UResultOperator(Operator combinedOperator) {
    _operator = combinedOperator;
  }

  @Override
  public boolean open() {
    _operator.open();
    return true;
  }

  @Override
  public Block nextBlock() {
    return new InstanceResponseBlock(_operator.nextBlock());
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    _operator.close();
    return true;
  }

}
