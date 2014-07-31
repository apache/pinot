package com.linkedin.pinot.core.query.plan.operator;

import java.util.List;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;


public class MResultOperator implements Operator {

  private final List<Operator> _operators;

  public MResultOperator(List<Operator> retOperators) {
    _operators = retOperators;
  }

  @Override
  public boolean open() {
    for (Operator op : _operators) {
      op.open();
    }
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
    for (Operator op : _operators) {
      op.close();
    }
    return true;
  }

}
