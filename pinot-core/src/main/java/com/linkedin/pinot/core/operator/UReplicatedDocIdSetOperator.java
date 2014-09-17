package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;


/**
 * In columnar data store, we don't want to filter multiple times for multiple
 * DataSources. So we use BDocIdSetOperator as a cache of docIdSetBlock. So
 * DocIdSet is re-accessable for multiple DataSources.
 *  
 * BReplicatedDocIdSetOperator will take BDocIdSetOperator as input and is the
 * input for ColumnarReaderDataSource.
 * It will always return the current block from BDocIdSetOperator.
 * So ALWAYS call BDocIdSetOperator.nextBlock() BEFORE calling this class.
 * 
 * 
 * @author xiafu
 *
 */
public class UReplicatedDocIdSetOperator implements Operator {

  private final BDocIdSetOperator _docIdSetOperator;

  public UReplicatedDocIdSetOperator(Operator docIdSetOperator) {
    _docIdSetOperator = (BDocIdSetOperator) docIdSetOperator;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    return _docIdSetOperator.getCurrentDocIdSetBlock();
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    return true;
  }

}
