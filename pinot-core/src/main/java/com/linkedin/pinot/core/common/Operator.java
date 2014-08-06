package com.linkedin.pinot.core.common;

public interface Operator {
  /*
   * allows the operator to set up/initialize processing
   */
  public boolean open();

  /**
   * Get the next non empty block, if there are additional predicates the
   * operator is responsible to apply the predicate and return the block that
   * has atleast one doc that satisfies the predicate
   * 
   * @return
   */
  public Block nextBlock();

  /**
   * Same as nextBlock but the caller specifies the BlockId to start from
   * TODO: Better to specify the docId and let the operator decide the block
   * to return. This may not be a problem now but when we add join, blockId
   * may not mean anything across different tables
   * @param BlockId
   * @return
   */
  public Block nextBlock(BlockId BlockId);

  public boolean close();
}
