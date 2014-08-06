package com.linkedin.pinot.core.common;

public interface Block {

  // boolean applyPredicate(Block resultBlock, Predicate predicate);

  boolean applyPredicate(Predicate predicate);

  BlockId getId();

  BlockValSet getBlockValueSet();

  BlockDocIdValueSet getBlockDocIdValueSet();

  BlockDocIdSet getBlockDocIdSet();

  BlockMetadata getMetadata();

  /**
   *
   * This should be pushed down to the sets
   *
   */
  int getIntValue(int docId);

  float getFloatValue(int docId);

  void resetBlock();

  // block val info
}
