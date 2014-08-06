package com.linkedin.pinot.core.block;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;


public class IntBlockDocIdSet implements BlockDocIdSet {

  private final int[] docIdArray;

  public IntBlockDocIdSet(int[] docIdArray) {
    this.docIdArray = docIdArray;

  }

  @Override
  public BlockDocIdIterator iterator() {
    return new IntBlockDocIdIterator(docIdArray);
  }

}
