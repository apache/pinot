package com.linkedin.pinot.core.operator;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;


public class IntBlockDocIdSet implements BlockDocIdSet {

  private final MutableRoaringBitmap bit;

  public IntBlockDocIdSet(MutableRoaringBitmap bitmap) {
    bit = bitmap;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return BlockUtils.getBLockDocIdSetBackedByBitmap(bit).iterator();
  }

  @Override
  public Object getRaw() {
    return bit;
  }
}
