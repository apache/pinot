package com.linkedin.pinot.index.block;

import java.util.Arrays;

import com.linkedin.pinot.index.common.Block;
import com.linkedin.pinot.index.common.BlockDocIdSet;
import com.linkedin.pinot.index.common.BlockDocIdValueSet;
import com.linkedin.pinot.index.common.BlockId;
import com.linkedin.pinot.index.common.BlockMetadata;
import com.linkedin.pinot.index.common.BlockValSet;
import com.linkedin.pinot.index.common.Predicate;


public class FakeBlock implements Block {

  int[] array;
  int start;
  int end;
  BlockId id;
  Predicate p;

  /**
   * 
   * this fake block does not take dictionary or inverted index for now
   * @param intArrayRef
   * @param start
   * @param end
   * @param index
   * @param p
   * 
   */
  public FakeBlock(int[] intArrayRef, int start, int end, int index) {
    id = new BlockId(index);
    array = intArrayRef;
    this.start = start;
    this.end = end;
    this.p = null;
  }

  /**
   * make sure you call iterator after you call apply predicte
   * in the case where you want apredicate to be set
   */
  @Override
  public boolean applyPredicate(Predicate predicate) {
    p = predicate;
    return true;
  }

  @Override
  public BlockId getId() {
    return id;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new FakeBlockValSet(Arrays.copyOfRange(array, start, end), p);
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    return new FakeBlockDocIdValueSet(Arrays.copyOfRange(array, start, end), p);
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return new FakeBlockDocIdSet(Arrays.copyOfRange(array, start, end), p);
  }

  @Override
  public BlockMetadata getMetadata() {
    return null;
  }

  @Override
  public int getIntValue(int docId) {
    if (docId >= array.length)
      return -1;
    return array[docId];
  }

  @Override
  public float getFloatValue(int docId) {
    return 0;
  }

  @Override
  public void resetBlock() {
    // TODO Auto-generated method stub

  }
}
