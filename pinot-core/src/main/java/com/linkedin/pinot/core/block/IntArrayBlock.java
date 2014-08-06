package com.linkedin.pinot.core.block;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;


/**
 * Uses array to represent data and index
 * 
 * @author kgopalak
 * 
 */
public class IntArrayBlock implements Block {

  private final int[] data;
  private final int[] filteredDocIds;
  private final BlockId blockId;

  public IntArrayBlock(BlockId id, int data[], int[] filteredDocIds) {
    this.blockId = id;
    this.data = data;
    this.filteredDocIds = filteredDocIds;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public int getIntValue(int docId) {
    return data[docId];
  }

  @Override
  public float getFloatValue(int docId) {
    return 0;
  }

  @Override
  public BlockId getId() {
    return blockId;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return null;
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    return null;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return new IntBlockDocIdSet(filteredDocIds);
  }

  @Override
  public BlockMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void resetBlock() {
    throw new UnsupportedOperationException("reset block is not yet supported");
  }

}
