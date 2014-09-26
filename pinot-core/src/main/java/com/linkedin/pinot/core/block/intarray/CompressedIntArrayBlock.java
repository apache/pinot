package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.columnar.BitmapInvertedIndex;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class CompressedIntArrayBlock implements Block {

  final IntArray intArray;
  final int start;
  final int end;
  final BlockId id;
  Predicate p;
  final Dictionary<?> dictionary;
  final BitmapInvertedIndex invertedIdx;

  /**
   *
   * This fake block does not take dictionary or inverted index for now
   * @param intArrayRef
   * @param start
   * @param end
   * @param index
   * @param p
   *
   */
  public CompressedIntArrayBlock(IntArray forwardIndex, Dictionary<?> dictionary, int start, int end, int index,
      BitmapInvertedIndex invertedIndex) {
    id = new BlockId(index);
    intArray = forwardIndex;
    this.start = start;
    this.end = end;
    p = null;
    this.dictionary = dictionary;
    invertedIdx = invertedIndex;
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
    return new CompressedIntBlockValSet(intArray, dictionary, start, end, p);
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    return new CompressedIntBlockDocIdValueSet(intArray, dictionary, start, end, p);
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    return new CompressedIntBlockDocIdSet(intArray, dictionary, start, end, p, invertedIdx);
  }

  @Override
  public BlockMetadata getMetadata() {
    return null;
  }
}
