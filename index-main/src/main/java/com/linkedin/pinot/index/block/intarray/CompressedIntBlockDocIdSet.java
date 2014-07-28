package com.linkedin.pinot.index.block.intarray;

import java.nio.ByteBuffer;
import java.util.Map;

import com.linkedin.pinot.index.block.intarray.utils.SortedBlockDocIdSet;
import com.linkedin.pinot.index.block.intarray.utils.UnSortedBlockDocIdSet;
import com.linkedin.pinot.index.common.BlockDocIdIterator;
import com.linkedin.pinot.index.common.BlockDocIdSet;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;
import com.linkedin.pinot.segments.v1.segment.utils.SortedIntArray;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class CompressedIntBlockDocIdSet implements BlockDocIdSet {

  IntArray intArray;
  Dictionary<?> dictionary;
  int start, end;
  Predicate p;

  public CompressedIntBlockDocIdSet(IntArray intArray, Dictionary<?> dictionary, int start, int end, Predicate p) {
    this.intArray = intArray;
    this.dictionary = dictionary;
    this.start = start;
    this.end = end;
    this.p = p;
  }
  
  private BlockDocIdIterator unsortedIterator() {
    if (p == null) {
      return UnSortedBlockDocIdSet.getDefaultIterator(intArray, start, end);
    }

    switch (p.getType()) {
      case EQ:
        int equalsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockDocIdSet.getEqualityMatchIterator(intArray, start, end, equalsLookup);
      case NEQ:
        int notEqualsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockDocIdSet.getNotEqualsMatchIterator(intArray, start, end, notEqualsLookup);
      default:
        throw new UnsupportedOperationException("current I don't support predicate type : " + p.getType());
    }
  }

  private BlockDocIdIterator sortedIterator() {
    if (p == null) {
      return SortedBlockDocIdSet.getDefaultIterator((SortedIntArray) intArray, start, end);
    }

    switch (p.getType()) {
      case EQ:
        int equalsLookup = dictionary.indexOf(p.getRhs().get(0));
        return SortedBlockDocIdSet.getEqualityMatchIterator((SortedIntArray) intArray, start, end, equalsLookup);
      case NEQ:
        int notEqualsLookup = dictionary.indexOf(p.getRhs().get(0));
        return SortedBlockDocIdSet.getNotEqualsMatchIterator((SortedIntArray) intArray, start, end, notEqualsLookup);
      default:
        throw new UnsupportedOperationException("current I don't support predicate type : " + p.getType());
    }
  }

  @Override
  public BlockDocIdIterator iterator() {
    //    if (intArray instanceof SortedIntArray) {
    //      return sortedIterator();
    //    }
    return unsortedIterator();
  }
}
