package com.linkedin.pinot.index.block.intarray;

import java.util.Map;

import com.linkedin.pinot.index.block.intarray.utils.UnSortedBlockDocIdSet;
import com.linkedin.pinot.index.common.BlockDocIdIterator;
import com.linkedin.pinot.index.common.BlockDocIdSet;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;



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

  public CompressedIntBlockDocIdSet(IntArray intArray, Dictionary<?> dictionary, int start, int end,
      Predicate p) {
    this.intArray = intArray;
    this.dictionary = dictionary;
    this.start = start;
    this.end = end;
    this.p = p;
  }

  @Override
  public BlockDocIdIterator iterator() {
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
}
