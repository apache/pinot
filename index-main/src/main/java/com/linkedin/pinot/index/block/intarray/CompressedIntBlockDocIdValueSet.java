package com.linkedin.pinot.index.block.intarray;

import java.util.Map;

import com.linkedin.pinot.index.block.intarray.utils.UnSortedBlockDocIdValSet;
import com.linkedin.pinot.index.common.BlockDocIdValueIterator;
import com.linkedin.pinot.index.common.BlockDocIdValueSet;
import com.linkedin.pinot.index.common.Constants;
import com.linkedin.pinot.index.common.Pairs;
import com.linkedin.pinot.index.common.Pairs.IntPair;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class CompressedIntBlockDocIdValueSet implements BlockDocIdValueSet {

  IntArray intArray;
  Dictionary<?> dictionary;
  int start, end;
  Predicate p;

  public CompressedIntBlockDocIdValueSet(IntArray intArray, Dictionary<?> dictionary, int start, int end,
      Predicate p) {
    this.intArray = intArray;
    this.dictionary = dictionary;
    this.start = start;
    this.end = end;
    this.p = p;
  }

  @Override
  public BlockDocIdValueIterator iterator() {
    if (p == null) {
      return UnSortedBlockDocIdValSet.getDefaultIterator(intArray, start, end);
    }

    switch (p.getType()) {
      case EQ:
        int equalsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockDocIdValSet.getEqualityMatchIterator(intArray, start, end, equalsLookup);
      case NEQ:
        int notEqualsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockDocIdValSet.getNotEqualsMatchIterator(intArray, start, end, notEqualsLookup);
      default:
        throw new UnsupportedOperationException("current I don't support predicate type : " + p.getType());
    }
  }

}
