package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.core.block.intarray.utils.UnSortedBlockValSet;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;



/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class CompressedIntBlockValSet implements BlockValSet {

  IntArray intArray;
  final Predicate p;
  int start;
  int end;
  Dictionary<?> dictionary;

  public CompressedIntBlockValSet(IntArray intArray, Dictionary<?> dictionary, int start, int end,
      Predicate p) {
    this.intArray = intArray;
    this.p = p;
    this.start = start;
    this.end = end;
    this.dictionary = dictionary;
  }

  @Override
  public BlockValIterator iterator() {
    
    if (p == null) {
      return UnSortedBlockValSet.getDefaultIterator(intArray, start, end);
    }

    switch (p.getType()) {
      case EQ:
        int equalsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockValSet.getEqualityMatchIterator(equalsLookup, intArray, start, end);
      case NEQ:
        int notEqualsLookup = dictionary.indexOf(p.getRhs().get(0));
        return UnSortedBlockValSet.getNoEqualsMatchIterator(notEqualsLookup, intArray, start, end);
      default:
        throw new UnsupportedOperationException("current I don't support predicate type : " + p.getType());
    }
  }
}
