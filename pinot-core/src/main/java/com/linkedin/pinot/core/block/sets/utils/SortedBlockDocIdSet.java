package com.linkedin.pinot.core.block.sets.utils;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.utils.SortedIntArray;


public class SortedBlockDocIdSet {

  public static BlockDocIdIterator getDefaultIterator(final SortedIntArray intArray, final int start, final int end) {
    return new BlockDocIdIterator() {
      int counter = start;

      @Override
      public int skipTo(int targetDocId) {
        if (targetDocId >= end)
          return Constants.EOF;
        counter = targetDocId;
        return counter;
      }

      @Override
      public int next() {
        if (counter < end)
          return counter++;
        return Constants.EOF;
      }

      @Override
      public int currentDocId() {
        return counter;
      }
    };
  }

  /**
   * this is an inclusive match call this function with appropriate start and ends
   * @param start
   * @param end
   * @param values
   * @return
   * 
   */
  public static BlockDocIdIterator getRangeMatchIterator(final SortedIntArray intArray, final int start, final int end,
      final int rangeStart, final int rangeEnd) {
    return new BlockDocIdIterator() {
      int counter = 0;

      @Override
      public int skipTo(int targetDocId) {
        if (targetDocId >= end)
          return Constants.EOF;
        counter = targetDocId;
        return counter;
      }

      @Override
      public int next() {
        while (counter < end) {
          int val = intArray.getInt(counter);
          if (val >= start & val <= end) {
            int ret = counter;
            counter++;
            return ret;
          }
          counter++;
        }
        return Constants.EOF;
      }

      @Override
      public int currentDocId() {
        return counter;
      }
    };
  }

  /**
   * 
   * @param valueToLookup
   * @param values
   * @return
   */
  public static BlockDocIdIterator getNotEqualsMatchIterator(final SortedIntArray intArray, final int start,
      final int end, final int valueToLookup) {
    return new BlockDocIdIterator() {
      int notStart = intArray.getMinDocId(valueToLookup);
      int notEnd = intArray.getMaxDocId(valueToLookup);
      int counter = start;

      @Override
      public int skipTo(int targetDocId) {
        if (targetDocId >= intArray.getMaxDocId(valueToLookup))
          return Constants.EOF;
        counter = targetDocId;
        return counter;
      }

      @Override
      public int next() {
        if (counter == notStart)
          counter = notEnd + 1;
        if (counter < end) {
          int ret = counter;
          counter++;
          return ret;
        }
        return Constants.EOF;
      }

      @Override
      public int currentDocId() {
        return counter;
      }
    };
  }

  /**
   * 
   * @param valueToLookup
   * @param values
   * @return
   */
  public static BlockDocIdIterator getEqualityMatchIterator(final SortedIntArray intArray, final int start,
      final int end, final int valueToLookup) {
    return new BlockDocIdIterator() {
      int counter = intArray.getMinDocId(valueToLookup);

      @Override
      public int skipTo(int targetDocId) {
        if (targetDocId >= intArray.getMaxDocId(valueToLookup))
          return Constants.EOF;
        counter = targetDocId;
        return counter;
      }

      @Override
      public int next() {
        System.out.println("value to lookup : " + valueToLookup);
        if (counter <= intArray.getMaxDocId(valueToLookup)) {
          int ret = counter;
          counter++;
          return ret;
        }
        return Constants.EOF;
      }

      @Override
      public int currentDocId() {
        return counter;
      }
    };
  }
}
