package com.linkedin.pinot.core.block.intarray.utils;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class UnSortedBlockDocIdSet {
  
  public static BlockDocIdIterator getDefaultIterator(final IntArray intArray, final int start,
      final int end) {
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
  public static BlockDocIdIterator getRangeMatchIterator(final IntArray intArray, final int start,
      final int end, final int rangeStart, final int rangeEnd) {
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
  public static BlockDocIdIterator getNotEqualsMatchIterator(final IntArray intArray, final int start,
      final int end, final int valueToLookup) {
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
        while (counter < end) {
          int val = intArray.getInt(counter);
          if (valueToLookup != val) {
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
  public static BlockDocIdIterator getEqualityMatchIterator(final IntArray intArray, final int start,
      final int end, final int valueToLookup) {
    return new BlockDocIdIterator() {
      private int counter = start;

      @Override
      public int skipTo(int targetDocId) {
        if (targetDocId >= end)
          return Constants.EOF;
        counter = targetDocId;
        return targetDocId;
      }

      @Override
      public int next() {
        while (counter < end) {
          int val = intArray.getInt(counter);
          if (val == valueToLookup) {
            return counter++;
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

}
