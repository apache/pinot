package com.linkedin.pinot.core.block.sets.utils;

import com.linkedin.pinot.core.common.BlockDocIdValueIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;


public class UnSortedBlockDocIdValSet {

  public static BlockDocIdValueIterator getDefaultIterator(final IntArray intArray, final int start, final int end) {

    return new BlockDocIdValueIterator() {
      int counter = start;

      @Override
      public int currentVal() {
        if (counter >= end)
          return Constants.EOF;
        return intArray.getInt(counter);
      }

      @Override
      public int currentDocId() {
        if (counter >= end)
          return Constants.EOF;
        return counter;
      }

      @Override
      public boolean advance() {
        while (counter < end) {
          counter++;
          return true;
        }
        return false;
      }
    };
  }

  /**
   * 
   * @param start
   * @param end
   * @param values
   * @return
   */
  public static BlockDocIdValueIterator getRangeMatchIterator(final IntArray intArray, final int start, final int end,
      final int rangeStart, final int rangeEnd) {
    return new BlockDocIdValueIterator() {
      int counter = start;
      int currentIndex = -1;

      @Override
      public int currentVal() {
        return intArray.getInt(currentIndex);
      }

      @Override
      public int currentDocId() {
        return currentIndex;
      }

      @Override
      public boolean advance() {
        counter++;
        while (counter < end) {
          int val = intArray.getInt(counter);
          if (val >= start && val <= end) {
            currentIndex = counter;
            counter++;
            return true;
          }
          counter++;
        }
        return false;
      }
    };
  }

  /**
   * 
   * @param valuesToLookup
   * @param values
   * @return
   */
  public static BlockDocIdValueIterator getNotEqualsMatchIterator(final IntArray intArray, final int start,
      final int end, final int valueToLookup) {
    return new BlockDocIdValueIterator() {
      int counter = start;
      int currentIndex = -1;

      @Override
      public int currentVal() {
        return intArray.getInt(currentIndex);
      }

      @Override
      public int currentDocId() {
        return currentIndex;
      }

      @Override
      public boolean advance() {
        while (counter < end) {
          int val = intArray.getInt(counter);
          if (val != valueToLookup) {
            currentIndex = counter;
            counter++;
            return true;
          }
          counter++;
        }
        return false;
      }
    };
  }

  /**
   * 
   * @param valueToLookup
   * @param values
   * @return
   */
  public static BlockDocIdValueIterator getEqualityMatchIterator(final IntArray intArray, final int start,
      final int end, final int valueToLookup) {
    return new BlockDocIdValueIterator() {
      int counter = start;
      int currentIndex = -1;

      @Override
      public int currentVal() {
        return intArray.getInt(currentIndex);
      }

      @Override
      public int currentDocId() {
        return currentIndex;
      }

      @Override
      public boolean advance() {
        while (counter < end) {
          int val = intArray.getInt(counter);
          if (val == valueToLookup) {
            currentIndex = counter;
            counter++;
            return true;
          }
          counter++;
        }
        return false;
      }
    };
  }
}
