package com.linkedin.pinot.index.block.utils;

import com.linkedin.pinot.index.common.BlockDocIdIterator;
import com.linkedin.pinot.index.common.BlockDocIdSet;
import com.linkedin.pinot.index.common.Constants;
import com.linkedin.pinot.index.common.Predicate;

public class UnSortedBlockDocIdSet {

  /**
   * 
   * @param values
   * @return
   */
  public BlockDocIdIterator getDefaultIterator(final int[] values) {
    return new BlockDocIdIterator() {
      int counter = 0;

      @Override
      public int skipTo(int targetDocId) {
        if (targetDocId >= values.length)
          return -1;
        counter = targetDocId;
        return counter;
      }

      @Override
      public int next() {
        if (counter < values.length)
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
  public BlockDocIdIterator getRangeMatchIterator(final int start, final int end, final int[] values) {
    return new BlockDocIdIterator() {
      int counter = 0;

      @Override
      public int skipTo(int targetDocId) {
        if (targetDocId >= values.length)
          return -1;
        counter = targetDocId;
        return counter;
      }

      @Override
      public int next() {
        while (counter < values.length) {
          if (values[counter] >= start & values[counter] <= end) {
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
  public BlockDocIdIterator getNotEqualsMatchIterator(final int valueToLookup, final int[] values) {
    return new BlockDocIdIterator() {
      int counter = 0;

      @Override
      public int skipTo(int targetDocId) {
        // TODO Auto-generated method stub
        if (targetDocId >= values.length)
          return -1;
        counter = targetDocId;
        return counter;
      }

      @Override
      public int next() {
        while (counter < values.length) {
          if (valueToLookup != values[counter]) {
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
        // TODO Auto-generated method stub
        return 0;
      }
    };
  }

  /**
   * 
   * @param valueToLookup
   * @param values
   * @return
   */
  public BlockDocIdIterator getEqualityMatchIterator(final int valueToLookup, final int[] values) {
    return new BlockDocIdIterator() {
      private int counter = 0;

      @Override
      public int skipTo(int targetDocId) {
        // TODO Auto-generated method stub
        if (targetDocId >= values.length)
          return -1;
        counter = targetDocId;
        return targetDocId;
      }

      @Override
      public int next() {
        while (counter < values.length) {
          if (values[counter] == valueToLookup) {
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

}
