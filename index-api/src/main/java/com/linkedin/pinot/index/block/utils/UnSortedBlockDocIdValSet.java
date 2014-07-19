package com.linkedin.pinot.index.block.utils;

import com.linkedin.pinot.index.common.BlockDocIdValueIterator;
import com.linkedin.pinot.index.common.Constants;


public class UnSortedBlockDocIdValSet {

  public BlockDocIdValueIterator getDefaultIterator(final int[] values) {
    return new BlockDocIdValueIterator() {
      int counter = 0;

      @Override
      public int currentVal() {
        if (counter >= values.length)
          return Constants.EOF;
        return values[counter];
      }

      @Override
      public int currentDocId() {
        if (counter >= values.length)
          return Constants.EOF;
        return counter;
      }

      @Override
      public boolean advance() {
        while (counter < values.length) {
          counter++;
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
  public BlockDocIdValueIterator getRangeMatchIterator(final int start, final int end, final int[] values) {
    return new BlockDocIdValueIterator() {
      int counter = 0;

      @Override
      public int currentVal() {
        return values[counter];
      }

      @Override
      public int currentDocId() {
        return counter;
      }

      @Override
      public boolean advance() {
        while (counter < values.length) {
          if (values[counter] >= start && values[counter] <= end) {
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
  public BlockDocIdValueIterator getNotEqualsMatchIterator(final int valueToLookup, final int[] values) {
    return new BlockDocIdValueIterator() {
      int counter = 0;

      @Override
      public int currentVal() {
        return values[counter];
      }

      @Override
      public int currentDocId() {
        return counter;
      }

      @Override
      public boolean advance() {
        while (counter < values.length) {
          if (values[counter] != valueToLookup) {
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
  public BlockDocIdValueIterator getEqualityMatchIterator(final int valueToLookup, final int[] values) {
    return new BlockDocIdValueIterator() {
      int counter = 0;

      @Override
      public int currentVal() {
        return values[counter];
      }

      @Override
      public int currentDocId() {
        return counter;
      }

      @Override
      public boolean advance() {
        while (counter < values.length) {
          if (values[counter] == valueToLookup) {
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
