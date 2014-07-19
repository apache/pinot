package com.linkedin.pinot.index.block.utils;

import com.linkedin.pinot.index.common.BlockValIterator;
import com.linkedin.pinot.index.common.Constants;


public class UnSortedBlockValSet {

  /**
   * 
   * @param values
   * @return
   */
  public BlockValIterator getDefaultIterator(final int[] values) {
    return new BlockValIterator() {
      int counter = 0;

      @Override
      public boolean reset() {
        counter = 0;
        return true;
      }

      @Override
      public int nextVal() {
        if (counter < values.length)
          return values[counter++];
        return Constants.EOF;
      }

      @Override
      public int currentValId() {
        return values[counter];
      }

      @Override
      public int currentDocId() {
        return counter;
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
  public BlockValIterator getRangeMatchIterator(final int start, final int end, final int[] values) {
    return new BlockValIterator() {
      int counter = 0;

      @Override
      public boolean reset() {
        counter = 0;
        return true;
      }

      @Override
      public int nextVal() {
        while (counter < values.length) {
          if (values[counter] >= start & values[counter] <= end) {
            int ret = values[counter];
            counter++;
            return ret;
          }
          counter++;
        }
        return Constants.EOF;
      }

      @Override
      public int currentValId() {
        return values[counter];
      }

      @Override
      public int currentDocId() {
        return counter;
      }
    };
  }

  /**
   * Currently not taking dictionary into account, in reality there will be a dictionary
   * @param valueToLookup
   * @param values
   * @return
   */
  public BlockValIterator getNoEqualsMatchIterator(final int valueToLookup, final int[] values) {
    return new BlockValIterator() {
      int counter = 0;

      @Override
      public boolean reset() {
        counter = 0;
        return true;
      }

      @Override
      public int nextVal() {
        while (counter < values.length) {
          if (valueToLookup != values[counter]) {
            int ret = values[counter];
            counter++;
            return ret;
          }
          counter++;
        }
        return Constants.EOF;
      }

      @Override
      public int currentValId() {
        return values[counter];
      }

      @Override
      public int currentDocId() {
        return counter;
      }
    };
  }

  /**
   * Currently not taking dictionary into account, in reality there will be a dictionary
   * @param valueToLookup
   * @param values
   * @return
   */
  public BlockValIterator getEqualityMatchIterator(final int valueToLookup, final int[] values) {
    return new BlockValIterator() {
      int counter = 0;

      @Override
      public boolean reset() {
        counter = 0;
        return true;
      }

      @Override
      public int nextVal() {
        while (counter < values.length) {
          if (valueToLookup == values[counter]) {
            int ret = values[counter];
            counter++;
            return ret;
          }
          counter++;
        }
        return Constants.EOF;
      }

      @Override
      public int currentValId() {
        return values[counter];
      }

      @Override
      public int currentDocId() {
        // TODO Auto-generated method stub
        return counter;
      }
    };
  }
}
