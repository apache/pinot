package com.linkedin.pinot.core.block.sets.utils;

import org.roaringbitmap.IntIterator;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.columnar.BitmapInvertedIndex;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class UnSortedBlockDocIdSet {

  public static class OnInvertedIndex {
    public static BlockDocIdIterator getEqualityMatchIterator(final BitmapInvertedIndex invertedIndex, final int start, final int end,
        final int valueToLookup) {

      invertedIndex.getImmutable(valueToLookup).getIntIterator();
      return new BlockDocIdIterator() {
        int current = -1;
        private final IntIterator iterator = invertedIndex.getImmutable(valueToLookup).getIntIterator();

        @Override
        public int skipTo(int targetDocId) {
          return -1;
        }

        @Override
        public int next() {
          if (iterator.hasNext()) {
            current = iterator.next();
          } else {
            return Constants.EOF;
          }
          return current;
        }

        @Override
        public int currentDocId() {
          return current;
        }
      };
    }
  }

  public static class OnForwardIndex {
    public static BlockDocIdIterator getDefaultIterator(final IntArray intArray, final int start, final int end) {
      return new BlockDocIdIterator() {
        int counter = start;

        @Override
        public int skipTo(int targetDocId) {
          if (targetDocId >= end) {
            return Constants.EOF;
          }
          counter = targetDocId;
          return counter;
        }

        @Override
        public int next() {
          if (counter < end) {
            return counter++;
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
     * this is an inclusive match call this function with appropriate start and ends
     * @param start
     * @param end
     * @param values
     * @return
     *
     */
    public static BlockDocIdIterator getRangeMatchIterator(final IntArray intArray, final int start, final int end, final int rangeStart,
        final int rangeEnd) {
      return new BlockDocIdIterator() {
        int counter = 0;

        @Override
        public int skipTo(int targetDocId) {
          if (targetDocId >= end) {
            return Constants.EOF;
          }
          counter = targetDocId;
          return counter;
        }

        @Override
        public int next() {
          while (counter < end) {
            final int val = intArray.getInt(counter);
            if (val >= start & val <= end) {
              final int ret = counter;
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
    public static BlockDocIdIterator getNotEqualsMatchIterator(final IntArray intArray, final int start, final int end,
        final int valueToLookup) {
      return new BlockDocIdIterator() {
        int counter = start;

        @Override
        public int skipTo(int targetDocId) {
          if (targetDocId >= end) {
            return Constants.EOF;
          }
          counter = targetDocId;
          return counter;
        }

        @Override
        public int next() {
          while (counter < end) {
            final int val = intArray.getInt(counter);
            if (valueToLookup != val) {
              final int ret = counter;
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
    public static BlockDocIdIterator getEqualityMatchIterator(final IntArray intArray, final int start, final int end,
        final int valueToLookup) {
      return new BlockDocIdIterator() {
        private int counter = start;

        @Override
        public int skipTo(int targetDocId) {
          if (targetDocId >= end) {
            return Constants.EOF;
          }
          counter = targetDocId;
          return targetDocId;
        }

        @Override
        public int next() {
          while (counter < end) {
            final int val = intArray.getInt(counter);
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

}
