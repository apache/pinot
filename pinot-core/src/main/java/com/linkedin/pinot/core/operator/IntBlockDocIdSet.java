package com.linkedin.pinot.core.operator;

import java.util.Arrays;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;


public class IntBlockDocIdSet implements BlockDocIdSet {

  private final int[] docIdArray;
  private final MutableRoaringBitmap bit;

  public IntBlockDocIdSet(MutableRoaringBitmap bitmap) {
    bit = bitmap;
    docIdArray = bit.toArray();
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new BlockDocIdIterator() {

      private final int[] data = docIdArray;

      int currentIndex = -1;

      /**
       * throws arrayoutofboundsException is currentIndex is beyond the size of array
       */
      @Override
      public int next() {
        if (currentIndex + 1 >= data.length) {
          return Constants.EOF;
        }
        currentIndex = currentIndex + 1;
        return data[currentIndex];
      }

      @Override
      public int skipTo(int skipToDocId) {
        if (skipToDocId >= data.length) {
          return Constants.EOF;
        }
        final int index = Arrays.binarySearch(data, currentIndex, data.length, skipToDocId);
        return data[index];
      }

      @Override
      public int currentDocId() {
        return currentIndex;
      }
    };
  }

  @Override
  public Object getRaw() {
    return bit;
  }

}
