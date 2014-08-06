package com.linkedin.pinot.core.block;

import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Constants;


public class IntBlockValIterator implements BlockValIterator {

  private final int[] data;

  int currentIndex = 0;

  public IntBlockValIterator(int[] data) {
    this.data = data;
  }

  /**
   * throws arrayoutofboundsException is currentIndex is beyond the size of
   * array
   */
  @Override
  public int nextVal() {
    if (currentIndex >= this.data.length) {
      return Constants.EOF;
    }
    int ret = data[currentIndex];
    currentIndex = currentIndex + 1;
    return ret;
  }

  @Override
  public int currentDocId() {
    return currentIndex;
  }

  @Override
  public int currentValId() {
    return data[currentIndex];
  }

  public static void main(String[] args) {
    IntBlockValIterator iterator = new IntBlockValIterator(new int[] { 1, 4, 6, 12, 15, 19 });
    int val;

    while ((val = iterator.nextVal()) != Constants.EOF) {
      System.out.println(val);
    }

    iterator.reset();
  }

  @Override
  public boolean reset() {
    currentIndex = 0;
    return true;
  }

}
