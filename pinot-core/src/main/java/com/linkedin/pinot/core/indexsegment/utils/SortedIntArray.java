package com.linkedin.pinot.core.indexsegment.utils;

import java.util.Arrays;


public class SortedIntArray implements IntArray {

  GenericRowColumnDataFileReader sortedIndexFile;
  SearchableByteBufferUtil searchableBuffer;

  public SortedIntArray(GenericRowColumnDataFileReader reader) {
    sortedIndexFile = reader;
    searchableBuffer = new SearchableByteBufferUtil(reader);
  }

  @Override
  public void setInt(int index, int value) {
    return;
  }

  @Override
  public int getInt(int docId) {
    int index = searchableBuffer.binarySearch(1, docId);
    if (index < 0) {
      index = (index + 1) * -1;
    }
    if (index >= size()) {
      index--;
    }
    if (sortedIndexFile.getInt(index, 0) < docId) {
      return index;
    }
    return -1;
  }

  public int getMinDocId(int dictionaryId) {
    return sortedIndexFile.getInt(0, dictionaryId);
  }

  public int getMaxDocId(int dictionaryId) {
    System.out.println("looking for dictionary id : " + dictionaryId);
    return sortedIndexFile.getInt(1, dictionaryId);
  }

  @Override
  public int size() {
    return sortedIndexFile.getNumberOfRows();
  }

  public static void main(String[] args) {
    int[] a = { 1, 6, 11, 13, 18, 32 };
    int[] b = { 5, 10, 12, 17, 31, 40 };
    System.out.println(a[(Arrays.binarySearch(b, 14) + 1) * -1]);
  }
}
