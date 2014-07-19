package com.linkedin.pinot.segments.v1.segment.dictionary.heap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;


public class InMemoryIntDictionary extends Dictionary<Integer> {

  int[] dictionaryArray;
  private File dictFile;

  public InMemoryIntDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    dictionaryArray = new int[dictionarySize];
    dictFile = dictionaryFile;
    load();
  }

  public void load() throws IOException {
    GenericMMappedDataFile file =
        new GenericMMappedDataFile(dictFile, dictionaryArray.length, 1, V1Constants.Dict.INT_DICTIONARY_COL_SIZE);
    for (int i = 0; i < dictionaryArray.length; i++) {
      dictionaryArray[i] = file.getInt(i, 0);
    }
  }

  @Override
  public boolean contains(Object o) {
    return Arrays.binarySearch(dictionaryArray, searchable(o)) < 0;
  }

  private int searchable(Object o) {
    if (o == null)
      return V1Constants.Numbers.NULL_INT;
    if (o instanceof Integer)
      return ((Integer) o).intValue();
    if (o instanceof String)
      return Integer.parseInt(o.toString());
    return -1;
  }

  @Override
  public int indexOf(Object o) {
    return Arrays.binarySearch(dictionaryArray, searchable(o));
  }

  @Override
  public int size() {
    return dictionaryArray.length;
  }

  @Override
  public Integer getRaw(int index) {
    return new Integer(dictionaryArray[index]);
  }

  @Override
  public String getString(int index) {
    return String.valueOf(dictionaryArray[index]);
  }
}
