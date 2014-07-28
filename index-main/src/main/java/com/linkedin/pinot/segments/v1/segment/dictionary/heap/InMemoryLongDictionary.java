package com.linkedin.pinot.segments.v1.segment.dictionary.heap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.GenericRowColumnDataFileReader;


public class InMemoryLongDictionary extends Dictionary<Long> {

  long[] dictionaryArray;
  private File dictFile;

  public InMemoryLongDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    dictionaryArray = new long[dictionarySize];
    dictFile = dictionaryFile;
    load();
  }

  public void load() throws IOException {
    GenericRowColumnDataFileReader file =
        GenericRowColumnDataFileReader.forMmap(dictFile, dictionaryArray.length, 1, V1Constants.Dict.LONG_DICTIONARY_COL_SIZE);
    for (int i = 0; i < dictionaryArray.length; i++) {
      dictionaryArray[i] = file.getLong(i, 0);
    }
  }

  private long searchable(Object o) {
    if (o == null)
      return V1Constants.Numbers.NULL_LONG;
    if (o instanceof Long)
      return ((Long) o).longValue();
    if (o instanceof String)
      return Long.parseLong(o.toString());
    return -1L;
  }

  @Override
  public boolean contains(Object o) {
    return Arrays.binarySearch(dictionaryArray, searchable(o)) < 0;
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
  public Long getRaw(int index) {
    return new Long(dictionaryArray[index]);
  }

  @Override
  public String getString(int index) {
    return String.valueOf(dictionaryArray[index]);
  }

}
