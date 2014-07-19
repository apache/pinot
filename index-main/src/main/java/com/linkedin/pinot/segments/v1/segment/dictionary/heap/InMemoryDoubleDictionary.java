package com.linkedin.pinot.segments.v1.segment.dictionary.heap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;


public class InMemoryDoubleDictionary extends Dictionary<Double> {

  double[] dictionaryArray;
  private File dictFile;

  public InMemoryDoubleDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    dictionaryArray = new double[dictionarySize];
    dictFile = dictionaryFile;
    load();
  }

  public void load() throws IOException {
    GenericMMappedDataFile file =
        new GenericMMappedDataFile(dictFile, dictionaryArray.length, 1, V1Constants.Dict.DOUBLE_DICTIONARY_COL_SIZE);
    for (int i = 0; i < dictionaryArray.length; i++) {
      dictionaryArray[i] = file.getDouble(i, 0);
    }
  }

  private double searchable(Object o) {
    if (o == null)
      return V1Constants.Numbers.NULL_DOUBLE;
    if (o instanceof Long)
      return ((Double) o).doubleValue();
    if (o instanceof String)
      return Double.parseDouble(o.toString());
    return -1D;
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
  public Double getRaw(int index) {
    return new Double(dictionaryArray[index]);
  }

  @Override
  public String getString(int index) {
    return String.valueOf(dictionaryArray[index]);
  }

}
