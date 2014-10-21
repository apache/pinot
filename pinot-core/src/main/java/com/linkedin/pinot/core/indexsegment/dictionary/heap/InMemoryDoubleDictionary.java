package com.linkedin.pinot.core.indexsegment.dictionary.heap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;


public class InMemoryDoubleDictionary extends Dictionary<Double> {

  double[] dictionaryArray;
  private File dictFile;

  public InMemoryDoubleDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    dictionaryArray = new double[dictionarySize];
    dictFile = dictionaryFile;
    load();
  }

  public void load() throws IOException {
    GenericRowColumnDataFileReader file =
        GenericRowColumnDataFileReader.forMmap(dictFile, dictionaryArray.length, 1,
            V1Constants.Dict.DOUBLE_DICTIONARY_COL_SIZE);
    for (int i = 0; i < dictionaryArray.length; i++) {
      dictionaryArray[i] = file.getDouble(i, 0);
    }
  }

  private double searchable(Object o) {
    if (o == null) {
      return V1Constants.Numbers.NULL_DOUBLE;
    }
    if (o instanceof Long) {
      return ((Double) o).doubleValue();
    }
    if (o instanceof String) {
      if (o.equals("null")) {
        return V1Constants.Numbers.NULL_DOUBLE;
      }
      return Double.parseDouble(o.toString());
    }
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

  @Override
  public int getInteger(int index) {
    return (int) dictionaryArray[index];
  }

  @Override
  public float getFloat(int index) {
    return (float) dictionaryArray[index];
  }

  @Override
  public long getLong(int index) {
    return (long) dictionaryArray[index];
  }

  @Override
  public double getDouble(int index) {
    return dictionaryArray[index];
  }

}
