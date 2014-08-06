package com.linkedin.pinot.core.indexsegment.dictionary.heap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;


public class InMemoryFloatDictionary extends Dictionary<Float> {

  float[] dictionaryArray;
  private File dictFile;

  public InMemoryFloatDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    dictionaryArray = new float[dictionarySize];
    dictFile = dictionaryFile;
    load();
  }

  public void load() throws IOException {
    GenericRowColumnDataFileReader file =
        GenericRowColumnDataFileReader.forMmap(dictFile, dictionaryArray.length, 1,
            V1Constants.Dict.FOLAT_DICTIONARY_COL_SIZE);
    for (int i = 0; i < dictionaryArray.length; i++) {
      dictionaryArray[i] = file.getFloat(i, 0);
    }
  }

  private float searchable(Object o) {
    if (o == null)
      return V1Constants.Numbers.NULL_FLOAT;
    if (o instanceof Long)
      return ((Float) o).floatValue();
    if (o instanceof String)
      return Float.parseFloat(o.toString());
    return -1F;
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
  public Float getRaw(int index) {
    return new Float(dictionaryArray[index]);
  }

  @Override
  public String getString(int index) {
    return String.valueOf(dictionaryArray[index]);
  }

}
