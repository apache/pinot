package com.linkedin.pinot.segments.v1.segment.dictionary;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;
import com.linkedin.pinot.segments.v1.segment.utils.SearchableMMappedDataFile;

public class FloatDictionary extends Dictionary<Float> {

  GenericMMappedDataFile mmappedFile;
  SearchableMMappedDataFile searchableMmapFile;
  int size;

  public FloatDictionary(File dictionaryFile, int dictionarySize)
      throws IOException {
    mmappedFile = new GenericMMappedDataFile(dictionaryFile, dictionarySize, 1,
        V1Constants.Dict.FOLAT_DICTIONARY_COL_SIZE);
    searchableMmapFile = new SearchableMMappedDataFile(mmappedFile);
    this.size = dictionarySize;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) <= -1 ? false : true;
  }

  public Float searchableValue(Object e) {
    if (e == null)
      return new Float(V1Constants.Numbers.NULL_FLOAT);
    if (e instanceof Float)
      return (Float) e;
    else
      return new Float(Float.parseFloat(e.toString()));
  }

  @Override
  public int indexOf(Object o) {
    return searchableMmapFile.binarySearch(0, searchableValue(o), 0, size);
  }

  @Override
  public int size() {
    return size;
  }

}
