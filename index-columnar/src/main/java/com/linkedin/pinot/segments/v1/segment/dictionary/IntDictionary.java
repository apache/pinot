package com.linkedin.pinot.segments.v1.segment.dictionary;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;
import com.linkedin.pinot.segments.v1.segment.utils.SearchableMMappedDataFile;


public class IntDictionary extends Dictionary<Integer> {
  GenericMMappedDataFile mmappedFile;
  SearchableMMappedDataFile searchableMmapFile;
  int size;

  public IntDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    mmappedFile =
        new GenericMMappedDataFile(dictionaryFile, dictionarySize, 1,
            V1Constants.Dict.INT_DICTIONARY_COL_SIZE);
    searchableMmapFile = new SearchableMMappedDataFile(mmappedFile);
    this.size = dictionarySize;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) <= -1 ? false : true;
  }

  public Integer searchableValue(Object e) {
    if (e == null)
      return new Integer(V1Constants.Numbers.NULL_INT);
    if (e instanceof Integer)
      return (Integer) e;
    else
      return new Integer(Integer.parseInt(e.toString()));
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
