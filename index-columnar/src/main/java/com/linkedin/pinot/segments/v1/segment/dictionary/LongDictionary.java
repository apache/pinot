package com.linkedin.pinot.segments.v1.segment.dictionary;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;
import com.linkedin.pinot.segments.v1.segment.utils.SearchableMMappedDataFile;

public class LongDictionary extends Dictionary<Long> {

  GenericMMappedDataFile mmappedFile;
  SearchableMMappedDataFile searchableMmapFile;
  int size;
  
  public LongDictionary(File dictionaryFile, int dictionarySize)
      throws IOException {
    mmappedFile = new GenericMMappedDataFile(dictionaryFile, dictionarySize, 1,
        V1Constants.Dict.LONG_DICTIONARY_COL_SIZE);
    searchableMmapFile = new SearchableMMappedDataFile(mmappedFile);
    this.size = dictionarySize;
  }
  
  @Override
  public boolean contains(Object o) {
    return indexOf(o) <= -1 ? false : true;
  }

  public Long searchableValue(Object e) {
    if (e == null)
      return new Long(V1Constants.Numbers.NULL_LONG);
    if (e instanceof Long)
      return (Long) e;
    else
      return new Long(Long.parseLong(e.toString()));
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
