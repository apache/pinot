package com.linkedin.pinot.segments.v1.segment.dictionary;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;
import com.linkedin.pinot.segments.v1.segment.utils.SearchableMMappedDataFile;

public class StringDictionary extends Dictionary<String> {

  GenericMMappedDataFile mmappedFile;
  SearchableMMappedDataFile searchableMmapFile;
  int perEntrySize;
  int size;

  public StringDictionary(File dictionaryFile, int dictionarySize,
      int lengthPerEntry) throws IOException {
    mmappedFile = new GenericMMappedDataFile(dictionaryFile, dictionarySize, 1,
        new int[] { lengthPerEntry });
    searchableMmapFile = new SearchableMMappedDataFile(mmappedFile);
    this.size = dictionarySize;
    this.perEntrySize = lengthPerEntry;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) <= -1 ? false : true;
  }

  public String searchableValue(Object e) {
    if (e == null)
      return "";

    return (String) e;
  }

  @Override
  public int indexOf(Object o) {
    return searchableMmapFile.binarySearch(0, searchableValue(o));
  }

  @Override
  public int size() {
    return size;
  }
}
