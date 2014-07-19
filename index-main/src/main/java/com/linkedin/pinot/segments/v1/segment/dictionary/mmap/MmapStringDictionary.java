package com.linkedin.pinot.segments.v1.segment.dictionary.mmap;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;
import com.linkedin.pinot.segments.v1.segment.utils.SearchableMMappedDataFile;


public class MmapStringDictionary extends Dictionary<String> {

  GenericMMappedDataFile mmappedFile;
  SearchableMMappedDataFile searchableMmapFile;
  int perEntrySize;
  int size;

  public MmapStringDictionary(File dictionaryFile, int dictionarySize, int lengthPerEntry) throws IOException {
    mmappedFile = new GenericMMappedDataFile(dictionaryFile, dictionarySize, 1, new int[] { lengthPerEntry });
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
      return V1Constants.Str.NULL_STRING;
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

  @Override
  public String getRaw(int index) {
    return StringUtils.remove(mmappedFile.getString(index, 0), V1Constants.Str.STRING_PAD_CHAR);
  }

  @Override
  public String getString(int index) {
    // TODO Auto-generated method stub
    return StringUtils.remove(mmappedFile.getString(index, 0), V1Constants.Str.STRING_PAD_CHAR);
  }
}
