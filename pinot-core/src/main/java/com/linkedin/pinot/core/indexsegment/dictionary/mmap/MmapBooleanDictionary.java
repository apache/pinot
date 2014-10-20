package com.linkedin.pinot.core.indexsegment.dictionary.mmap;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;
import com.linkedin.pinot.core.indexsegment.utils.SearchableByteBufferUtil;


public class MmapBooleanDictionary extends Dictionary<Boolean> {

  GenericRowColumnDataFileReader mmappedFile;
  SearchableByteBufferUtil searchableMmapFile;
  int perEntrySize;
  int size;

  public MmapBooleanDictionary(File dictionaryFile, int dictionarySize, int lengthPerEntry) throws IOException {
    mmappedFile =
        GenericRowColumnDataFileReader.forMmap(dictionaryFile, dictionarySize, 1, new int[] { lengthPerEntry });
    searchableMmapFile = new SearchableByteBufferUtil(mmappedFile);
    this.size = dictionarySize;
    this.perEntrySize = lengthPerEntry;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) <= -1 ? false : true;
  }

  public String searchableValue(Object e) {
    if (e == null) {
      return "";
    }

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
  public Boolean getRaw(int index) {
    return Boolean.valueOf(mmappedFile.getString(index, 0));
  }

  @Override
  public String getString(int index) {
    return mmappedFile.getString(index, 0);
  }

  @Override
  public int getInteger(int index) {
    throw new UnsupportedOperationException("Not support getInteger value from MmapBooleanDictionary");
  }

  @Override
  public float getFloat(int index) {
    throw new UnsupportedOperationException("Not support getFloat value from MmapBooleanDictionary");
  }

  @Override
  public long getLong(int index) {
    throw new UnsupportedOperationException("Not support getLong value from MmapBooleanDictionary");
  }

  @Override
  public double getDouble(int index) {
    throw new UnsupportedOperationException("Not support getDouble value from MmapBooleanDictionary");
  }
}
