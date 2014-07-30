package com.linkedin.pinot.core.indexsegment.dictionary.mmap;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;
import com.linkedin.pinot.core.indexsegment.utils.SearchableByteBufferUtil;

public class MmapLongDictionary extends Dictionary<Long> {

  GenericRowColumnDataFileReader mmappedFile;
  SearchableByteBufferUtil searchableMmapFile;
  int size;
  
  public MmapLongDictionary(File dictionaryFile, int dictionarySize)
      throws IOException {
    mmappedFile = GenericRowColumnDataFileReader.forMmap(dictionaryFile, dictionarySize, 1,
        V1Constants.Dict.LONG_DICTIONARY_COL_SIZE);
    searchableMmapFile = new SearchableByteBufferUtil(mmappedFile);
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

  @Override
  public Long getRaw(int index) {
    return new Long(mmappedFile.getLong(index, 0));
  }

  @Override
  public String getString(int index) {
    return String.valueOf(mmappedFile.getLong(index, 0));
  }

}
