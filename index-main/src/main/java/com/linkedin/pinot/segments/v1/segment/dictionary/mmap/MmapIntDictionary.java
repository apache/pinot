package com.linkedin.pinot.segments.v1.segment.dictionary.mmap;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.GenericRowColumnDataFileReader;
import com.linkedin.pinot.segments.v1.segment.utils.SearchableByteBufferUtil;


public class MmapIntDictionary extends Dictionary<Integer> {
  GenericRowColumnDataFileReader mmappedFile;
  SearchableByteBufferUtil searchableMmapFile;
  int size;

  public MmapIntDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    mmappedFile =
        GenericRowColumnDataFileReader.forMmap(dictionaryFile, dictionarySize, 1,
            V1Constants.Dict.INT_DICTIONARY_COL_SIZE);
    searchableMmapFile = new SearchableByteBufferUtil(mmappedFile);
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

  @Override
  public Integer getRaw(int index) {
    return new Integer(mmappedFile.getInt(index, 0));
  }

  @Override
  public String getString(int index) {
    return String.valueOf(mmappedFile.getInt(index, 0));
  }

}
