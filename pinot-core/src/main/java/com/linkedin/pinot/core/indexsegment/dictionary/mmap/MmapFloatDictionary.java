package com.linkedin.pinot.core.indexsegment.dictionary.mmap;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;
import com.linkedin.pinot.core.indexsegment.utils.SearchableByteBufferUtil;


public class MmapFloatDictionary extends Dictionary<Float> {

  GenericRowColumnDataFileReader mmappedFile;
  SearchableByteBufferUtil searchableMmapFile;
  int size;

  public MmapFloatDictionary(File dictionaryFile, int dictionarySize) throws IOException {
    mmappedFile =
        GenericRowColumnDataFileReader.forMmap(dictionaryFile, dictionarySize, 1,
            V1Constants.Dict.FOLAT_DICTIONARY_COL_SIZE);
    searchableMmapFile = new SearchableByteBufferUtil(mmappedFile);
    this.size = dictionarySize;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) <= -1 ? false : true;
  }

  public Float searchableValue(Object e) {
    if (e == null) {
      return new Float(V1Constants.Numbers.NULL_FLOAT);
    }
    if (e instanceof Float) {
      return (Float) e;
    } else {
      return new Float(Float.parseFloat(e.toString()));
    }
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
  public Float getRaw(int index) {
    return new Float(mmappedFile.getFloat(index, 0));
  }

  @Override
  public String getString(int index) {
    return String.valueOf(mmappedFile.getFloat(index, 0));
  }

  @Override
  public int getInteger(int index) {
    return (int) mmappedFile.getFloat(index, 0);
  }

  @Override
  public float getFloat(int index) {
    return mmappedFile.getFloat(index, 0);
  }

  @Override
  public long getLong(int index) {
    return (long) mmappedFile.getFloat(index, 0);
  }

  @Override
  public double getDouble(int index) {
    return mmappedFile.getFloat(index, 0);
  }
}
