package com.linkedin.pinot.core.indexsegment.dictionary.mmap;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.GenericRowColumnDataFileReader;
import com.linkedin.pinot.core.indexsegment.utils.SearchableByteBufferUtil;


public class MmapStringDictionary extends Dictionary<String> {

  GenericRowColumnDataFileReader mmappedFile;
  SearchableByteBufferUtil searchableMmapFile;
  int lengthOfEachEntry;
  int size;

  public MmapStringDictionary(File dictionaryFile, int dictionarySize, int lengthPerEntry) throws IOException {
    mmappedFile =
        GenericRowColumnDataFileReader.forMmap(dictionaryFile, dictionarySize, 1, new int[] { lengthPerEntry });
    searchableMmapFile = new SearchableByteBufferUtil(mmappedFile);
    this.size = dictionarySize;
    this.lengthOfEachEntry = lengthPerEntry;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) <= -1 ? false : true;
  }

  public String searchableValue(Object o) {
    if (o == null) {
      o = V1Constants.Str.NULL_STRING;
    }
    StringBuilder b = new StringBuilder();
    for (int i = o.toString().length(); i < lengthOfEachEntry; i++) {
      b.append(V1Constants.Str.STRING_PAD_CHAR);
    }
    b.append(o.toString());

    return b.toString();
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

  @Override
  public int getInteger(int index) {
    return Integer.parseInt(StringUtils.remove(mmappedFile.getString(index, 0), V1Constants.Str.STRING_PAD_CHAR));
  }

  @Override
  public float getFloat(int index) {
    return Float.parseFloat(StringUtils.remove(mmappedFile.getString(index, 0), V1Constants.Str.STRING_PAD_CHAR));
  }

  @Override
  public long getLong(int index) {
    return Long.parseLong(StringUtils.remove(mmappedFile.getString(index, 0), V1Constants.Str.STRING_PAD_CHAR));
  }

  @Override
  public double getDouble(int index) {
    return Double.parseDouble(StringUtils.remove(mmappedFile.getString(index, 0), V1Constants.Str.STRING_PAD_CHAR));
  }
}
