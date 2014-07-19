package com.linkedin.pinot.segments.v1.segment.dictionary.heap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.segments.v1.creator.V1Constants;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.GenericMMappedDataFile;


public class InMemoryStringDictionary extends Dictionary<String> {

  String[] dictionaryArray;
  private File dictFile;
  private int lengthOfEachEntry;

  public InMemoryStringDictionary(File dictionaryFile, int dictionarySize, int lengthOfEachEntry) throws IOException {
    dictionaryArray = new String[dictionarySize];
    dictFile = dictionaryFile;
    this.lengthOfEachEntry = lengthOfEachEntry;
    load();
  }

  public void load() throws IOException {
    GenericMMappedDataFile file =
        new GenericMMappedDataFile(dictFile, dictionaryArray.length, 1, new int[] { lengthOfEachEntry });
    System.out.println(dictFile.getAbsolutePath());;
    for (int i = 0; i < dictionaryArray.length; i++) {
      String val = file.getString(i, 0);
      dictionaryArray[i] = val;
    }
  }

  private String searchable(Object o) {
    if (o == null)
      return V1Constants.Str.NULL_STRING;
    StringBuilder b = new StringBuilder();
    for (int i = o.toString().length(); i < lengthOfEachEntry; i++) {
      b.append(V1Constants.Str.STRING_PAD_CHAR);
    }
    b.append(o.toString());
    return b.toString();
  }

  @Override
  public boolean contains(Object o) {
    return Arrays.binarySearch(dictionaryArray, searchable(o)) < 0;
  }

  @Override
  public int indexOf(Object o) {
    int ret = Arrays.binarySearch(dictionaryArray, searchable(o));
    return ret;
  }

  @Override
  public int size() {
    return dictionaryArray.length;
  }

  @Override
  public String getRaw(int index) {
    return StringUtils.remove(dictionaryArray[index], V1Constants.Str.STRING_PAD_CHAR);
  }

  @Override
  public String getString(int index) {
    return StringUtils.remove(dictionaryArray[index], V1Constants.Str.STRING_PAD_CHAR);
  }

}
