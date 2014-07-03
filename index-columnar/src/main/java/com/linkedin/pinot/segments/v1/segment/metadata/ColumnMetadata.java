package com.linkedin.pinot.segments.v1.segment.metadata;

public class ColumnMetadata  {
  private String name;
  private int dictionarySize;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getDictionarySize() {
    return dictionarySize;
  }

  public void setDictionarySize(int dictionarySize) {
    this.dictionarySize = dictionarySize;
  }

}
