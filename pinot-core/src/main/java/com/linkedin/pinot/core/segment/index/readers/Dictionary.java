package com.linkedin.pinot.core.segment.index.readers;

public interface Dictionary {

  int getInt(int dictionaryId);

  String getString(int dictionaryId);

  float getFloat(int dictionaryId);

  long getLong(int dictionaryId);

  double getDouble(int dictionaryId);

  int indexOf(Object rawValue);

  Object get(int dictionaryId);

  long getLongValue(int dictionaryId);

  double getDoubleValue(int dictionaryId);

  String toString(int dictionaryId);

  int length();
}
