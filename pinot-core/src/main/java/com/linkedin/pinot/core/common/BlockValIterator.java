package com.linkedin.pinot.core.common;

public interface BlockValIterator {

  int nextVal();

  int currentDocId();

  int currentValId();

  boolean reset();

  int nextIntVal();

  float nextFloatVal();

  long nextLongVal();

  double nextDoubleVal();

  String nextStringVal();

  boolean hasNext();

  int size();

  int getIntVal(int docId);

  float getFloatVal(int docId);

  long getLongVal(int docId);

  double getDoubleVal(int docId);

  String getStringVal(int docId);
}
