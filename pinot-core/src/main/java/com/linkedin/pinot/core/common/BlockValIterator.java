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

  int nextDictVal();
}
