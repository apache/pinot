package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec.DataType;


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

  DataType getValueType();
}
