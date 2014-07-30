package com.linkedin.pinot.core.common;



public interface BlockDocIdValueIterator {

  boolean advance();

  int currentDocId();

  int currentVal();
}
