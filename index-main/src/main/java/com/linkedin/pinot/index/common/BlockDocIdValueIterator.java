package com.linkedin.pinot.index.common;



public interface BlockDocIdValueIterator {

  boolean advance();

  int currentDocId();

  int currentVal();
}
