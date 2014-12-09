package com.linkedin.pinot.core.common;

public interface BlockDocIdSet {

  BlockDocIdIterator iterator();

  Object getRaw();
}
