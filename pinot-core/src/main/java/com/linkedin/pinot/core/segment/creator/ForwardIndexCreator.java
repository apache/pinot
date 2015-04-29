package com.linkedin.pinot.core.segment.creator;

public interface ForwardIndexCreator {

  public abstract void index(int docId, Object e);

  public abstract void close();

}
