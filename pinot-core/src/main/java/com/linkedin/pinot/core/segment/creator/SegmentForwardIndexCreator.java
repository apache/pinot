package com.linkedin.pinot.core.segment.creator;

public interface SegmentForwardIndexCreator {

  public abstract void index(Object e);

  public abstract void close();

}
