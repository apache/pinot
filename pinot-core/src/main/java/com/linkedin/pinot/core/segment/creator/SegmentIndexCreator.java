package com.linkedin.pinot.core.segment.creator;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 6, 2014
 */

public interface SegmentIndexCreator {

  public void init();

  public void build();
}
