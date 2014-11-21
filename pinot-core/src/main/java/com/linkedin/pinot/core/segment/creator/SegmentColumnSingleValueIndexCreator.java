package com.linkedin.pinot.core.segment.creator;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 6, 2014
 */

public interface SegmentColumnSingleValueIndexCreator {

  public void init();
  public void index(int entry);

}
