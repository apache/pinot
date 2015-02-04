package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public interface MutableIndexSegment extends IndexSegment {

  /**
   * expects a generic row that has all the columns
   * specified in the schema which was used to
   * initialize the realtime segment
   * @param row
   */
  public void index(GenericRow row);
}
