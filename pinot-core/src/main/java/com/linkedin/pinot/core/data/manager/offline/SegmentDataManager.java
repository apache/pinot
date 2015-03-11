package com.linkedin.pinot.core.data.manager.offline;

import com.linkedin.pinot.core.indexsegment.IndexSegment;


public interface SegmentDataManager {

  public IndexSegment getSegment();

  public String getSegmentName();
}
