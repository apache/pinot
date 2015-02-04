package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.core.indexsegment.IndexSegment;

public interface RealtimeSegmentDataManager {

  /**
   *
   */
  public void init(RealtimeIndexingConfig realtimeIndexingConfig);

  /**
   *
   */
  public void start();

  /**
   *
   * @return
   */
  public IndexSegment getRealtimeSegment();

  /**
   *
   */
  public void convertToOffline();

  /**
   *
   */
  public void shutdown();

}
