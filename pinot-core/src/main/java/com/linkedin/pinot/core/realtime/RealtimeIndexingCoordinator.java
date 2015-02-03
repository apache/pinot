package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.core.indexsegment.IndexSegment;

public interface RealtimeIndexingCoordinator {

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
  public void shutdown();

}
