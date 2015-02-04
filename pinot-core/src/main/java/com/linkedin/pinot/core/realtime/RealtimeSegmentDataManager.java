package com.linkedin.pinot.core.realtime;


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
  public MutableIndexSegment getRealtimeSegment();

  /**
   *
   */
  public void convertToOffline();

  /**
   *
   */
  public void shutdown();

}
