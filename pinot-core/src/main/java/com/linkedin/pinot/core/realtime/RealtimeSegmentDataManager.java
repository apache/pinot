package com.linkedin.pinot.core.realtime;

/**
 * TODO://shouldn't we have a segmentaDataManager interface that is inherited by historic and
 * realtime
 */
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
