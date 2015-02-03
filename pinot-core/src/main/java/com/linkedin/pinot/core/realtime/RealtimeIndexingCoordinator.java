package com.linkedin.pinot.core.realtime;

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
   */
  public void shutdown();

}
