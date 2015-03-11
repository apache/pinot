package com.linkedin.pinot.core.data.manager.realtime;

public class RealtimeIndexRunnable implements Runnable {
  final RealtimeSegmentDataManager segManager;
  final long start = System.currentTimeMillis();

  public RealtimeIndexRunnable(RealtimeSegmentDataManager seg) {
    segManager = seg;
  }

  @Override
  public void run() {
    while (segManager.index()) {
    }
  }
}
