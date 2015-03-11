package com.linkedin.pinot.core.data.manager.realtime;

import java.util.Timer;


public class TimerService {
  public static Timer timer = new Timer("RealtimeIndexingSegmentDataManager", true);

}
