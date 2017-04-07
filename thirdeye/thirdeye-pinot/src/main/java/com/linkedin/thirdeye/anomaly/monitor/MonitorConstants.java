package com.linkedin.thirdeye.anomaly.monitor;

import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.TimeGranularity;

public class MonitorConstants {
  public enum MonitorType {
    UPDATE,
    EXPIRE
  }

  public static int DEFAULT_EXPIRE_DAYS_AGO = 3;
  public static TimeGranularity DEFAULT_MONITOR_FREQUENCY = new TimeGranularity(1, TimeUnit.HOURS);

}
