package com.linkedin.thirdeye.anomaly.monitor;

public class MonitorConstants {
  public enum MonitorType {
    UPDATE,
    EXPIRE
  }

  public static int DEFAULT_EXPIRE_DAYS_AGO = 7;
  public static int DEFAULT_MONITOR_FREQUENCY_HOURS = 1;

}
