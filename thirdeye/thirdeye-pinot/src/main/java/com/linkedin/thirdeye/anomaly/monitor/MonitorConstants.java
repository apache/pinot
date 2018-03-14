package com.linkedin.thirdeye.anomaly.monitor;

import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.TimeGranularity;

public class MonitorConstants {
  public enum MonitorType {
    UPDATE,
    EXPIRE
  }

  public static int DEFAULT_RETENTION_DAYS = 30;
  public static int DEFAULT_COMPLETED_JOB_RETENTION_DAYS = 14;
  public static int DEFAULT_DETECTION_STATUS_RETENTION_DAYS = 7;
  public static int DEFAULT_RAW_ANOMALY_RETENTION_DAYS = 30;
  public static TimeGranularity DEFAULT_MONITOR_FREQUENCY = new TimeGranularity(15, TimeUnit.MINUTES);

}
