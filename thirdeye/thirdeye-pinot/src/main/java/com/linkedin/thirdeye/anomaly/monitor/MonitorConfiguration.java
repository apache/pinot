package com.linkedin.thirdeye.anomaly.monitor;

import com.linkedin.thirdeye.api.TimeGranularity;

public class MonitorConfiguration {

  private int expireDaysAgo = MonitorConstants.DEFAULT_EXPIRE_DAYS_AGO;
  private TimeGranularity monitorFrequency = MonitorConstants.DEFAULT_MONITOR_FREQUENCY;

  public int getExpireDaysAgo() {
    return expireDaysAgo;
  }
  public void setExpireDaysAgo(int expireDaysAgo) {
    this.expireDaysAgo = expireDaysAgo;
  }
  public TimeGranularity getMonitorFrequency() {
    return monitorFrequency;
  }
  public void setMonitorFrequency(TimeGranularity monitorFrequency) {
    this.monitorFrequency = monitorFrequency;
  }
}
