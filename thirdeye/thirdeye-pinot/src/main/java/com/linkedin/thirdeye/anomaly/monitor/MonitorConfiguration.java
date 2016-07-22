package com.linkedin.thirdeye.anomaly.monitor;



public class MonitorConfiguration {

  private int expireDaysAgo = MonitorConstants.DEFAULT_EXPIRE_DAYS_AGO;
  private int monitorFrequencyHours = MonitorConstants.DEFAULT_MONITOR_FREQUENCY_HOURS;

  public int getExpireDaysAgo() {
    return expireDaysAgo;
  }
  public void setExpireDaysAgo(int expireDaysAgo) {
    this.expireDaysAgo = expireDaysAgo;
  }
  public int getMonitorFrequencyHours() {
    return monitorFrequencyHours;
  }
  public void setMonitorFrequencyHours(int monitorFrequencyHours) {
    this.monitorFrequencyHours = monitorFrequencyHours;
  }



}
