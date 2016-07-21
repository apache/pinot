package com.linkedin.thirdeye.anomaly.monitor;

public class MonitorConfiguration {

  private int expireDaysAgo = 7;
  private int monitorFrequencyHours = 1;

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
