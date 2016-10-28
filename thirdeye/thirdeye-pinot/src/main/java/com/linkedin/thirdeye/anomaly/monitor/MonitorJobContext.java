package com.linkedin.thirdeye.anomaly.monitor;


import com.linkedin.thirdeye.anomaly.job.JobContext;

public class MonitorJobContext extends JobContext {

  private MonitorConfiguration monitorConfiguration;

  public MonitorConfiguration getMonitorConfiguration() {
    return monitorConfiguration;
  }

  public void setMonitorConfiguration(MonitorConfiguration monitorConfiguration) {
    this.monitorConfiguration = monitorConfiguration;
  }


}
