package com.linkedin.thirdeye.anomaly.monitor;

import java.util.List;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;

public class MonitorJobContext extends JobContext {

  private List<AnomalyJobSpec> anomalyJobSpecs;
  private MonitorConfiguration monitorConfiguration;

  public List<AnomalyJobSpec> getAnomalyJobSpecs() {
    return anomalyJobSpecs;
  }

  public void setAnomalyJobSpecs(List<AnomalyJobSpec> anomalyJobSpecs) {
    this.anomalyJobSpecs = anomalyJobSpecs;
  }

  public MonitorConfiguration getMonitorConfiguration() {
    return monitorConfiguration;
  }

  public void setMonitorConfiguration(MonitorConfiguration monitorConfiguration) {
    this.monitorConfiguration = monitorConfiguration;
  }


}
