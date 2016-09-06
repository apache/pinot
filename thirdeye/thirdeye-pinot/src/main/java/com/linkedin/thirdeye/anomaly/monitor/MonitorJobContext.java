package com.linkedin.thirdeye.anomaly.monitor;

import java.util.List;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;

public class MonitorJobContext extends JobContext {

  private List<JobDTO> anomalyJobSpecs;
  private MonitorConfiguration monitorConfiguration;

  public List<JobDTO> getAnomalyJobSpecs() {
    return anomalyJobSpecs;
  }

  public void setAnomalyJobSpecs(List<JobDTO> anomalyJobSpecs) {
    this.anomalyJobSpecs = anomalyJobSpecs;
  }

  public MonitorConfiguration getMonitorConfiguration() {
    return monitorConfiguration;
  }

  public void setMonitorConfiguration(MonitorConfiguration monitorConfiguration) {
    this.monitorConfiguration = monitorConfiguration;
  }


}
