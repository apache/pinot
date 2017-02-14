package com.linkedin.thirdeye.detector.functionex;

import java.util.Map;


public class AnomalyFunctionExContext {
  String name;
  Map<String, String> config;
  Map<String, AnomalyFunctionExDataSource> dataSources;

  // used by time series data sources
  long monitoringWindowStart = -1;
  long monitoringWindowEnd = -1;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  public Map<String, AnomalyFunctionExDataSource> getDataSources() {
    return dataSources;
  }

  public void setDataSources(Map<String, AnomalyFunctionExDataSource> dataSources) {
    this.dataSources = dataSources;
  }

  public long getMonitoringWindowStart() {
    return monitoringWindowStart;
  }

  public void setMonitoringWindowStart(long monitoringWindowStart) {
    this.monitoringWindowStart = monitoringWindowStart;
  }

  public long getMonitoringWindowEnd() {
    return monitoringWindowEnd;
  }

  public void setMonitoringWindowEnd(long monitoringWindowEnd) {
    this.monitoringWindowEnd = monitoringWindowEnd;
  }
}
