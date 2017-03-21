package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;


@JsonIgnoreProperties(ignoreUnknown=true)
public class AnomalyFunctionExBean extends AbstractBean {

  // Anomaly function parameterization
  String name;
  String className;
  Map<String, String> config;

  // scheduling
  String cron;
  boolean active;
  long monitoringWindowAlignment;
  long monitoringWindowLookback;

  // merging
  long mergeWindow;

  // display
  String displayMetric;
  String displayCollection;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public long getMonitoringWindowAlignment() {
    return monitoringWindowAlignment;
  }

  public void setMonitoringWindowAlignment(long monitoringWindowAlignment) {
    this.monitoringWindowAlignment = monitoringWindowAlignment;
  }

  public long getMonitoringWindowLookback() {
    return monitoringWindowLookback;
  }

  public void setMonitoringWindowLookback(long monitoringWindowLookback) {
    this.monitoringWindowLookback = monitoringWindowLookback;
  }

  public long getMergeWindow() {
    return mergeWindow;
  }

  public void setMergeWindow(long mergeWindow) {
    this.mergeWindow = mergeWindow;
  }

  public String getDisplayMetric() {
    return displayMetric;
  }

  public void setDisplayMetric(String displayMetric) {
    this.displayMetric = displayMetric;
  }

  public String getDisplayCollection() {
    return displayCollection;
  }

  public void setDisplayCollection(String displayCollection) {
    this.displayCollection = displayCollection;
  }
}
