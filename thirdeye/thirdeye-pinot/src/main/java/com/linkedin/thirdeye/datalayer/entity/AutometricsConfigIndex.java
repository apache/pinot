package com.linkedin.thirdeye.datalayer.entity;

public class AutometricsConfigIndex extends AbstractIndexEntity {

  private String metric;
  private String dashboard;
  private String rrd;
  private String autometricsType;

  public String getMetric() {
    return metric;
  }
  public void setMetric(String metric) {
    this.metric = metric;
  }
  public String getDashboard() {
    return dashboard;
  }
  public void setDashboard(String dashboard) {
    this.dashboard = dashboard;
  }
  public String getRrd() {
    return rrd;
  }
  public void setRrd(String rrd) {
    this.rrd = rrd;
  }
  public String getAutometricsType() {
    return autometricsType;
  }
  public void setAutometricsType(String autometricType) {
    this.autometricsType = autometricType;
  }



}