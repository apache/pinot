package com.linkedin.thirdeye.datalayer.entity;

public class IngraphMetricConfigIndex extends AbstractIndexEntity {

  private String rrdName;

  private String metricName;

  private String dashboardName;

  public String getRrdName() {
    return rrdName;
  }

  public void setRrdName(String rrdName) {
    this.rrdName = rrdName;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public String getDashboardName() {
    return dashboardName;
  }

  public void setDashboardName(String dashboardName) {
    this.dashboardName = dashboardName;
  }



}