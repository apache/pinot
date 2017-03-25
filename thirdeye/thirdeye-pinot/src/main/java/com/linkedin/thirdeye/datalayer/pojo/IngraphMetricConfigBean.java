package com.linkedin.thirdeye.datalayer.pojo;

public class IngraphMetricConfigBean extends AbstractBean {

  private String rrdName;

  private String metricName;

  private String dashboardName;

  private String metricDataType;

  private String metricSourceType;

  private String container;

  public IngraphMetricConfigBean() {
  }

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

  public String getMetricDataType() {
    return metricDataType;
  }

  public void setMetricDataType(String metricDataType) {
    this.metricDataType = metricDataType;
  }

  public String getMetricSourceType() {
    return metricSourceType;
  }

  public void setMetricSourceType(String metricSourceType) {
    this.metricSourceType = metricSourceType;
  }

  public String getContainer() {
    return container;
  }

  public void setContainer(String container) {
    this.container = container;
  }
}
