package com.linkedin.thirdeye.datalayer.pojo;

import java.util.Objects;


/**
 * Class to represent the autometric definition in thirdeye database
 */
public class AutometricsConfigBean extends AbstractBean {

  /** Metric name as in MetricConfig **/
  private String metric;
  /** Dashboard name in external dashboard, and dataset name in datasetConfig **/
  private String dashboard;
  private String rrd;
  private String metricDataType;
  /**  COUNTER or GAUGE **/
  private String autometricsType;
  private String container;
  private String fabricGroup;
  private boolean active = true;

  public AutometricsConfigBean() {
  }

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

  public String getMetricDataType() {
    return metricDataType;
  }

  public void setMetricDataType(String metricDataType) {
    this.metricDataType = metricDataType;
  }

  public String getAutometricsType() {
    return autometricsType;
  }

  public void setAutometricsType(String autometricType) {
    this.autometricsType = autometricType;
  }

  public String getContainer() {
    return container;
  }

  public void setContainer(String container) {
    this.container = container;
  }

  public String getFabricGroup() {
    return fabricGroup;
  }

  public void setFabricGroup(String fabricGroup) {
    this.fabricGroup = fabricGroup;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  @Override
  public int hashCode() {
    return Objects.hash(metric, dashboard, rrd, metricDataType, autometricsType, container, fabricGroup, active);
  }

  @Override
  public boolean equals(Object o) {

    if (!(o instanceof AutometricsConfigBean)) {
      return false;
    }
    AutometricsConfigBean ac = (AutometricsConfigBean) o;
    return Objects.equals(getMetric(), ac.getMetric())
        && Objects.equals(getDashboard(), ac.getDashboard())
        && Objects.equals(getRrd(), ac.getRrd())
        && Objects.equals(getMetricDataType(), ac.getMetricDataType())
        && Objects.equals(getAutometricsType(), ac.getAutometricsType())
        && Objects.equals(getContainer(), ac.getContainer())
        && Objects.equals(getFabricGroup(), ac.getFabricGroup())
        && Objects.equals(isActive(), ac.isActive());
  }

}
