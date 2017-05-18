package com.linkedin.thirdeye.dashboard.configs;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datasource.MetricExpression;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DashboardConfig extends AbstractConfig {

  public DashboardConfig() {
  }

  Integer dashboardId;

  String dashboardName;

  String collectionName;

  List<MetricExpression> metricExpressions;

  String filterClause;

  public Integer getDashboardId() {
    return dashboardId;
  }

  public void setDashboardId(Integer dashboardId) {
    this.dashboardId = dashboardId;
  }

  public String getDashboardName() {
    return dashboardName;
  }

  public void setDashboardName(String dashboardName) {
    this.dashboardName = dashboardName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public List<MetricExpression> getMetricExpressions() {
    return metricExpressions;
  }

  public void setMetricExpressions(List<MetricExpression> metricExpressions) {
    this.metricExpressions = metricExpressions;
  }

  public String getFilterClause() {
    return filterClause;
  }

  public void setFilterClause(String filterClause) {
    this.filterClause = filterClause;
  }

  @Override
  public String toJSON() throws Exception {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Override
  public String getConfigName() {
    return getDashboardName();
  }

}
