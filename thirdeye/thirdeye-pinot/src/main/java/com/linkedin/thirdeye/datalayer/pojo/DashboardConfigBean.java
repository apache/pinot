package com.linkedin.thirdeye.datalayer.pojo;

import java.util.List;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.MoreObjects;

@JsonIgnoreProperties(ignoreUnknown=true)
public class DashboardConfigBean extends AbstractBean {

  private String name;

  private String dataset;

  private List<Long> metricIds;

  private String filterClause;

  private String groupBy;

  private List<Long> anomalyFunctionIds;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Long> getMetricIds() {
    return metricIds;
  }

  public void setMetricIds(List<Long> metricIds) {
    this.metricIds = metricIds;
  }


  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getFilterClause() {
    return filterClause;
  }

  public void setFilterClause(String filterClause) {
    this.filterClause = filterClause;
  }

  public String getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(String groupBy) {
    this.groupBy = groupBy;
  }

  public List<Long> getAnomalyFunctionIds() {
    return anomalyFunctionIds;
  }

  public void setAnomalyFunctionIds(List<Long> anomalyFunctionIds) {
    this.anomalyFunctionIds = anomalyFunctionIds;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DashboardConfigBean)) {
      return false;
    }
    DashboardConfigBean dc = (DashboardConfigBean) o;
    return Objects.equals(getId(), dc.getId())
        && Objects.equals(name, dc.getName())
        && Objects.equals(metricIds, dc.getMetricIds())
        && Objects.equals(dataset, dc.getDataset())
        && Objects.equals(filterClause, dc.getFilterClause())
        && Objects.equals(groupBy, dc.getGroupBy())
        && Objects.equals(anomalyFunctionIds, dc.getAnomalyFunctionIds());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), name, metricIds, dataset, filterClause, groupBy, anomalyFunctionIds);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("name", name)
        .add("metricIds", metricIds).add("dataset", getDataset()).add("filterClause", filterClause)
        .add("groupBy", groupBy).add("anomalyFunctionIds", anomalyFunctionIds).toString();
  }
}
