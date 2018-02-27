package com.linkedin.thirdeye.datasource.pinot.resultset;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class ThirdEyeResultSetMetaData {
  private List<String> groupKeyColumnNames = Collections.emptyList();
  private List<String> metricColumnNames = Collections.emptyList();
  private List<String> allColumnNames = Collections.emptyList();

  public ThirdEyeResultSetMetaData(List<String> groupKeyColumnNames, List<String> metricColumnNames) {
    Preconditions.checkNotNull(groupKeyColumnNames);
    Preconditions.checkNotNull(metricColumnNames);

    this.groupKeyColumnNames = ImmutableList.copyOf(groupKeyColumnNames);
    this.metricColumnNames = ImmutableList.copyOf(metricColumnNames);
    this.allColumnNames =
        ImmutableList.<String>builder().addAll(this.groupKeyColumnNames).addAll(this.metricColumnNames).build();
  }

  public List<String> getGroupKeyColumnNames() {
    return groupKeyColumnNames;
  }

  public List<String> getMetricColumnNames() {
    return metricColumnNames;
  }

  public List<String> getAllColumnNames() {
    return allColumnNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ThirdEyeResultSetMetaData metaData = (ThirdEyeResultSetMetaData) o;
    return Objects.equals(getGroupKeyColumnNames(), metaData.getGroupKeyColumnNames()) && Objects.equals(
        getMetricColumnNames(), metaData.getMetricColumnNames()) && Objects.equals(getAllColumnNames(),
        metaData.getAllColumnNames());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getGroupKeyColumnNames(), getMetricColumnNames(), getAllColumnNames());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ThirdEyeResultSetMetaData{");
    sb.append("groupKeyColumnNames=").append(groupKeyColumnNames);
    sb.append(", metricColumnNames=").append(metricColumnNames);
    sb.append(", allColumnNames=").append(allColumnNames);
    sb.append('}');
    return sb.toString();
  }
}
