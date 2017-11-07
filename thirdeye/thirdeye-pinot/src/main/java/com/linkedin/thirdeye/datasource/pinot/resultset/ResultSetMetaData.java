package com.linkedin.thirdeye.datasource.pinot.resultset;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

public class ResultSetMetaData {
  private List<String> groupKeyColumnNames = Collections.emptyList();
  private List<String> metricColumnNames = Collections.emptyList();
  private List<String> allColumnNames = Collections.emptyList();

  public ResultSetMetaData(List<String> groupKeyColumnNames, List<String> metricColumnNames) {
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
}
