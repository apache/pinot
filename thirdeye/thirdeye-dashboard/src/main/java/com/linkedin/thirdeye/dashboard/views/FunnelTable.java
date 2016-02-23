package com.linkedin.thirdeye.dashboard.views;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.dashboard.api.MetricDataRow;
import com.linkedin.thirdeye.dashboard.api.MetricTable;
import com.linkedin.thirdeye.dashboard.api.funnel.FunnelSpec;

public class FunnelTable {
  private final String name;

  private final Map<String, String> aliasToActualMap;
  private final MetricTable metricTable;
  private final String current;
  private final String baseline;

  public FunnelTable(FunnelSpec spec, MetricTable metricTable, DateTime current,
      DateTime baseline) {
    this.metricTable = metricTable;
    this.name = spec.getName();
    aliasToActualMap = spec.getAliasToActualMetrics();
    this.current = current.toLocalDate().toString();
    this.baseline = baseline.toLocalDate().toString();
  }

  public String getCurrent() {
    return current;
  }

  public String getBaseline() {
    return baseline;
  }

  public Map<String, String> getAliasToActualMap() {
    return aliasToActualMap;
  }

  public String getName() {
    return name;
  }

  public List<MetricDataRow> getTable() {
    return metricTable.getRows();
  }

  public List<MetricDataRow> getCumulativeTable() {
    return metricTable.getCumulativeRows();
  }
}
