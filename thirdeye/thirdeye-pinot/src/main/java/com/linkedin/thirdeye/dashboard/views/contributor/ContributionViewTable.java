package com.linkedin.thirdeye.dashboard.views.contributor;

import java.util.List;

public class ContributionViewTable {

  private List<ContributionCell> cells;
  private String metricName;
  private String dimensionName;

  public ContributionViewTable(String metricName, String dimensionName,
      List<ContributionCell> cells) {
    this.metricName = metricName;
    this.dimensionName = dimensionName;
    this.cells = cells;
  }

  public List<ContributionCell> getCells() {
    return cells;
  }

  public String getMetricName() {
    return metricName;
  }

  public String getDimensionName() {
    return dimensionName;
  }
}
