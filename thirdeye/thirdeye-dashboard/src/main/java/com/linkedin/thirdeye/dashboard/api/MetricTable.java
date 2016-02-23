package com.linkedin.thirdeye.dashboard.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class MetricTable {
  private final int metricCount;
  private final List<MetricDataRow> rows;
  private List<MetricDataRow> cumulativeRows;

  public MetricTable(List<MetricDataRow> rows, int metricCount) {
    this.metricCount = metricCount;
    this.rows = rows;
    this.cumulativeRows = null; // calculated on demand and cached if needed.
  }

  public int getMetricCount() {
    return metricCount;
  }

  public List<MetricDataRow> getRows() {
    return rows;
  }

  public List<MetricDataRow> getCumulativeRows() {
    if (cumulativeRows == null) {
      cumulativeRows = computeCumulativeRows(rows, metricCount);
    }
    return cumulativeRows;
  }

  /**
   * Computes cumulative values for the input rows. <tt>metricCount</tt> should be
   * the number of values in each baseline/current dataset for each row.
   */
  private List<MetricDataRow> computeCumulativeRows(List<MetricDataRow> rows, int metricCount) {

    if (rows.isEmpty()) {
      return Collections.emptyList();
    }
    List<MetricDataRow> cumulativeRows = new LinkedList<>();
    Number[] cumulativeBaselineValues = new Number[metricCount];
    Arrays.fill(cumulativeBaselineValues, 0.0);
    Number[] cumulativeCurrentValues = new Number[metricCount];
    Arrays.fill(cumulativeCurrentValues, 0.0);

    for (MetricDataRow row : rows) {
      Number[] baselineValues = row.getBaseline();
      if (baselineValues != null) {
        for (int i = 0; i < baselineValues.length; i++) {
          cumulativeBaselineValues[i] = cumulativeBaselineValues[i].doubleValue()
              + (baselineValues[i] == null ? 0.0 : baselineValues[i].doubleValue());
        }
      }

      Number[] currentValues = row.getCurrent();
      if (currentValues != null) {
        for (int i = 0; i < currentValues.length; i++) {
          cumulativeCurrentValues[i] = cumulativeCurrentValues[i].doubleValue()
              + (currentValues[i] == null ? 0.0 : currentValues[i].doubleValue());
        }
      }

      Number[] cumulativeBaselineValuesCopy =
          Arrays.copyOf(cumulativeBaselineValues, cumulativeBaselineValues.length);
      Number[] cumulativeCurrentValuesCopy =
          Arrays.copyOf(cumulativeCurrentValues, cumulativeCurrentValues.length);

      MetricDataRow cumulativeRow = new MetricDataRow(row.getBaselineTime(),
          cumulativeBaselineValuesCopy, row.getCurrentTime(), cumulativeCurrentValuesCopy);
      cumulativeRows.add(cumulativeRow);
    }
    return cumulativeRows;
  }

  // Debugging only: prints rows
  @Override
  public String toString() {
    return StringUtils.join(rows, ",");
  }
}
