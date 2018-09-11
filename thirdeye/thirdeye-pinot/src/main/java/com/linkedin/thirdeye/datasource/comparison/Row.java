/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource.comparison;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

// TODO rename to TimeOnTimeComparisonRow?
public class Row {

  private Row() {

  }

  DateTime baselineStart;

  DateTime baselineEnd;

  DateTime currentStart;

  DateTime currentEnd;

  String dimensionName = "all";

  String dimensionValue = "all";

  List<Row.Metric> metrics = new ArrayList<>();

  static String DELIM = "\t\t";

  public DateTime getBaselineStart() {
    return baselineStart;
  }

  public DateTime getBaselineEnd() {
    return baselineEnd;
  }

  public DateTime getCurrentStart() {
    return currentStart;
  }

  public DateTime getCurrentEnd() {
    return currentEnd;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }

  public List<Row.Metric> getMetrics() {
    return metrics;
  }

  public static void setDELIM(String dELIM) {
    DELIM = dELIM;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(baselineStart).append(DELIM).append(baselineEnd).append(DELIM);
    sb.append(currentStart).append(DELIM).append(currentEnd).append(DELIM);
    sb.append(dimensionName).append(DELIM).append(dimensionValue);
    for (Metric metric : metrics) {
      sb.append(DELIM).append(metric.baselineValue).append(DELIM).append(metric.currentValue);
    }
    return sb.toString();
  }

  static class Builder {

    Row row;

    public Builder() {
      row = new Row();
    }

    public void setBaselineStart(DateTime baselineStart) {
      row.baselineStart = baselineStart;
    }

    public void setBaselineEnd(DateTime baselineEnd) {
      row.baselineEnd = baselineEnd;
    }

    public void setCurrentStart(DateTime currentStart) {
      row.currentStart = currentStart;
    }

    public void setCurrentEnd(DateTime currentEnd) {
      row.currentEnd = currentEnd;
    }

    public void setDimensionName(String dimensionName) {
      row.dimensionName = dimensionName;
    }

    public void setDimensionValue(String dimensionValue) {
      row.dimensionValue = dimensionValue;
    }

    public void addMetric(String metricName, double baselineValue, double currentValue) {
      row.metrics.add(new Metric(metricName, baselineValue, currentValue));
    }

    public Row build() {
      return row;
    }

  }

  public static class Metric {
    String metricName;
    Double baselineValue;
    Double currentValue;

    public String getMetricName() {
      return metricName;
    }

    public Double getBaselineValue() {
      return baselineValue;
    }

    public Double getCurrentValue() {
      return currentValue;
    }

    public Metric(String metricName, Double baselineValue, Double currentValue) {
      super();
      this.metricName = metricName;
      this.baselineValue = baselineValue;
      this.currentValue = currentValue;
    }
  }

}
