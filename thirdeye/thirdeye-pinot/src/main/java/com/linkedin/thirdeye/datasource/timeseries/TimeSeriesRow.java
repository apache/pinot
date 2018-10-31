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

package com.linkedin.thirdeye.datasource.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang.ObjectUtils;
import org.joda.time.DateTime;

public class TimeSeriesRow implements Comparable<TimeSeriesRow> {
  private final long start;
  private final long end;
  private final List<String> dimensionNames;
  private final List<String> dimensionValues;
  private final List<TimeSeriesMetric> metrics;

  private static final String DELIM = "\t\t";

  private TimeSeriesRow(Builder builder) {
    this.start = builder.start.getMillis();
    this.end = builder.end.getMillis();
    this.dimensionNames = builder.dimensionNames;
    this.dimensionValues = builder.dimensionValues;
    this.metrics = builder.metrics;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<String> getDimensionValues() {
    return dimensionValues;
  }

  public List<TimeSeriesMetric> getMetrics() {
    return metrics;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TimeSeriesRow)) {
      return false;
    }
    TimeSeriesRow ts = (TimeSeriesRow) o;
    return Objects.equals(start, ts.getStart())
        && Objects.equals(end, ts.getEnd())
        && Objects.equals(dimensionNames, ts.getDimensionNames())
        && Objects.equals(dimensionValues, ts.getDimensionValues())
        && Objects.equals(metrics, ts.getMetrics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end, dimensionNames, dimensionValues, metrics);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(start).append(DELIM).append(end).append(DELIM).append(dimensionNames).append(DELIM)
        .append(dimensionValues);
    for (TimeSeriesMetric metric : metrics) {
      sb.append(DELIM).append(metric.getValue());
    }
    return sb.toString();
  }

  @Override
  public int compareTo(TimeSeriesRow o) {

    int startDiff = ObjectUtils.compare(this.start, o.start);
    if (startDiff != 0)
      return startDiff;

    int dimensionNameDiff = compareListOfComparable(this.dimensionNames, o.dimensionNames);
    if (dimensionNameDiff != 0)
      return dimensionNameDiff;
    int dimensionValueDiff = compareListOfComparable(this.dimensionValues, o.dimensionValues);
    if (dimensionValueDiff != 0)
      return dimensionValueDiff;
    return compareListOfComparable(this.metrics, o.metrics);
  }

  private static <E extends Comparable<E>> int compareListOfComparable(List<E> a, List<E> b) {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;

    int sizeA = a.size();
    int sizeB = b.size();
    int sizeDiff = ObjectUtils.compare(sizeA, sizeB);
    if (sizeDiff != 0) {
      return sizeDiff;
    }
    for (int i = 0; i < sizeA; i++) {
      E comparableA = a.get(i);
      E comparableB = b.get(i);
      int comparableDiff = ObjectUtils.compare(comparableA, comparableB);
      if (comparableDiff != 0) {
        return comparableDiff;
      }
    }
    return 0;
  }

  static class Builder {
    private DateTime start;
    private DateTime end;
    private List<String> dimensionNames = Collections.singletonList("all");
    private List<String> dimensionValues = Collections.singletonList("all");
    private final List<TimeSeriesMetric> metrics = new ArrayList<>();

    public void setStart(DateTime start) {
      this.start = start;
    }

    public void setEnd(DateTime end) {
      this.end = end;
    }

    public void setDimensionNames(List<String> dimensionNames) {
      this.dimensionNames = dimensionNames;
    }

    public void setDimensionValues(List<String> dimensionValues) {
      this.dimensionValues = dimensionValues;
    }

    public void addMetric(String metricName, double value) {
      this.metrics.add(new TimeSeriesMetric(metricName, value));
    }

    public void addMetrics(TimeSeriesMetric... metrics) {
      Collections.addAll(this.metrics, metrics);
    }

    public TimeSeriesRow build() {
      return new TimeSeriesRow(this);
    }

    public void clear() {
      setStart(null);
      setEnd(null);
      setDimensionNames(null);
      setDimensionValues(null);
      this.metrics.clear();
    }
  }

  public static class TimeSeriesMetric implements Comparable<TimeSeriesMetric> {
    private final String metricName;
    private final Double value;

    public TimeSeriesMetric(String metricName, Double value) {
      this.metricName = metricName;
      this.value = value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(metricName, value);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof TimeSeriesMetric)) {
        return false;
      }
      TimeSeriesMetric ts = (TimeSeriesMetric) o;
      return Objects.equals(metricName, ts.getMetricName())
          && Objects.equals(value, ts.getValue());
    }

    @Override
    public int compareTo(TimeSeriesMetric otherMetric) {
      // shouldn't ever need to compare by value
      return this.metricName.compareTo(otherMetric.metricName);
    }

    public String getMetricName() {
      return metricName;
    }

    public Double getValue() {
      return value;
    }

  }
}
