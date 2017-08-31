package com.linkedin.thirdeye.dataframe.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


public final class MetricSlice {
  final long metricId;
  final long start;
  final long end;
  final Multimap<String, String> filters;

  MetricSlice(long metricId, long start, long end, Multimap<String, String> filters) {
    this.metricId = metricId;
    this.start = start;
    this.end = end;
    this.filters = filters;
  }

  public long getMetricId() {
    return metricId;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public Multimap<String, String> getFilters() {
    return filters;
  }

  public MetricSlice withStart(long start) {
    return new MetricSlice(metricId, start, end, filters);
  }

  public MetricSlice withEnd(long end) {
    return new MetricSlice(metricId, start, end, filters);
  }

  public MetricSlice withFilters(Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters);
  }

  public static MetricSlice from(long metricId, long start, long end) {
    return new MetricSlice(metricId, start, end, ArrayListMultimap.<String, String>create());
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters);
  }
}
