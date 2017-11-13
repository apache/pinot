package com.linkedin.thirdeye.dataframe.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import java.util.concurrent.TimeUnit;


public final class MetricSlice {
  public static final TimeGranularity NATIVE_GRANULARITY = new TimeGranularity(0, TimeUnit.MILLISECONDS);

  final long metricId;
  final long start;
  final long end;
  final Multimap<String, String> filters;
  final TimeGranularity granularity;

  MetricSlice(long metricId, long start, long end, Multimap<String, String> filters, TimeGranularity granularity) {
    this.metricId = metricId;
    this.start = start;
    this.end = end;
    this.filters = filters;
    this.granularity = granularity;
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

  public TimeGranularity getGranularity() {
    return granularity;
  }

  public MetricSlice withStart(long start) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public MetricSlice withEnd(long end) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public MetricSlice withFilters(Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public MetricSlice withGranularity(TimeGranularity granularity) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }

  public static MetricSlice from(long metricId, long start, long end) {
    return new MetricSlice(metricId, start, end, ArrayListMultimap.<String, String>create(), NATIVE_GRANULARITY);
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters, NATIVE_GRANULARITY);
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters, TimeGranularity granularity) {
    return new MetricSlice(metricId, start, end, filters, granularity);
  }
}
