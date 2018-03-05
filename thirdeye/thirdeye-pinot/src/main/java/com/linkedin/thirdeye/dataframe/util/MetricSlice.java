package com.linkedin.thirdeye.dataframe.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import java.util.concurrent.TimeUnit;


public final class MetricSlice {
  public static final TimeGranularity NATIVE_GRANULARITY = new TimeGranularity(0, TimeUnit.MILLISECONDS);
  public static final long DEFAULT_GRANULARITY_OFFSET = 0;

  final long metricId;
  final long start;
  final long end;
  final Multimap<String, String> filters;
  final TimeGranularity granularity;
  final long granularityOffset;

  MetricSlice(long metricId, long start, long end, Multimap<String, String> filters, TimeGranularity granularity, long granularityOffset) {
    this.metricId = metricId;
    this.start = start;
    this.end = end;
    this.filters = filters;
    this.granularity = granularity;
    this.granularityOffset = granularityOffset;
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

  public long getGranularityOffset() {
    return granularityOffset;
  }

  public MetricSlice withMetricId(long metricId) {
    return new MetricSlice(metricId, start, end, filters, granularity, granularityOffset);
  }

  public MetricSlice withStart(long start) {
    return new MetricSlice(metricId, start, end, filters, granularity, granularityOffset);
  }

  public MetricSlice withEnd(long end) {
    return new MetricSlice(metricId, start, end, filters, granularity, granularityOffset);
  }

  public MetricSlice withFilters(Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters, granularity, granularityOffset);
  }

  public MetricSlice withGranularity(TimeGranularity granularity) {
    return new MetricSlice(metricId, start, end, filters, granularity, granularityOffset);
  }

  public MetricSlice withGranularityOffset(long granularityOffset) {
    return new MetricSlice(metricId, start, end, filters, granularity, granularityOffset);
  }

  public static MetricSlice from(long metricId, long start, long end) {
    return new MetricSlice(metricId, start, end, ArrayListMultimap.<String, String>create(), NATIVE_GRANULARITY, DEFAULT_GRANULARITY_OFFSET);
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters) {
    return new MetricSlice(metricId, start, end, filters, NATIVE_GRANULARITY, DEFAULT_GRANULARITY_OFFSET);
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters, TimeGranularity granularity) {
    return new MetricSlice(metricId, start, end, filters, granularity, DEFAULT_GRANULARITY_OFFSET);
  }

  public static MetricSlice from(long metricId, long start, long end, Multimap<String, String> filters, TimeGranularity granularity, long granularityOffset) {
    return new MetricSlice(metricId, start, end, filters, granularity, granularityOffset);
  }
}
