package org.apache.pinot.thirdeye.detection.cache;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.util.CacheUtils;


public class MetricCacheResponse {
  final List<String> times;
  final List<String> values;

  public MetricCacheResponse(List<String> times, List<String> values) {
    this.times = times;
    this.values = values;
  }

  public List<String> getTimes() { return this.times; }

  public List<String> getValues() { return this.values; }
}
