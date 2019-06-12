/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dataframe.util;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.rootcause.Entity;


/**
 * Selector for time series and aggregate values of a specific metric, independent of
 * data source.
 */
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

  /**
   * check if current metric slice contains another metric slice
   */
  public boolean containSlice(MetricSlice slice) {
    return slice.metricId == this.metricId && slice.granularity.equals(this.granularity) && filtersEquals(this.getFilters(), slice.getFilters()) &&
        slice.start >= this.start && slice.end <= this.end;
  }

  /**
   * check if two filter multi-maps are equal regardless of dimension value orders
   * @param filters1 filter 1
   * @param filters2 filter 2
   * @return <tt>true</tt> if two filters are equal
   */
  private static boolean filtersEquals(Multimap<String, String> filters1, Multimap<String, String> filters2) {
    Map<String, Collection<String>> filterMaps1 = filters1.asMap();
    Map<String, Collection<String>> filterMaps2 = filters2.asMap();

    if (!filterMaps1.keySet().equals(filterMaps2.keySet())) {
      return false;
    }
    for (Map.Entry<String, Collection<String>> entry : filterMaps1.entrySet()) {
      Collection<String> filter1Values = entry.getValue();
      Collection<String> filter2Values = filterMaps2.get(entry.getKey());
      if (!(filter1Values.containsAll(filter2Values) && filter2Values.containsAll(filter1Values))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricSlice that = (MetricSlice) o;
    return metricId == that.metricId && start == that.start && end == that.end && Objects.equals(filters, that.filters)
        && Objects.equals(granularity, that.granularity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricId, start, end, filters, granularity);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
