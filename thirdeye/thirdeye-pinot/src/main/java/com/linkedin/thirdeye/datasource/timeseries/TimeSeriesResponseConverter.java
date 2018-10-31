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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.map.MultiKeyMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.datasource.timeseries.TimeSeriesRow.TimeSeriesMetric;

/**
 * Util class to support the older ThirdEyeDataSource API for time series responses. See
 * {@link #toMap(TimeSeriesResponse, List)}
 */
public class TimeSeriesResponseConverter {
  private static final TimeSeriesResponseConverter instance = new TimeSeriesResponseConverter();

  private TimeSeriesResponseConverter() {
  }

  public static TimeSeriesResponseConverter getInstance() {
    return instance;
  }

  /**
   * Convert the response to a Map<DimensionKey, MetricTimeSeries>. DimensionKey is generated based
   * off of schemaDimensions, while the MetricTimeSeries objects are generated based on the rows
   * within the response input. The metrics returned in the MetricTimeSeries instances correspond to
   * the metric names as opposed to the full metric function (eg __COUNT instead of SUM(__COUNT))
   */
  public static Map<DimensionKey, MetricTimeSeries> toMap(TimeSeriesResponse response,
      List<String> schemaDimensions) {
    DimensionKeyGenerator dimensionKeyGenerator = new DimensionKeyGenerator(schemaDimensions);

    List<String> metrics = new ArrayList<>(response.getMetrics());
    Set<String> metricSet = new HashSet<>(metrics);
    List<MetricType> types = Collections.nCopies(metrics.size(), MetricType.DOUBLE);
    MetricSchema metricSchema = new MetricSchema(metrics, types);

    SetMultimap<DimensionKey, TimeSeriesRow> dimensionKeyToRows = HashMultimap.create();
    // group the rows by their dimension key
    for (int i = 0; i < response.getNumRows(); i++) {
      TimeSeriesRow row = response.getRow(i);
      DimensionKey dimensionKey =
          dimensionKeyGenerator.get(row.getDimensionNames(), row.getDimensionValues());
      dimensionKeyToRows.put(dimensionKey, row);
    }

    Map<DimensionKey, MetricTimeSeries> result = new HashMap<>();
    for (Entry<DimensionKey, Collection<TimeSeriesRow>> entry : dimensionKeyToRows.asMap()
        .entrySet()) {
      DimensionKey key = entry.getKey();
      MetricTimeSeries metricTimeSeries = new MetricTimeSeries(metricSchema);
      result.put(key, metricTimeSeries);
      for (TimeSeriesRow timeSeriesRow : entry.getValue()) {
        long timestamp = timeSeriesRow.getStart();
        for (TimeSeriesMetric metric : timeSeriesRow.getMetrics()) {
          String metricName = metric.getMetricName();
          // Only add the row metric if it's listed in the response object. The row metric may
          // contain additional info, eg the raw metrics required for calculating derived ones.
          if (metricSet.contains(metricName)) {
            Double value = metric.getValue();
            metricTimeSeries.increment(timestamp, metricName, value);
          }
        }
      }
    }
    return result;
  }

  private static class DimensionKeyGenerator {
    private final String[] baseKey;
    private final DimensionKey baseDimensionKey;
    private final Map<String, Integer> dimensionIndexMap = new HashMap<>();
    private final MultiKeyMap dimensionKeyCache = new MultiKeyMap();

    DimensionKeyGenerator(List<String> dimensions) {
      this.baseKey = new String[dimensions.size()];
      Arrays.fill(this.baseKey, "*");
      this.baseDimensionKey = new DimensionKey(baseKey);
      for (int i = 0; i < dimensions.size(); i++) {
        String dimension = dimensions.get(i);
        dimensionIndexMap.put(dimension, i);
      }
    }

    DimensionKey get(List<String> dimensionNames, List<String> dimensionValues) {
      // returns base key if the input is not grouped properly
      if (dimensionNames == null) {
        return baseDimensionKey;
      }
      for (String dimensionName : dimensionNames) {
        if (!dimensionIndexMap.containsKey(dimensionName)) {
          return baseDimensionKey;
        }
      }
      if (!dimensionKeyCache.containsKey(dimensionNames, dimensionValues)) {
        String[] key = Arrays.copyOf(this.baseKey, this.baseKey.length);
        for (int idx = 0; idx < dimensionNames.size(); ++idx) {
          int i = this.dimensionIndexMap.get(dimensionNames.get(idx));
          key[i] = dimensionValues.get(idx);
        }
        DimensionKey dimensionKey = new DimensionKey(key);
        dimensionKeyCache.put(dimensionNames, dimensionValues, dimensionKey);
        return dimensionKey;
      } else {
        return (DimensionKey) dimensionKeyCache.get(dimensionNames, dimensionValues);
      }
    }
  }
}
