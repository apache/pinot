package com.linkedin.thirdeye.client.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.map.MultiKeyMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow.TimeSeriesMetric;

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
  public Map<DimensionKey, MetricTimeSeries> toMap(TimeSeriesResponse response,
      List<String> schemaDimensions) {
    DimensionKeyGenerator dimensionKeyGenerator = new DimensionKeyGenerator(schemaDimensions);

    List<String> metrics = new ArrayList<>(response.getMetrics());
    List<MetricType> types = Collections.nCopies(metrics.size(), MetricType.DOUBLE);
    MetricSchema metricSchema = new MetricSchema(metrics, types);

    Multimap<DimensionKey, TimeSeriesRow> dimensionKeyToRows = ArrayListMultimap.create();
    // group the rows by their dimension key
    for (int i = 0; i < response.getNumRows(); i++) {
      TimeSeriesRow row = response.getRow(i);
      DimensionKey dimensionKey =
          dimensionKeyGenerator.get(row.getDimensionName(), row.getDimensionValue());
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
          String metricName = metric.getMetricFunction().getMetricName();
          Double value = metric.getValue();
          metricTimeSeries.increment(timestamp, metricName, value);
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

    DimensionKey get(String dimensionName, String dimensionValue) {
      if (dimensionName == null || !dimensionIndexMap.containsKey(dimensionName)) {
        // base key or otherwise not grouped properly
        return baseDimensionKey;
      }
      if (!dimensionKeyCache.containsKey(dimensionName, dimensionValue)) {
        String[] key = Arrays.copyOf(this.baseKey, this.baseKey.length);
        int i = this.dimensionIndexMap.get(dimensionName);
        key[i] = dimensionValue;
        DimensionKey dimensionKey = new DimensionKey(key);
        dimensionKeyCache.put(dimensionName, dimensionValue, dimensionKey);
      }
      return (DimensionKey) dimensionKeyCache.get(dimensionName, dimensionValue);
    }
  }
}
