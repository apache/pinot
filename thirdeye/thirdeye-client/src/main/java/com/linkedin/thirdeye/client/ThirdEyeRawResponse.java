package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;

public class ThirdEyeRawResponse {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference<List<String>> LIST_TYPE_REF =
      new TypeReference<List<String>>() {
      };

  private List<String> metrics;
  private List<String> dimensions;
  private Map<String, Map<String, Number[]>> data;

  public ThirdEyeRawResponse() {
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  /** Key should be dimension key followed by timestamp. */
  public Map<String, Map<String, Number[]>> getData() {
    return data;
  }

  /** Key should be dimension key followed by timestamp. */
  public void setData(Map<String, Map<String, Number[]>> data) {
    this.data = data;
  }

  /**
   * Converts this response to a Map<DimensionKey, MetricTimeSeries> using the provided metric types
   * for each metric. If the metricType is null, this method assumes the corresponding metric is a
   * derived metric and should be interpreted as a Double.
   */
  public Map<DimensionKey, MetricTimeSeries> convert(List<MetricType> metricTypes)
      throws Exception {
    List<MetricType> filteredMetricTypes = new ArrayList<>(metricTypes);
    for (int i = 0; i < filteredMetricTypes.size(); i++) {
      MetricType metricType = filteredMetricTypes.get(i);
      if (metricType == null) {
        filteredMetricTypes.set(i, MetricType.DOUBLE);
      }
    }
    MetricSchema metricSchema = new MetricSchema(metrics, filteredMetricTypes);

    // Convert raw data
    Map<DimensionKey, MetricTimeSeries> converted = new HashMap<>();
    for (Map.Entry<String, Map<String, Number[]>> entry : data.entrySet()) {
      // Dimension
      String dimensionString = entry.getKey();
      List<String> dimensionValues = OBJECT_MAPPER.readValue(dimensionString, LIST_TYPE_REF);
      String[] valueArray = new String[dimensionValues.size()];
      dimensionValues.toArray(valueArray);
      DimensionKey dimensionKey = new DimensionKey(valueArray);

      // Metrics / time
      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
      for (Map.Entry<String, Number[]> point : entry.getValue().entrySet()) {
        Long time = Long.valueOf(point.getKey());
        for (int i = 0; i < metrics.size(); i++) {
          String metricName = metrics.get(i);
          Number metricValue = point.getValue()[i];
          timeSeries.increment(time, metricName, metricValue);
        }
      }

      converted.put(dimensionKey, timeSeries);
    }

    return converted;
  }
}
