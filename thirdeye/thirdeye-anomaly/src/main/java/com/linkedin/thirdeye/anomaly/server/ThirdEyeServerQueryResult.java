package com.linkedin.thirdeye.anomaly.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDataset;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.dashboard.api.QueryResult;

/**
 * Parses query result from the Third Eye server back into dimension keys and their associated metric time series.
 */
public class ThirdEyeServerQueryResult extends AnomalyDetectionDataset {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public ThirdEyeServerQueryResult(List<MetricSpec> metricSpecs, QueryResult queryResult) throws IOException {
    dimensions = queryResult.getDimensions();
    metrics = queryResult.getMetrics();

    MetricSchema metricSchema = MetricSchema.fromMetricSpecs(metricSpecs);

    for (Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
      DimensionKey dimensionKey = dimensionKeyFromString(entry.getKey());
      MetricTimeSeries metricTimeSeries = metricTimeSeriesFromMap(metricSchema, metrics, entry.getValue());
      data.put(dimensionKey, metricTimeSeries);
    }
  }

  private DimensionKey dimensionKeyFromString(String dimensionKeyString) throws JsonParseException,
    JsonMappingException, IOException {
    String[] dimensionValues = OBJECT_MAPPER.readValue(dimensionKeyString, String[].class);
    return new DimensionKey(dimensionValues);
  }

  private MetricTimeSeries metricTimeSeriesFromMap(MetricSchema metricSchema, List<String> metrics,
      Map<String, Number[]> seriesMap) {
    MetricTimeSeries metricTimeSeries = new MetricTimeSeries(metricSchema);
    for (Entry<String, Number[]> entry : seriesMap.entrySet()) {
      long timeWindow = Long.valueOf(entry.getKey());
      Number[] value = entry.getValue();
      for (int i = 0; i < metrics.size(); i++) {
        metricTimeSeries.set(timeWindow, metrics.get(i), value[i]);
      }
    }
    return metricTimeSeries;
  }
}
