package com.linkedin.thirdeye.dashboard.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class FlotTimeSeries {
  private final String metric;
  private final String dimensions;
  private final String dimensionNames;
  private final String label;
  private final List<Number[]> data;

  public FlotTimeSeries(String metric,
                        String dimensions,
                        String dimensionNames,
                        String label,
                        List<Number[]> data) {
    this.metric = metric;
    this.dimensions = dimensions;
    this.dimensionNames = dimensionNames;
    this.label = label;
    this.data = data;
  }

  @JsonProperty
  public String getMetric() {
    return metric;
  }

  @JsonProperty
  public String getDimensionNames() {
    return dimensionNames;
  }

  @JsonProperty
  public String getDimensions() {
    return dimensions;
  }

  @JsonProperty
  public String getLabel() {
    return label;
  }

  @JsonProperty
  public List<Number[]> getData() {
    return data;
  }

  public static List<FlotTimeSeries> fromQueryResult(ObjectMapper objectMapper, QueryResult queryResult) throws Exception {
    return fromQueryResult(objectMapper, queryResult, null);
  }

  public static List<FlotTimeSeries> fromQueryResult(
      ObjectMapper objectMapper,
      QueryResult queryResult,
      String labelPrefix) throws Exception {
    List<FlotTimeSeries> allSeries = new ArrayList<>();

    String dimensionNamesJson = objectMapper.writeValueAsString(queryResult.getDimensions());

    for (Map.Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
      Map<String, List<Number[]>> timeSeriesByMetric = new HashMap<>();
      for (String metric : queryResult.getMetrics()) {
        timeSeriesByMetric.put(metric, new ArrayList<Number[]>());
      }

      for (Map.Entry<String, Number[]> timeEntry : entry.getValue().entrySet()) {
        Long time = Long.valueOf(timeEntry.getKey()); // must not use ISO8601 strings, instead epoch time
        for (int i = 0; i < queryResult.getMetrics().size(); i++) {
          String name = queryResult.getMetrics().get(i);
          Number value = timeEntry.getValue()[i];
          timeSeriesByMetric.get(name).add(new Number[] { time, value });
        }
      }

      for (Map.Entry<String, List<Number[]>> metricSeriesEntry : timeSeriesByMetric.entrySet()) {
        List<Number[]> series = metricSeriesEntry.getValue();

        Collections.sort(series, COMPARATOR_BY_TIME);

        StringBuilder label = new StringBuilder();

        if (labelPrefix != null) {
          label.append(labelPrefix);
        }

        label.append(metricSeriesEntry.getKey());

        if (queryResult.getData().size() > 1) {
          label.append(" (").append(entry.getKey()).append(")"); // multi-dimensional
        }

        if (!series.isEmpty()) {
          double baseline = series.get(0)[1].doubleValue();
          if (baseline > 0) {
            double current = series.get(series.size() - 1)[1].doubleValue();
            double percentChange = 100 * (current - baseline) / baseline;
            label.append(String.format(" (%.2f%%)", percentChange));
          } else {
            label.append(" (N/A)");
          }
        }

        allSeries.add(new FlotTimeSeries(
            metricSeriesEntry.getKey(),
            entry.getKey(),
            dimensionNamesJson,
            label.toString(),
            series));
      }
    }

    return allSeries;
  }

  private static final Comparator<Number[]> COMPARATOR_BY_TIME = new Comparator<Number[]>() {
    @Override
    public int compare(Number[] o1, Number[] o2) {
      return (int) (o1[0].doubleValue() - o2[0].doubleValue());
    }
  };
}
