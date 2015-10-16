package com.linkedin.thirdeye.dashboard.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.database.AnomalyTableRow;
import com.linkedin.thirdeye.api.DimensionKey;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

public class FlotTimeSeries {
  private final String metric;
  private final String dimensions;
  private final String dimensionNames;
  private final String label;
  private final List<Number[]> data;
  private final MultivaluedMap<String, String> annotations;

  public FlotTimeSeries(String metric,
                        String dimensions,
                        String dimensionNames,
                        String label,
                        List<Number[]> data,
                        MultivaluedMap<String, String> annotations) {
    this.metric = metric;
    this.dimensions = dimensions;
    this.dimensionNames = dimensionNames;
    this.label = label;
    this.data = data;
    this.annotations = annotations;
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

  @JsonProperty
  public MultivaluedMap<String, String> getAnnotations() {
    return annotations;
  }

  public static List<FlotTimeSeries> fromQueryResult(
      CollectionSchema schema,
      ObjectMapper objectMapper,
      QueryResult queryResult) throws Exception {
    return fromQueryResult(schema, objectMapper, queryResult, null);
  }

  public static List<FlotTimeSeries> fromQueryResult(
      CollectionSchema schema,
      ObjectMapper objectMapper,
      QueryResult queryResult,
      String labelPrefix) throws Exception {
    List<FlotTimeSeries> allSeries = new ArrayList<>();

    String dimensionNamesJson = objectMapper.writeValueAsString(queryResult.getDimensions());

    Map<String, String> aliases = getAliases(schema);

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

        // Metric name or alias
        String metricName = metricSeriesEntry.getKey();
        String metricAlias = aliases.get(metricName);
        if (metricAlias == null) {
          label.append(metricName);
        } else {
          label.append(metricAlias);
        }

        if (queryResult.getData().size() > 1) {
          label.append(" (").append(entry.getKey()).append(")"); // multi-dimensional
        }

        if (series.size() > 1) {
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
            series,
            null));
      }
    }

    return allSeries;
  }

  public static List<FlotTimeSeries> anomaliesFromQueryResult(
      CollectionSchema schema,
      ObjectMapper objectMapper,
      QueryResult queryResult,
      String labelPrefix,
      List<AnomalyTableRow> anomalies) throws Exception {

    // remap anomalies by dimension key
    Multimap<DimensionKey, AnomalyTableRow> anomaliesByDimensionKey = groupAnomaliesByDimensionKey(schema, anomalies);

    // annotations

    List<FlotTimeSeries> allSeries = new ArrayList<>();
    String dimensionNamesJson = objectMapper.writeValueAsString(queryResult.getDimensions());

    // Mapping of metric name to alias if exists
    Map<String, String> aliases = getAliases(schema);

    for (Map.Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
      Map<String, List<Number[]>> timeSeriesByMetric = new HashMap<>();
      Map<String, MultivaluedMap<String, String>> annotationsByMetric = new HashMap<>();
      for (String metric : queryResult.getMetrics()) {
        timeSeriesByMetric.put(metric, new ArrayList<Number[]>());
        annotationsByMetric.put(metric, new MultivaluedMapImpl());
      }

      String[] dimensionValues = objectMapper.reader(String[].class).readValue(entry.getKey());
      DimensionKey dimensionKey = new DimensionKey(dimensionValues);

      for (AnomalyTableRow anomaly : anomaliesByDimensionKey.get(dimensionKey)) {
        long timeWindow = anomaly.getTimeWindow();
        String timeWindowString = Long.toString(timeWindow);
        Number[] metricValuesInResult = entry.getValue().get(timeWindowString);

        for (String anomalousMetric : anomaly.getMetrics()) {
          int anomalousMetricIndex = queryResult.getMetrics().indexOf(anomalousMetric);
          if (anomalousMetricIndex != -1) {
            Number metricValue = (metricValuesInResult != null) ? metricValuesInResult[anomalousMetricIndex] : 0;
            timeSeriesByMetric.get(anomalousMetric).add(new Number[] { timeWindow , metricValue });
            annotationsByMetric.get(anomalousMetric).add(timeWindowString,
                anomalyTableRowToAnnotation(anomaly));
          }
        }
      }

      for (Map.Entry<String, List<Number[]>> metricSeriesEntry : timeSeriesByMetric.entrySet()) {
        List<Number[]> series = metricSeriesEntry.getValue();

        Collections.sort(series, COMPARATOR_BY_TIME);

        StringBuilder label = new StringBuilder();
        label.append(labelPrefix);

        // Metric name or alias
        String metricName = metricSeriesEntry.getKey();
        String metricAlias = aliases.get(metricName);
        if (metricAlias == null) {
          label.append(metricName);
        } else {
          label.append(metricAlias);
        }

        if (series.size() == 1) {
          label.append(String.format(" (%d anomaly)", series.size()));
        } else {
          label.append(String.format(" (%d anomalies)", series.size()));
        }

        if (queryResult.getData().size() > 1) {
          label.append(" (").append(entry.getKey()).append(")"); // multi-dimensional
        }

        allSeries.add(new FlotTimeSeries(
            metricSeriesEntry.getKey(),
            entry.getKey(),
            dimensionNamesJson,
            label.toString(),
            series,
            annotationsByMetric.get(metricName)));
      }
    }
    return allSeries;
  }

  private static String anomalyTableRowToAnnotation(AnomalyTableRow anomaly) {
    StringBuilder sb = new StringBuilder()
      .append(String.format("<b>id</b>: %d (%s_%d)<br>", anomaly.getId(), anomaly.getFunctionTable(),
          anomaly.getFunctionId()))
      .append(String.format("<b>name</b>: %s<br>", anomaly.getFunctionName()))
      .append(String.format("<b>description</b>: %s<br>", anomaly.getFunctionDescription()))
      .append(String.format("<b>score, volume</b>: %.03f, %.03f<br>", anomaly.getAnomalyScore(),
          anomaly.getAnomalyVolume()))
      .append(anomaly.getProperties().toString());
    return sb.toString();
  }

  private static Multimap<DimensionKey, AnomalyTableRow> groupAnomaliesByDimensionKey(CollectionSchema schema,
      List<AnomalyTableRow> anomalies) {
    Multimap<DimensionKey, AnomalyTableRow> anomaliesByDimensionKey = ArrayListMultimap.create();
    for (AnomalyTableRow anomaly : anomalies) {
      String[] dimensionValues = new String[schema.getDimensions().size()];
      int dimensionIndex = 0;
      for (String dimension : schema.getDimensions()) {
        dimensionValues[dimensionIndex] = anomaly.getDimensions().get(dimension);
        dimensionIndex++;
      }
      DimensionKey dimensionKey = new DimensionKey(dimensionValues);
      anomaliesByDimensionKey.put(dimensionKey, anomaly);
    }
    return anomaliesByDimensionKey;
  }

  private static Map<String, String> getAliases(CollectionSchema schema) {
    Map<String, String> aliases = new HashMap<>();
    for (int i = 0; i < schema.getMetrics().size(); i++) {
      aliases.put(schema.getMetrics().get(i), schema.getMetricAliases().get(i));
    }
    return aliases;
  }

  private static final Comparator<Number[]> COMPARATOR_BY_TIME = new Comparator<Number[]>() {
    @Override
    public int compare(Number[] o1, Number[] o2) {
      return (int) (o1[0].doubleValue() - o2[0].doubleValue());
    }
  };
}
