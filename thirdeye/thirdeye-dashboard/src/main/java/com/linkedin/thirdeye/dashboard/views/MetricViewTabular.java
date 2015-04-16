package com.linkedin.thirdeye.dashboard.views;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.MetricTable;
import com.linkedin.thirdeye.dashboard.api.MetricTableRow;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import io.dropwizard.views.View;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.*;

public class MetricViewTabular extends View {
  private static final TypeReference<List<String>> STRING_LIST_REF = new TypeReference<List<String>>(){};

  private final ObjectMapper objectMapper;
  private final QueryResult baselineResult;
  private final QueryResult currentResult;
  private final List<MetricTable> metricTables;
  private final long baselineOffsetMillis;

  public MetricViewTabular(ObjectMapper objectMapper,
                           QueryResult baselineResult,
                           QueryResult currentResult,
                           long baselineOffsetMillis) throws Exception {
    super("metric/table.ftl");
    this.objectMapper = objectMapper;
    this.baselineResult = baselineResult;
    this.currentResult = currentResult;
    this.baselineOffsetMillis = baselineOffsetMillis;
    this.metricTables = generateMetricTables();
  }

  public List<MetricTable> getMetricTables() {
    return metricTables;
  }

  public List<String> getMetricNames() {
    return currentResult.getMetrics();
  }

  private Map<String, String> getDimensionValues(String dimensionKey) throws Exception {
    Map<String, String> valueMap = new TreeMap<>();
    List<String> dimensionNames = currentResult.getDimensions();
    List<String> dimensionValues = objectMapper.readValue(dimensionKey.getBytes(), STRING_LIST_REF);

    for (int i = 0; i < dimensionNames.size(); i++) {
      valueMap.put(dimensionNames.get(i), dimensionValues.get(i));
    }

    return valueMap;
  }

  private List<MetricTable> generateMetricTables() throws Exception {
    List<MetricTable> tables = new ArrayList<>();

    for (Map.Entry<String, Map<String, Number[]>> entry : currentResult.getData().entrySet()) {
      Map<String, Number[]> currentSeries = entry.getValue();
      Map<String, Number[]> baselineSeries = baselineResult.getData().get(entry.getKey());

      if (baselineSeries == null) {
        continue;
      }

      Map<String, String> dimensionValues = getDimensionValues(entry.getKey());

      // Get sorted current times
      List<Long> times = new ArrayList<>(currentSeries.size());
      for (String timeString : entry.getValue().keySet()) {
        times.add(Long.parseLong(timeString));
      }
      Collections.sort(times);

      // Compute cells
      List<MetricTableRow> rows = new ArrayList<>();
      for (Long time : times) {
        Number[] current = currentSeries.get(Long.toString(time));
        Number[] baseline = baselineSeries.get(Long.toString(time - baselineOffsetMillis));

        if (baseline == null) {
          throw new IllegalStateException("No baseline for time " + time);
        }

        rows.add(new MetricTableRow(
            new DateTime(time - baselineOffsetMillis).toDateTime(DateTimeZone.UTC),
            baseline,
            new DateTime(time).toDateTime(DateTimeZone.UTC),
            current));
      }

      tables.add(new MetricTable(dimensionValues, rows));
    }

    return tables;
  }
}
