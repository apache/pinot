package com.linkedin.thirdeye.dashboard.views;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.MetricDataRow;
import com.linkedin.thirdeye.dashboard.api.MetricTable;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;

import io.dropwizard.views.View;


public class MetricViewTabular extends View {
  private static final TypeReference<List<String>> STRING_LIST_REF = new TypeReference<List<String>>() {
  };

  private final CollectionSchema collectionSchema;
  private final ObjectMapper objectMapper;
  private final QueryResult result;
  private final List<MetricTable> metricTables;
  private final long currentMillis;
  private final long baselineOffsetMillis;
  private final long intraDayPeriod;
  private final Map<String, String> metricAliases;
  private final Map<String, String> dimensionAliases;

  public MetricViewTabular(CollectionSchema collectionSchema, ObjectMapper objectMapper, QueryResult result,
      long currentMillis, long baselineOffsetMillis, long intraDayPeriod) throws Exception {
    super("metric/intra-day.ftl");
    this.collectionSchema = collectionSchema;
    this.objectMapper = objectMapper;
    this.result = result;
    this.currentMillis = currentMillis;
    this.baselineOffsetMillis = baselineOffsetMillis;
    this.intraDayPeriod = intraDayPeriod;
    this.metricTables = generateMetricTables();
    this.metricAliases = generateMetricAliases();
    this.dimensionAliases = generateDimensionAliases();
  }

  public List<MetricTable> getMetricTables() {
    return metricTables;
  }

  public List<String> getMetricNames() {
    return result.getMetrics();
  }

  public Map<String, String> getMetricAliases() {
    return metricAliases;
  }

  public Map<String, String> getDimensionAliases() {
    return dimensionAliases;
  }

  private Map<String, String> generateMetricAliases() {
    Map<String, String> aliases = new HashMap<>();
    for (int i = 0; i < collectionSchema.getMetrics().size(); i++) {
      aliases.put(collectionSchema.getMetrics().get(i), collectionSchema.getMetricAliases().get(i));
    }
    return aliases;
  }

  private Map<String, String> generateDimensionAliases() {
    Map<String, String> aliases = new HashMap<>();
    for (int i = 0; i < collectionSchema.getDimensions().size(); i++) {
      aliases.put(collectionSchema.getDimensions().get(i), collectionSchema.getDimensionAliases().get(i));
    }
    return aliases;
  }

  private Map<String, String> getDimensionValues(String dimensionKey) throws Exception {
    Map<String, String> valueMap = new TreeMap<>();
    List<String> dimensionNames = result.getDimensions();
    List<String> dimensionValues = objectMapper.readValue(dimensionKey.getBytes(), STRING_LIST_REF);

    for (int i = 0; i < dimensionNames.size(); i++) {
      valueMap.put(dimensionNames.get(i), dimensionValues.get(i));
    }

    return valueMap;
  }

  private List<MetricTable> generateMetricTables() throws Exception {
    List<MetricTable> tables = new ArrayList<>();

    for (Map.Entry<String, Map<String, Number[]>> entry : result.getData().entrySet()) {
      Map<Long, Number[]> baselineData = new HashMap<>();
      for (Map.Entry<String, Number[]> dataEntry : entry.getValue().entrySet()) {
        baselineData.put(Long.valueOf(dataEntry.getKey()), dataEntry.getValue());
      }
      // No way to determine difference between baseline / current yet, so use the same map for both.
      Map<Long, Number[]> currentData = baselineData;

      List<MetricDataRow> rows = ViewUtils.extractMetricDataRows(baselineData, currentData, currentMillis,
          baselineOffsetMillis, intraDayPeriod);
      List<MetricDataRow> cumulativeRows = new LinkedList<>();

      if (!rows.isEmpty()) {

        int metricCount = result.getMetrics().size();
        Number[] cumulativeBaselineValues = new Number[metricCount];
        Arrays.fill(cumulativeBaselineValues, 0.0);
        Number[] cumulativeCurrentValues = new Number[metricCount];
        Arrays.fill(cumulativeCurrentValues, 0.0);

        for (MetricDataRow row : rows) {
          Number[] baselineValues = row.getBaseline();
          for (int i = 0; i < baselineValues.length; i++) {
            cumulativeBaselineValues[i] = cumulativeBaselineValues[i].doubleValue()
                + (baselineValues[i] == null ? 0.0 : baselineValues[i].doubleValue());
          }

          Number[] currentValues = row.getCurrent();
          for (int i = 0; i < currentValues.length; i++) {
            cumulativeCurrentValues[i] = cumulativeCurrentValues[i].doubleValue()
                + (currentValues[i] == null ? 0.0 : currentValues[i].doubleValue());
          }

          Number[] cumulativeBaselineValuesCopy =
              Arrays.copyOf(cumulativeBaselineValues, cumulativeBaselineValues.length);
          Number[] cumulativeCurrentValuesCopy = Arrays.copyOf(cumulativeCurrentValues, cumulativeCurrentValues.length);

          MetricDataRow cumulativeRow = new MetricDataRow(row.getBaselineTime(), cumulativeBaselineValuesCopy,
              row.getCurrentTime(), cumulativeCurrentValuesCopy);
          cumulativeRows.add(cumulativeRow);
        }
      }
      tables.add(new MetricTable(getDimensionValues(entry.getKey()), rows, cumulativeRows));
    }

    return tables;
  }
}
