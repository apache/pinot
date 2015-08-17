package com.linkedin.thirdeye.dashboard.views;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.DimensionTable;
import com.linkedin.thirdeye.dashboard.api.DimensionTableRow;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import io.dropwizard.views.View;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DimensionViewTabular extends View {
  private static final TypeReference<List<String>> STRING_LIST_REF = new TypeReference<List<String>>(){};
  private final ObjectMapper objectMapper;
  private final CollectionSchema schema;
  private final Map<String, QueryResult> results;
  private final Map<String, Map<String, String>> dimensionGroupMap;
  private final Map<String, Map<Pattern, String>> dimensionRegexMap;
  private final List<DimensionTable> dimensionTables;

  public DimensionViewTabular(CollectionSchema schema,
                              ObjectMapper objectMapper,
                              Map<String, QueryResult> results,
                              Map<String, Map<String, String>> dimensionGroupMap,
                              Map<String, Map<Pattern, String>> dimensionRegexMap) throws Exception {
    super("dimension/tabular.ftl");
    this.objectMapper = objectMapper;
    this.schema = schema;
    this.results = results;
    this.dimensionGroupMap = dimensionGroupMap;
    this.dimensionRegexMap = dimensionRegexMap;
    this.dimensionTables = generateDimensionTables();
  }

  public List<DimensionTable> getDimensionTables() {
    return dimensionTables;
  }

  private List<DimensionTable> generateDimensionTables() throws Exception {
    Map<String, String> metricAliases = new HashMap<>();
    for (int i = 0; i < schema.getMetrics().size(); i++) {
      metricAliases.put(schema.getMetrics().get(i), schema.getMetricAliases().get(i));
    }

    Map<String, String> dimensionAliases = new HashMap<>();
    for (int i = 0; i < schema.getDimensions().size(); i++) {
      dimensionAliases.put(schema.getDimensions().get(i), schema.getDimensionAliases().get(i));
    }

    List<DimensionTable> tables = new ArrayList<>();
    for (Map.Entry<String, QueryResult> entry : results.entrySet()) {
      String dimensionName = entry.getKey();
      QueryResult result = entry.getValue();
      List<String> dimensionNames = result.getDimensions();
      int dimensionIdx = dimensionNames.indexOf(dimensionName);

      // Process dimension groups
      Map<List<String>, Map<String, Number[]>> processedResult = ViewUtils.processDimensionGroups(
          result, objectMapper, dimensionGroupMap, dimensionRegexMap, dimensionName);

      List<DimensionTableRow> rows = new ArrayList<>();
      for (Map.Entry<List<String>, Map<String, Number[]>> combination : processedResult.entrySet()) {
        // Find min / max times (for current / baseline)
        long minTime = -1;
        long maxTime = -1;
        for (String timeString : combination.getValue().keySet()) {
          long time = Long.valueOf(timeString);
          if (minTime == -1 || time < minTime) {
            minTime = time;
          }
          if (maxTime == -1 || time > maxTime) {
            maxTime = time;
          }
        }

        String value = combination.getKey().get(dimensionIdx);
        Number[] baseline = combination.getValue().get(String.valueOf(minTime));
        Number[] current = combination.getValue().get(String.valueOf(maxTime));
        if (current != null && baseline != null) {
          DimensionTableRow row = new DimensionTableRow(value, baseline, current);
          rows.add(row);
        }
      }

      DimensionTable dimensionTable = new DimensionTable(
          result.getMetrics(),
          metricAliases,
          dimensionName,
          dimensionAliases.get(dimensionName),
          rows);
      tables.add(dimensionTable);
    }

    return tables;
  }
}
