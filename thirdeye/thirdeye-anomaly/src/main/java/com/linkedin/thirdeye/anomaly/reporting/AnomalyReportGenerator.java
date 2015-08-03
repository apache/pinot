package com.linkedin.thirdeye.anomaly.reporting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.database.AnomalyTable;
import com.linkedin.thirdeye.anomaly.database.AnomalyTableRow;
import com.linkedin.thirdeye.api.DimensionKey;

/**
 * API for creating anomaly report tables
 */
public class AnomalyReportGenerator {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  AnomalyDatabaseConfig dbConfig;

  public AnomalyReportGenerator(AnomalyDatabaseConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  /**
   * @param collection
   * @param metric
   * @param startTime
   * @param endTime
   * @param topK
   * @return
   *  AnomalyReportTable with default settings and ranking
   * @throws IOException
   */
  public AnomalyReportTable getAnomalyTable(String collection, String metric, long startTime, long endTime, int topK)
      throws IOException {
    List<AnomalyTableRow> anomalyTableRows = getAnomalyRowsHelper(dbConfig, collection, metric, startTime, endTime);
    return getAnomalyTable(anomalyTableRows, topK);
  }

  /**
   * @param anomalyTableRows
   * @param topK
   * @return
   *  AnomalyReportTable based on list of AnomalyTableRows provided
   * @throws IOException
   */
  public AnomalyReportTable getAnomalyTable(List<AnomalyTableRow> anomalyTableRows, int topK) throws IOException {
    List<AnomalyReportTableRow> reportTableRows = new ArrayList<AnomalyReportTableRow>(topK);

    List<String> dimensionSchema = null;

    for (AnomalyTableRow row : anomalyTableRows) {
      ObjectReader reader = OBJECT_MAPPER.reader(Map.class);
      Map<String, String> dimensions = reader.readValue(row.getDimensions());
      if (dimensionSchema == null) {
        dimensionSchema = new ArrayList<>(dimensions.keySet());
        Collections.sort(dimensionSchema);
      }

      String[] dimensionValues = new String[dimensions.size()];
      for (Entry<String, String> e : dimensions.entrySet()) {
        dimensionValues[dimensionSchema.indexOf(e.getKey())] = e.getValue();
      }

      AnomalyReportTableRow reportTableRow = new AnomalyReportTableRow(
          row.getTimeWindow(),
          new DimensionKey(dimensionValues).toString(),
          row.getFunctionDescription(),
          row.getFunctionName().toLowerCase().contains("percent"),
          row.getAnomalyScore(),
          row.getAnomalyVolume());
      reportTableRows.add(reportTableRow);
      if (reportTableRows.size() >= topK) {
        break;
      }
    }

    int totalViolationCount = reportTableRows.size();

    int topLevelViolationCount = 0;
    for (AnomalyTableRow row : anomalyTableRows) {
      if (row.getNonStarCount() == 0) {
        topLevelViolationCount++;
      }
    }

    AnomalyReportTable result = new AnomalyReportTable();
    result.setReportRows(reportTableRows);
    result.setTopLevelViolationCount(topLevelViolationCount);
    result.setTotalViolationCount(totalViolationCount);

    if (dimensionSchema != null)
    {
      result.setDimensionSchema(new DimensionKey(dimensionSchema.toArray(new String[0])).toString());
    }

    return result;
  }

  /**
   * @param dbConfig
   * @param collection
   * @param metric
   * @param startTimeWindow
   * @param endTimeWindow
   * @return
   *  A list of anomaly table rows using default ranking and parameters.
   */
  private List<AnomalyTableRow> getAnomalyRowsHelper(AnomalyDatabaseConfig dbConfig, String collection, String metric,
      long startTimeWindow, long endTimeWindow) {
    Set<String> metrics = null;
    if (metric != null) {
      metrics = new HashSet<>();
      metrics.add(metric);
    }
    List<String> orderBy = Arrays.asList(new String[]{
        "non_star_count",
        "dimensions_contribution DESC",
        "time_window DESC",
        "ABS(anomaly_score) DESC"
    });

    return AnomalyTable.selectRows(dbConfig, collection, null, null, metrics, false, orderBy, startTimeWindow, endTimeWindow);
  }


  public static void main(String[] argv) throws IOException {
    AnomalyDatabaseConfig dbConfig = new AnomalyDatabaseConfig("localhost/thirdeye", "rule", "anomaly", "alert", "",
        false);
    AnomalyReportGenerator rg = new AnomalyReportGenerator(dbConfig);
    System.out.println(rg.getAnomalyTable("ads", "cost", 0, 2000000000000L, 20));
  }
}

