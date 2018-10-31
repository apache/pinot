package com.linkedin.thirdeye.datasource.pinot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.client.TextTable;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.datasource.BaseThirdEyeResponse;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponseRow;

public class PinotThirdEyeResponse extends BaseThirdEyeResponse {
  private final Map<MetricFunction, Integer> metricFuncToIdMapping;
  private List<ThirdEyeResponseRow> responseRows;
  private List<String[]> rows;

  public PinotThirdEyeResponse(ThirdEyeRequest request, List<String[]> rows, TimeSpec dataTimeSpec) {
    super(request, dataTimeSpec);
    this.rows = rows;
    this.responseRows = new ArrayList<>(rows.size());
    this.metricFuncToIdMapping = new HashMap<>();
    for (int i = 0; i < metricFunctions.size(); i++) {
      metricFuncToIdMapping.put(metricFunctions.get(i), i + groupKeyColumns.size());
    }
    for (String[] row : rows) {
      int timeBucketId = -1;
      List<String> dimensions = new ArrayList<>();
      if (!groupKeyColumns.isEmpty()) {
        for (int i = 0; i < groupKeyColumns.size(); i++) {
          if (request.getGroupByTimeGranularity() != null && i == 0) {
            timeBucketId = Integer.parseInt(row[i]);
          } else {
            dimensions.add(row[i]);
          }
        }
      }
      List<Double> metrics = new ArrayList<>();
      for (int i = 0; i < metricFunctions.size(); i++) {
        metrics.add(Double.parseDouble(row[groupKeyColumns.size() + i]));
      }
      ThirdEyeResponseRow responseRow = new ThirdEyeResponseRow(timeBucketId, dimensions, metrics);
      responseRows.add(responseRow);
    }
  }

  @Override
  public int getNumRowsFor(MetricFunction metricFunction) {
    return rows.size();
  }

  @Override
  public Map<String, String> getRow(MetricFunction metricFunction, int rowId) {
    Map<String, String> rowMap = new HashMap<>();
    String[] rowValues = rows.get(rowId);
    for (int i = 0; i < groupKeyColumns.size(); i++) {
      String groupByKey = groupKeyColumns.get(i);
      rowMap.put(groupByKey, rowValues[i]);
    }
    rowMap.put(metricFunction.toString(), rowValues[metricFuncToIdMapping.get(metricFunction)]);
    return rowMap;
  }

  @Override
  public String toString() {
    TextTable textTable = new TextTable();
    textTable.addHeader(allColumnNames);
    for (String[] row : rows) {
      textTable.addRow(row);
    }
    return textTable.toString();
  }

  @Override
  public int getNumRows() {
    return rows.size();
  }

  @Override
  public ThirdEyeResponseRow getRow(int rowId) {
    return responseRows.get(rowId);
  }
}
