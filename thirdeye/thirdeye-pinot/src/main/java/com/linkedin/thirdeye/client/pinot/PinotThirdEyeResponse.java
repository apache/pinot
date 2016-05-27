package com.linkedin.thirdeye.client.pinot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.client.TextTable;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.BaseThirdEyeResponse;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

public class PinotThirdEyeResponse extends BaseThirdEyeResponse {
  private final Map<MetricFunction, Integer> metricFuncToIdMapping;
  private List<String[]> rows;
  List<String> dimensions;

  public PinotThirdEyeResponse(ThirdEyeRequest request, List<String[]> rows,
      TimeSpec dataTimeSpec) {
    super(request, dataTimeSpec);
    this.rows = rows;
    this.metricFuncToIdMapping = new HashMap<>();
    for (int i = 0; i < metricFunctions.size(); i++) {
      metricFuncToIdMapping.put(metricFunctions.get(i), i + groupKeyColumns.size());
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
}
