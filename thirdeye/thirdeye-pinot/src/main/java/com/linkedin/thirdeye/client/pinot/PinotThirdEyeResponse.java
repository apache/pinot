package com.linkedin.thirdeye.client.pinot;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.BaseThirdEyeResponse;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

public class PinotThirdEyeResponse extends BaseThirdEyeResponse {
  private final ResultSetGroup resultSetGroup;
  private final Map<MetricFunction, Integer> metricFuncToIdMapping;

  public PinotThirdEyeResponse(ThirdEyeRequest request, ResultSetGroup resultSetGroup,
      TimeSpec dataTimeSpec) {
    super(request, dataTimeSpec);
    this.resultSetGroup = resultSetGroup;
    this.metricFuncToIdMapping = new HashMap<>();
    for (int i = 0; i < metricFunctions.size(); i++) {
      metricFuncToIdMapping.put(metricFunctions.get(i), i);
    }
  }

  @Override
  public int getNumRowsFor(MetricFunction metricFunction) {
    int metricFunctionIndex = metricFuncToIdMapping.get(metricFunction);
    return resultSetGroup.getResultSet(metricFunctionIndex).getRowCount();
  }

  @Override
  public Map<String, String> getRow(MetricFunction metricFunction, int rowId) {
    int metricFunctionIndex = metricFuncToIdMapping.get(metricFunction);
    ResultSet resultSet = resultSetGroup.getResultSet(metricFunctionIndex);
    Map<String, String> ret = new HashMap<>();
    for (int groupColId = 0; groupColId < resultSet.getGroupKeyLength(); groupColId++) {
      String groupKeyVal = resultSet.getGroupKeyString(rowId, groupColId);
      ret.put(groupKeyColumns.get(groupColId), groupKeyVal);
    }
    for (int colId = 0; colId < resultSet.getColumnCount(); colId++) {
      // TODO need to standardize the key for results (currently relies on pinot convention)
      String colVal = resultSet.getString(rowId, colId);
      ret.put(resultSet.getColumnName(colId), colVal);
    }
    return ret;
  }
}
