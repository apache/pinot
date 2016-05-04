package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* TODO This is strictly designed for Pinot's ResultSet.
 * That being said, perhaps we could make a ThirdEyeResponse
 * interface and allow different client implementations to
 * provide their own response implementation.
 */
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.api.TimeSpec;

public class ThirdEyeResponse {

  private final List<MetricFunction> metricFunctions;
  private final ResultSetGroup resultSetGroup;
  private final Map<MetricFunction, Integer> metricFuncToIdMapping = new HashMap<>();
  private final ThirdEyeRequest request;
  private final TimeSpec dataTimeSpec;
  private final List<String> groupKeyColumns;

  public ThirdEyeResponse(ThirdEyeRequest request, ResultSetGroup resultSetGroup,
      TimeSpec dataTimeSpec) {
    this.request = request;
    this.resultSetGroup = resultSetGroup;
    this.dataTimeSpec = dataTimeSpec;
    metricFunctions = request.getMetricFunctions();
    for (int i = 0; i < metricFunctions.size(); i++) {
      metricFuncToIdMapping.put(metricFunctions.get(i), i);
    }
    groupKeyColumns = new ArrayList<>();
    if (request.getGroupByTimeGranularity() != null) {
      groupKeyColumns.add(dataTimeSpec.getColumnName());
    }
    groupKeyColumns.addAll(request.getGroupBy());
  }

  public List<MetricFunction> getMetricFunctions() {
    return metricFunctions;
  }

  public int getNumRowsFor(MetricFunction metricFunction) {
    int metricFunctionIndex = metricFuncToIdMapping.get(metricFunction);
    return resultSetGroup.getResultSet(metricFunctionIndex).getRowCount();
  }

  // TODO make new API methods to make it clearer how to retrieve metric values vs dimension values,
  // etc. These are all stored in the same map right now.
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

  public ThirdEyeRequest getRequest() {
    return request;
  }

  public TimeSpec getDataTimeSpec() {
    return dataTimeSpec;
  }

  public List<String> getGroupKeyColumns() {
    return groupKeyColumns;
  }

}
