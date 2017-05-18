package com.linkedin.thirdeye.datasource;

import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.api.TimeSpec;

/**
 * The result of calling {@link ThirdEyeDataSource#execute(ThirdEyeRequest)}.
 */
public interface ThirdEyeResponse {

  List<MetricFunction> getMetricFunctions();

  int getNumRows();
  
  ThirdEyeResponseRow getRow(int rowId);
  
  int getNumRowsFor(MetricFunction metricFunction);

  // TODO make new API methods to make it clearer how to retrieve metric values vs dimension values,
  // etc. These are all stored in the same map right now.
  Map<String, String> getRow(MetricFunction metricFunction, int rowId);

  ThirdEyeRequest getRequest();

  TimeSpec getDataTimeSpec();

  List<String> getGroupKeyColumns();

}
