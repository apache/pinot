package com.linkedin.thirdeye.datasource;

import java.util.List;

import com.linkedin.thirdeye.api.TimeSpec;

/**
 * The result of calling {@link ThirdEyeDataSource#execute(ThirdEyeRequest)}.
 */
public interface ThirdEyeResponse {
  // Metadata Getters
  ThirdEyeRequest getRequest();

  TimeSpec getDataTimeSpec();

  List<String> getGroupKeyColumns();

  // TODO: Replace with getMetricNames (i.e., ids)
  List<MetricFunction> getMetricFunctions();

  int getNumRows();

  // Data Getters
  ThirdEyeResponseRow getRow(int rowId);

}
