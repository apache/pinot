package com.linkedin.thirdeye.datasource;

import java.util.List;

import com.linkedin.thirdeye.api.TimeSpec;

/**
 * The result of calling {@link ThirdEyeDataSource#execute(ThirdEyeRequest)}.
 */
public interface ThirdEyeResponse {

  List<MetricFunction> getMetricFunctions();

  int getNumRows();

  ThirdEyeResponseRow getRow(int rowId);

  int getNumRowsFor(MetricFunction metricFunction);

  ThirdEyeRequest getRequest();

  TimeSpec getDataTimeSpec();

  List<String> getGroupKeyColumns();

}
