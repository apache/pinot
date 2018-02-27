package com.linkedin.thirdeye.dashboard.resources.v2.aggregation;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;


public interface AggregationLoader {
  String COL_DIMENSION_NAME = "dimName";
  String COL_DIMENSION_VALUE = "dimValue";
  String COL_VALUE = DataFrameUtils.COL_VALUE;

  DataFrame loadBreakdown(MetricSlice slice) throws Exception;

  double loadAggregate(MetricSlice slice) throws Exception;
}
