package com.linkedin.thirdeye.datasource.loader;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.List;


public interface AggregationLoader {
  String COL_DIMENSION_NAME = "dimName";
  String COL_DIMENSION_VALUE = "dimValue";
  String COL_VALUE = DataFrameUtils.COL_VALUE;

  DataFrame loadBreakdown(MetricSlice slice) throws Exception;

  DataFrame loadAggregate(MetricSlice slice, List<String> dimensions) throws Exception;
}
