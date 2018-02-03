package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.List;
import java.util.Map;


public interface Baseline {
  String COL_TIME = DataFrameUtils.COL_TIME;
  String COL_VALUE = DataFrameUtils.COL_VALUE;

  List<MetricSlice> from(MetricSlice slice);
  Map<MetricSlice, DataFrame> filter(MetricSlice slice, Map<MetricSlice, DataFrame> data);
  DataFrame compute(MetricSlice slice, Map<MetricSlice, DataFrame> data);
}
