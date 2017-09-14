package com.linkedin.thirdeye.dashboard.resources.v2.timeseries;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;


public interface TimeSeriesLoader {
  DataFrame load(MetricSlice slice, TimeGranularity granularity) throws Exception;
}
