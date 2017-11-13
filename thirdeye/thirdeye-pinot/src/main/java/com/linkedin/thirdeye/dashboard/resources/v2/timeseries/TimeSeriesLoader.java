package com.linkedin.thirdeye.dashboard.resources.v2.timeseries;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;


/**
 * Loader for metric series based on slice and an (optinal) granularity. Must be thread safe.
 */
public interface TimeSeriesLoader {
  /**
   * Returns the metric time series for a given time range and filter set, with a specified
   * time granularity. If the underlying time series resolution does not correspond to the desired
   * time granularity, it is up-sampled (via forward fill) or down-sampled (via sum if additive, or
   * last value otherwise) transparently.
   *
   * <br/><b>NOTE:</b> if the start timestamp does not align with the time
   * resolution, it is aligned with the nearest lower time stamp.
   *
   * @param slice metric slice to fetch
   * @return dataframe with aligned timestamps and values
   */
  DataFrame load(MetricSlice slice) throws Exception;
}
