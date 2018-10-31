package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Baseline that always returns an empty set of data
 */
public class BaselineNone implements Baseline {
  @Override
  public List<MetricSlice> scatter(MetricSlice slice) {
    return Collections.emptyList();
  }

  @Override
  public DataFrame gather(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    return new DataFrame(COL_TIME, LongSeries.empty())
        .addSeries(COL_VALUE, DoubleSeries.empty());
  }
}
