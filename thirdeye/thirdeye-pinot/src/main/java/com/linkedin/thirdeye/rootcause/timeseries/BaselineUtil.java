package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;


public class BaselineUtil {
  private BaselineUtil() {
    // left blank
  }

  public static LongSeries makeTimestamps(MetricSlice slice) {
    // NOTE: requires aligned slice!

    final long start = slice.getStart();
    final long interval = slice.getGranularity().toMillis();
    final int count = (int) ((slice.getEnd() - slice.getStart()) / interval);

    return LongSeries.sequence(start, count, interval);
  }
}
