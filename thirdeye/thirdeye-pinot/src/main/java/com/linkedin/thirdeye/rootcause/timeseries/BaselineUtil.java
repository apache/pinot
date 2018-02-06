package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;


/**
 * Utility class for synthetic baseline computation
 */
public class BaselineUtil {
  private BaselineUtil() {
    // left blank
  }

  /**
   * Returns a series of timestamps computed for a given metric slice, in slice's {@code granularity}
   * intervals starting from the slice's {@code start}.
   *
   * @param slice metric slice
   * @return LongSeries of timestamps
   */
  public static LongSeries makeTimestamps(MetricSlice slice) {
    if (slice.getGranularity().toMillis() <= 0) {
      return LongSeries.buildFrom(slice.getStart());
    }

    final long start = slice.getStart();
    final long interval = slice.getGranularity().toMillis();
    final int count = (int) ((slice.getEnd() - slice.getStart()) / interval);

    return LongSeries.sequence(start, count, interval);
  }
}
