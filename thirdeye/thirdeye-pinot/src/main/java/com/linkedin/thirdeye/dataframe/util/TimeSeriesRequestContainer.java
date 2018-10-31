package com.linkedin.thirdeye.dataframe.util;

import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import java.util.List;


public class TimeSeriesRequestContainer extends RequestContainer {
  final long start;
  final long end;
  final long interval;

  public TimeSeriesRequestContainer(ThirdEyeRequest request, List<MetricExpression> expressions, long start, long end, long interval) {
    super(request, expressions);
    this.start = start;
    this.end = end;
    this.interval = interval;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public long getInterval() {
    return interval;
  }
}
