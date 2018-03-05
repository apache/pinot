package com.linkedin.thirdeye.dataframe.util;

import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import java.util.List;


public class TimeSeriesRequestContainer extends RequestContainer {
  final long start;
  final long end;
  final long interval;
  final long offset;

  public TimeSeriesRequestContainer(ThirdEyeRequest request, List<MetricExpression> expressions, long start, long end, long interval, long offset) {
    super(request, expressions);
    this.start = start;
    this.end = end;
    this.interval = interval;
    this.offset = offset;
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

  public long getOffset() {
    return offset;
  }
}
