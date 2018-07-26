package com.linkedin.thirdeye.dataframe.util;

import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.Period;


public class TimeSeriesRequestContainer extends RequestContainer {
  final DateTime start;
  final DateTime end;
  final Period interval;

  public TimeSeriesRequestContainer(ThirdEyeRequest request, List<MetricExpression> expressions, DateTime start,
      DateTime end, Period interval) {
    super(request, expressions);
    this.start = start;
    this.end = end;
    this.interval = interval;
  }

  public DateTime getStart() {
    return start;
  }

  public DateTime getEnd() {
    return end;
  }

  public Period getInterval() {
    return interval;
  }
}
