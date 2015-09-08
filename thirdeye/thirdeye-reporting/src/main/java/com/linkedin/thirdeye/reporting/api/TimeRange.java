package com.linkedin.thirdeye.reporting.api;

import org.joda.time.DateTimeZone;

public class TimeRange
{

  private Long start;
  private Long end;
  private String timezone;

  public TimeRange() {}

  public TimeRange(Long start, Long end, String timezone)
  {
    this.start = start;
    this.end = end;
    this.timezone = timezone;

    if (start > end)
    {
      throw new IllegalArgumentException("start must be less than or equal to end: start=" + start + ", end=" + end);
    }
  }

  public Long getStart()
  {
    return start;
  }

  public Long getEnd()
  {
    return end;
  }

  @Override
  public String toString()
  {
    return "[" + ReportConstants.DATE_TIME_FORMATTER.withZone(DateTimeZone.forID(timezone)).print(start) + " - " + ReportConstants.DATE_TIME_FORMATTER.withZone(DateTimeZone.forID(timezone)).print(end) + "]";
  }
}
