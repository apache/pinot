package com.linkedin.thirdeye.reporting.api;

public class TimeRange
{

  private Long start;
  private Long end;

  public TimeRange() {}

  public TimeRange(Long start, Long end)
  {
    this.start = start;
    this.end = end;

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
    return "[" + ReportConstants.DATE_TIME_FORMATTER.print(start) + " - " + ReportConstants.DATE_TIME_FORMATTER.print(end) + "]";
  }
}
