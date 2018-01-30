package com.linkedin.thirdeye.alert.content;

import org.joda.time.DateTime;


public class EmailContentFormatterContext {
  private DateTime start; // anomaly search region starts
  private DateTime end; // anomaly search region ends

  public DateTime getStart() {
    return start;
  }

  public void setStart(DateTime start) {
    this.start = start;
  }

  public DateTime getEnd() {
    return end;
  }

  public void setEnd(DateTime end) {
    this.end = end;
  }
}
