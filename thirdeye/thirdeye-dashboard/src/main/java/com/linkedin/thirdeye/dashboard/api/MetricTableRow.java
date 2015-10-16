package com.linkedin.thirdeye.dashboard.api;

import org.joda.time.DateTime;


public class MetricTableRow {
  private final DateTime baselineTime;
  private final Number[] baseline;
  private final DateTime currentTime;
  private final Number[] current;
  private final Number[] ratio;

  public MetricTableRow(DateTime baselineTime, Number[] baseline, DateTime currentTime, Number[] current) {
    this.baselineTime = baselineTime;
    this.baseline = baseline;
    this.currentTime = currentTime;
    this.current = current;

    if (baseline == null || current == null) {
      this.ratio = null;
    } else {
      this.ratio = new Number[current.length];
      for (int i = 0; i < current.length; i++) {
        if (baseline[i] == null || baseline[i].doubleValue() == 0) {
          ratio[i] = null;
        } else if (current[i] == null) {
          ratio[i] = 0;
        } else {
          ratio[i] = (current[i].doubleValue() - baseline[i].doubleValue()) / baseline[i].doubleValue();
        }
      }
    }
  }

  public int getNumColumns() {
    return current.length;
  }

  public DateTime getBaselineTime() {
    return baselineTime;
  }

  public DateTime getCurrentTime() {
    return currentTime;
  }

  public Number[] getBaseline() {
    return baseline;
  }

  public Number[] getCurrent() {
    return current;
  }

  public Number[] getRatio() {
    return ratio;
  }
}
