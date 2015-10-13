package com.linkedin.thirdeye.dashboard.api;

public class FunnelHeatMapRow {
  private final long hour;
  private final Number[] baseline;
  private final Number[] current;
  private final Number[] ratio;

  public FunnelHeatMapRow(long hour, Number[] baseline, Number[] current) {
    this.hour = hour;
    this.baseline = baseline;
    this.current = current;

    if (baseline == null || current == null) {
      this.ratio = null;
    } else {
      this.ratio = new Number[baseline.length];
      for (int i = 0; i < baseline.length; i++) {
        if (baseline[i] == null || baseline[i].doubleValue() == 0.0) {
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

  public long getHour() {
    return hour;
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
