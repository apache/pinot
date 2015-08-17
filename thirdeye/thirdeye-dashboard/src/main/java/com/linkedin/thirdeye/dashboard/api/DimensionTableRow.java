package com.linkedin.thirdeye.dashboard.api;

public class DimensionTableRow {
  private final String value;
  private final Number[] baseline;
  private final Number[] current;
  private final Number[] ratio;

  public DimensionTableRow(String value, Number[] baseline, Number[] current) {
    this.value = value;
    this.baseline = baseline;
    this.current = current;
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

  public int getNumColumns() {
    return current.length;
  }

  public String getValue() {
    return value;
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
