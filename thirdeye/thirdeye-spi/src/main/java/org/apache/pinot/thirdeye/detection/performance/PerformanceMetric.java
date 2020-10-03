package org.apache.pinot.thirdeye.detection.performance;

import com.fasterxml.jackson.annotation.JsonProperty;


public class PerformanceMetric {

  @JsonProperty
  private double value;

  @JsonProperty
  private PerformanceMetricType type;

  public double getValue() { return value; }

  public void setValue(double value) {
    this.value = value;
  }

  public PerformanceMetricType getType() { return type; }

  public void setType(PerformanceMetricType type) {
    this.type = type;
  }
}
