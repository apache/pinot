package com.linkedin.thirdeye.hadoop.topk;

import java.util.Arrays;

public class DimensionValueMetricPair implements Comparable<DimensionValueMetricPair>{


  private String dimensionValue;
  private Number metricValue;


  public DimensionValueMetricPair(String dimensionValue, Number metricValue) {
    this.dimensionValue = dimensionValue;
    this.metricValue = metricValue;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }
  public void setDimensionValue(String dimensionValue) {
    this.dimensionValue = dimensionValue;
  }
  public Number getMetricValue() {
    return metricValue;
  }
  public void setMetricValue(Number metricValue) {
    this.metricValue = metricValue;
  }


  @Override
  public int compareTo(DimensionValueMetricPair other) {
    return this.metricValue.intValue() - other.metricValue.intValue();
  }

  @Override
  public String toString() {
    return "DimensionValueMetricPair [dimensionValue=" + dimensionValue + ", metricValue=" + metricValue + "]";
  }



}
