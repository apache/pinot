package com.linkedin.thirdeye.hadoop.topk;

/**
 * Class to manage dimension value and metric values pairs
 */
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
    return other.metricValue.intValue() - this.metricValue.intValue();
  }

  @Override
  public String toString() {
    return "DimensionValueMetricPair [dimensionValue=" + dimensionValue + ", metricValue=" + metricValue + "]";
  }



}
