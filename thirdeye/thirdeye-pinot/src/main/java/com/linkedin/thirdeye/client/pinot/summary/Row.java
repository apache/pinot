package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


public class Row {
  Dimensions dimensions;
  DimensionValues dimensionValues;
  int level;
  double baselineValue;
  double currentValue;
  public List<Double> targetRatios = new ArrayList<>(); // for development purpose

  public Dimensions getDimensions() {
    return dimensions;
  }

  public void setDimensions(Dimensions dimensions) {
    this.dimensions = dimensions;
  }

  public DimensionValues getDimensionValues() {
    return dimensionValues;
  }

  public void setDimensionValues(DimensionValues dimensionValues) {
    this.dimensionValues = dimensionValues;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public double getBaselineValue() {
    return baselineValue;
  }

  public void setBaselineValue(double baselineValue) {
    this.baselineValue = baselineValue;
  }

  public double getCurrentValue() {
    return currentValue;
  }

  public void setCurrentValue(double currentValue) {
    this.currentValue = currentValue;
  }

  boolean hasSameParentFast(Row o) {
    return dimensionValues.get(level-2).equals(o.dimensionValues.get(level-2));
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
