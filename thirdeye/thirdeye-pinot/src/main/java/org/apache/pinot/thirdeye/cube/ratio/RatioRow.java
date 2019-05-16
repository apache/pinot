package org.apache.pinot.thirdeye.cube.ratio;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.node.CubeNode;
import org.apache.pinot.thirdeye.cube.data.dbrow.BaseRow;


public class RatioRow extends BaseRow {
  protected double baselineValue;
  protected double currentValue;
  protected double baselineDenominatorValue;
  protected double currentDenominatorValue;

  public RatioRow(Dimensions dimensions, DimensionValues dimensionValues) {
    this.dimensions = Preconditions.checkNotNull(dimensions);
    this.dimensionValues = Preconditions.checkNotNull(dimensionValues);
    this.baselineValue = 0.0;
    this.currentValue = 0.0;
    this.baselineDenominatorValue = 0.0;
    this.currentDenominatorValue = 0.0;
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

  public double getBaselineDenominatorValue() {
    return baselineDenominatorValue;
  }

  public void setBaselineDenominatorValue(double denominatorBaselineValue) {
    this.baselineDenominatorValue = denominatorBaselineValue;
  }

  public double getCurrentDenominatorValue() {
    return currentDenominatorValue;
  }

  public void setCurrentDenominatorValue(double denominatorCurrentValue) {
    this.currentDenominatorValue = denominatorCurrentValue;
  }

  @Override
  public RatioCubeNode toNode() {
    return new RatioCubeNode(this);
  }

  @Override
  public CubeNode toNode(int level, int index, CubeNode parent) {
    return new RatioCubeNode(level, index, this, (RatioCubeNode) parent);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RatioRow)) {
      return false;
    }
    RatioRow ratioRow = (RatioRow) o;
    return Double.compare(ratioRow.baselineValue, baselineValue) == 0
        && Double.compare(ratioRow.currentValue, currentValue) == 0
        && Double.compare(ratioRow.baselineDenominatorValue, baselineDenominatorValue) == 0
        && Double.compare(ratioRow.currentDenominatorValue, currentDenominatorValue) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(baselineValue, currentValue, baselineDenominatorValue, currentDenominatorValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("baselineNumerator", baselineValue)
        .add("baselineDenominator", baselineDenominatorValue)
        .add("currentNumerator", currentValue)
        .add("currentDenominator", currentDenominatorValue)
        .add("changeRatio", currentValue / baselineValue)
        .add("dimensions", super.getDimensions())
        .add("dimensionValues", super.getDimensionValues())
        .toString();
  }
}
