package org.apache.pinot.thirdeye.cube.ratio;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.cube.data.cube.CubeUtils;
import org.apache.pinot.thirdeye.cube.data.node.BaseCubeNode;


public class RatioCubeNode extends BaseCubeNode<RatioCubeNode, RatioRow> {
  private double baselineNumeratorValue;
  private double currentNumeratorValue;
  private double baselineDenominatorValue;
  private double currentDenominatorValue;

  NodeStatus status;

  public RatioCubeNode(RatioRow data) {
    super(data);
    resetValues();
  }

  public RatioCubeNode(int level, int index, RatioRow data, RatioCubeNode parent) {
    super(level, index, data, parent);
    resetValues();
  }

  public void resetValues() {
    this.baselineNumeratorValue = data.getBaselineValue();
    this.currentNumeratorValue = data.getCurrentValue();
    this.baselineDenominatorValue = data.getBaselineDenominatorValue();
    this.currentDenominatorValue = data.getCurrentDenominatorValue();
  }

  public void removeNodeValues(RatioCubeNode node) {
    baselineNumeratorValue = CubeUtils.doubleMinus(baselineNumeratorValue, node.baselineNumeratorValue);
    currentNumeratorValue = CubeUtils.doubleMinus(currentNumeratorValue, node.currentNumeratorValue);
    baselineDenominatorValue = CubeUtils.doubleMinus(baselineDenominatorValue, node.baselineDenominatorValue);
    currentDenominatorValue = CubeUtils.doubleMinus(currentDenominatorValue, node.currentDenominatorValue);
    Preconditions.checkArgument(
        !(this.baselineNumeratorValue < 0 || this.currentNumeratorValue < 0 || baselineDenominatorValue < 0
            || currentDenominatorValue < 0));
  }

  public void addNodeValues(RatioCubeNode node) {
    this.baselineNumeratorValue += node.baselineNumeratorValue;
    this.currentNumeratorValue += node.currentNumeratorValue;
    this.baselineDenominatorValue += node.baselineDenominatorValue;
    this.currentDenominatorValue += node.currentDenominatorValue;
  }

  public double getBaselineSize() {
    return baselineNumeratorValue + baselineDenominatorValue;
  }

  public double getCurrentSize() {
    return currentNumeratorValue + currentDenominatorValue;
  }

  public double getOriginalBaselineSize() {
    return data.getBaselineValue() + data.getBaselineDenominatorValue();
  }

  public double getOriginalCurrentSize() {
    return data.getCurrentValue() + data.getCurrentDenominatorValue();
  }

  public double getBaselineValue() {
    if (baselineNumeratorValue == 0 && baselineDenominatorValue == 0) {
      return 0.0;
    } else if (baselineDenominatorValue <= 0d) {
      return baselineNumeratorValue;
    } else {
      return baselineNumeratorValue / baselineDenominatorValue;
    }
  }

  public double getCurrentValue() {
    if (currentNumeratorValue == 0 && currentDenominatorValue == 0) {
      return 0.0;
    } else if (currentDenominatorValue <= 0d) {
      return currentNumeratorValue;
    } else {
      return currentNumeratorValue / currentDenominatorValue;
    }
  }

  public double getOriginalBaselineValue() {
    return data.getBaselineValue() / data.getBaselineDenominatorValue();
  }

  public double getOriginalCurrentValue() {
    return data.getCurrentValue() / data.getCurrentDenominatorValue();
  }

  public double originalChangeRatio() {
    return (data.getCurrentValue() / data.getCurrentDenominatorValue()) / (data.getBaselineValue() / data.getBaselineDenominatorValue());
  }

  public double changeRatio() {
    return (currentNumeratorValue / currentDenominatorValue) / (baselineNumeratorValue / baselineDenominatorValue);
  }

  public double getBaselineNumeratorValue() {
    return baselineNumeratorValue;
  }

  public double getBaselineDenominatorValue() {
    return baselineDenominatorValue;
  }

  public double getCurrentNumeratorValue() {
    return currentNumeratorValue;
  }

  public double getCurrentDenominatorValue() {
    return currentDenominatorValue;
  }

  public void computeStatus() {
    NodeStatus status = new NodeStatus();
    status.baselineRatio = this.getBaselineValue();
    status.currentRatio = this.getCurrentValue();
    status.baselineSize = this.getBaselineSize();
    status.currentSize = this.getCurrentSize();
  }

  public static class NodeStatus {
    double baselineRatio;
    double currentRatio;
    double baselineSize;
    double currentSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RatioCubeNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RatioCubeNode that = (RatioCubeNode) o;
    return Double.compare(that.baselineDenominatorValue, baselineDenominatorValue) == 0
        && Double.compare(that.currentDenominatorValue, currentDenominatorValue) == 0 && Objects.equal(data,
        that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), baselineDenominatorValue, currentDenominatorValue, data);
  }
}
