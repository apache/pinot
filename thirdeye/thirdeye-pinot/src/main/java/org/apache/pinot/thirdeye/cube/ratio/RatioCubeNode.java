/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.cube.ratio;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;
import org.apache.pinot.thirdeye.cube.data.cube.CubeUtils;
import org.apache.pinot.thirdeye.cube.data.node.BaseCubeNode;


/**
 * A CubeNode for ratio metrics such as "observed over expected ratio".
 */
public class RatioCubeNode extends BaseCubeNode<RatioCubeNode, RatioRow> {
  private static double epsilon = 0.0001;

  private double baselineNumeratorValue;
  private double currentNumeratorValue;
  private double baselineDenominatorValue;
  private double currentDenominatorValue;

  NodeStatus status;

  /**
   * Constructs a root CubeNode whose level and index is 0 and parent pointer is null.
   *
   * @param data the data of this root node.
   */
  public RatioCubeNode(RatioRow data) {
    super(data);
    resetValues();
  }

  /**
   * Constructs a CubeNode which is specified information.
   *
   * @param level the level of this node.
   * @param index the index of this node that is located in its parent's children list.
   * @param data the data of this node.
   * @param parent the parent of this node.
   */
  public RatioCubeNode(int level, int index, RatioRow data, RatioCubeNode parent) {
    super(level, index, data, parent);
    resetValues();
  }

  @Override
  public void resetValues() {
    this.baselineNumeratorValue = data.getBaselineNumeratorValue();
    this.currentNumeratorValue = data.getCurrentNumeratorValue();
    this.baselineDenominatorValue = data.getBaselineDenominatorValue();
    this.currentDenominatorValue = data.getCurrentDenominatorValue();
  }

  @Override
  public void removeNodeValues(RatioCubeNode node) {
    baselineNumeratorValue = CubeUtils.doubleMinus(baselineNumeratorValue, node.baselineNumeratorValue);
    currentNumeratorValue = CubeUtils.doubleMinus(currentNumeratorValue, node.currentNumeratorValue);
    baselineDenominatorValue = CubeUtils.doubleMinus(baselineDenominatorValue, node.baselineDenominatorValue);
    currentDenominatorValue = CubeUtils.doubleMinus(currentDenominatorValue, node.currentDenominatorValue);
    Preconditions.checkArgument(!(DoubleMath.fuzzyCompare(baselineNumeratorValue, 0, epsilon) < 0
        || DoubleMath.fuzzyCompare(currentNumeratorValue, 0, epsilon) < 0
        || DoubleMath.fuzzyCompare(baselineDenominatorValue, 0, epsilon) < 0
        || DoubleMath.fuzzyCompare(currentDenominatorValue, 0, epsilon) < 0));
  }

  @Override
  public void addNodeValues(RatioCubeNode node) {
    this.baselineNumeratorValue += node.baselineNumeratorValue;
    this.currentNumeratorValue += node.currentNumeratorValue;
    this.baselineDenominatorValue += node.baselineDenominatorValue;
    this.currentDenominatorValue += node.currentDenominatorValue;
  }

  @Override
  public double getBaselineSize() {
    return baselineNumeratorValue + baselineDenominatorValue;
  }

  @Override
  public double getCurrentSize() {
    return currentNumeratorValue + currentDenominatorValue;
  }

  @Override
  public double getOriginalBaselineSize() {
    return data.getBaselineNumeratorValue() + data.getBaselineDenominatorValue();
  }

  @Override
  public double getOriginalCurrentSize() {
    return data.getCurrentNumeratorValue() + data.getCurrentDenominatorValue();
  }

  @Override
  public double getBaselineValue() {
    if (!DoubleMath.fuzzyEquals(baselineDenominatorValue, 0, epsilon)) {
      return baselineNumeratorValue / baselineDenominatorValue;
    } else if (DoubleMath.fuzzyEquals(baselineNumeratorValue, 0, epsilon)) {
      return 0d;
    } else {
      // divide the numerator value by numerator sum to prevent large change diff.
      return baselineNumeratorValue / (currentNumeratorValue + baselineNumeratorValue);
    }
  }

  @Override
  public double getCurrentValue() {
    if (!DoubleMath.fuzzyEquals(currentDenominatorValue, 0, epsilon)) {
      return currentNumeratorValue / currentDenominatorValue;
    } else if (DoubleMath.fuzzyEquals(currentNumeratorValue, 0, epsilon)) {
      return 0d;
    } else {
      // divide the numerator value by numerator sum to prevent large change diff.
      return currentNumeratorValue / (currentNumeratorValue + baselineNumeratorValue);
    }
  }

  @Override
  public double getOriginalBaselineValue() {
    return data.getBaselineNumeratorValue() / data.getBaselineDenominatorValue();
  }

  @Override
  public double getOriginalCurrentValue() {
    return data.getCurrentNumeratorValue() / data.getCurrentDenominatorValue();
  }

  @Override
  public double originalChangeRatio() {
    return (data.getCurrentNumeratorValue() / data.getCurrentDenominatorValue()) / (data.getBaselineNumeratorValue() / data.getBaselineDenominatorValue());
  }

  @Override
  public double changeRatio() {
    return (currentNumeratorValue / currentDenominatorValue) / (baselineNumeratorValue / baselineDenominatorValue);
  }

  /**
   * Returns the baseline numerator value.
   *
   * @return the baseline numerator value.
   */
  public double getBaselineNumeratorValue() {
    return baselineNumeratorValue;
  }

  /**
   * Returns the baseline denominator value.
   *
   * @return the baseline denominator value.
   */
  public double getBaselineDenominatorValue() {
    return baselineDenominatorValue;
  }

  /**
   * Returns the current numerator value.
   *
   * @return the current numerator value.
   */
  public double getCurrentNumeratorValue() {
    return currentNumeratorValue;
  }

  /**
   * Returns the current denominator value.
   *
   * @return the current denominator value.
   */
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
    return Double.compare(that.baselineNumeratorValue, baselineNumeratorValue) == 0
        && Double.compare(that.currentNumeratorValue, currentNumeratorValue) == 0
        && Double.compare(that.baselineDenominatorValue, baselineDenominatorValue) == 0
        && Double.compare(that.currentDenominatorValue, currentDenominatorValue) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), baselineNumeratorValue, currentNumeratorValue, baselineDenominatorValue,
        currentDenominatorValue);
  }

  /**
   * ToString that handles if the given cube node is null, i.e., a root cube node. Moreover, it does not invoke
   * parent's toString() to prevent multiple calls of toString to their parents.
   *
   * @param node the node to be converted to string.
   *
   * @return a simple string representation of a parent cube node, which does not toString its parent node recursively.
   */
  private String toStringAsParent(RatioCubeNode node) {
    if (node == null) {
      return "null";
    } else {
      return MoreObjects.toStringHelper(this)
          .add("level", level)
          .add("index", index)
          .add("baselineNumeratorValue", baselineNumeratorValue)
          .add("baselineDenominatorValue", baselineDenominatorValue)
          .add("currentNumeratorValue", currentNumeratorValue)
          .add("currentDenominatorValue", currentDenominatorValue)
          .add("cost", cost)
          .add("data", data)
          .toString();
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("level", level)
        .add("index", index)
        .add("baselineNumeratorValue", baselineNumeratorValue)
        .add("baselineDenominatorValue", baselineDenominatorValue)
        .add("currentNumeratorValue", currentNumeratorValue)
        .add("currentDenominatorValue", currentDenominatorValue)
        .add("cost", cost)
        .add("data", data)
        .add("parent", toStringAsParent(parent))
        .toString();
  }
}
