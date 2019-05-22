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
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.node.CubeNode;
import org.apache.pinot.thirdeye.cube.data.dbrow.BaseRow;


/**
 * Stores the ratio metric that is returned from DB.
 */
public class RatioRow extends BaseRow {
  protected double baselineNumeratorValue;
  protected double currentNumeratorValue;
  protected double baselineDenominatorValue;
  protected double currentDenominatorValue;

  /**
   * Constructs an ratio row.
   *
   * @param dimensions the dimension names of this row.
   * @param dimensionValues the dimension values of this row.
   */
  public RatioRow(Dimensions dimensions, DimensionValues dimensionValues) {
    super(dimensions, dimensionValues);
    this.baselineNumeratorValue = 0.0;
    this.currentNumeratorValue = 0.0;
    this.baselineDenominatorValue = 0.0;
    this.currentDenominatorValue = 0.0;
  }

  /**
   * Constructs an ratio row.
   *
   * @param dimensions the dimension names of this row.
   * @param dimensionValues the dimension values of this row.
   * @param baselineNumeratorValue the baseline numerator of this ratio row.
   * @param baselineDenominatorValue the baseline denominator of this ratio row.
   * @param currentNumeratorValue the current numerator of this ratio row.
   * @param currentDenominatorValue the current denominator of this ratio row.
   */
  public RatioRow(Dimensions dimensions, DimensionValues dimensionValues, double baselineNumeratorValue,
      double baselineDenominatorValue, double currentNumeratorValue, double currentDenominatorValue) {
    super(dimensions, dimensionValues);
    this.baselineNumeratorValue = baselineNumeratorValue;
    this.baselineDenominatorValue = baselineDenominatorValue;
    this.currentNumeratorValue = currentNumeratorValue;
    this.currentDenominatorValue = currentDenominatorValue;
  }

  /**
   * Returns the baseline numerator of this ratio row.
   *
   * @return the baseline numerator of this ratio row.
   */
  public double getBaselineNumeratorValue() {
    return baselineNumeratorValue;
  }

  /**
   * Sets the baseline numerator value of this ratio row.
   *
   * @param baselineNumeratorValue the baseline numerator value of this ratio row.
   */
  public void setBaselineNumeratorValue(double baselineNumeratorValue) {
    this.baselineNumeratorValue = baselineNumeratorValue;
  }

  /**
   * Returns the current numerator of this ratio row.
   *
   * @return the current numerator of this ratio row.
   */
  public double getCurrentNumeratorValue() {
    return currentNumeratorValue;
  }

  /**
   * Sets the baseline numerator value of this ratio row.
   *
   * @param currentNumeratorValue the baseline numerator value of this ratio row.
   */
  public void setCurrentNumeratorValue(double currentNumeratorValue) {
    this.currentNumeratorValue = currentNumeratorValue;
  }

  /**
   * Returns the baseline denominator of this ratio row.
   *
   * @return the baseline denominator of this ratio row.
   */
  public double getBaselineDenominatorValue() {
    return baselineDenominatorValue;
  }

  /**
   * Sets the baseline denominator value of this ratio row.
   *
   * @param denominatorBaselineValue the baseline denominator value of this ratio row.
   */
  public void setBaselineDenominatorValue(double denominatorBaselineValue) {
    this.baselineDenominatorValue = denominatorBaselineValue;
  }

  /**
   * Returns the current denominator of this ratio row.
   *
   * @return the current denominator of this ratio row.
   */
  public double getCurrentDenominatorValue() {
    return currentDenominatorValue;
  }

  /**
   * Sets the current denominator value of this ratio row.
   *
   * @param denominatorCurrentValue the current denominator value of this ratio row.
   */
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
    if (!super.equals(o)) {
      return false;
    }
    RatioRow ratioRow = (RatioRow) o;
    return Double.compare(ratioRow.baselineNumeratorValue, baselineNumeratorValue) == 0
        && Double.compare(ratioRow.currentNumeratorValue, currentNumeratorValue) == 0
        && Double.compare(ratioRow.baselineDenominatorValue, baselineDenominatorValue) == 0
        && Double.compare(ratioRow.currentDenominatorValue, currentDenominatorValue) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), baselineNumeratorValue, currentNumeratorValue, baselineDenominatorValue,
        currentDenominatorValue);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("baselineNumerator", baselineNumeratorValue)
        .add("baselineDenominator", baselineDenominatorValue)
        .add("currentNumerator", currentNumeratorValue)
        .add("currentDenominator", currentDenominatorValue)
        .add("changeRatio", currentNumeratorValue / baselineNumeratorValue)
        .add("dimensions", super.getDimensions())
        .add("dimensionValues", super.getDimensionValues())
        .toString();
  }
}
