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

package org.apache.pinot.thirdeye.cube.additive;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.node.CubeNode;
import org.apache.pinot.thirdeye.cube.data.dbrow.BaseRow;


/**
 * Stores the additive metric that is returned from DB.
 */
public class AdditiveRow extends BaseRow {
  protected double baselineValue;
  protected double currentValue;

  /**
   * Constructs an additive row.
   *
   * @param dimensions the dimension names of this row.
   * @param dimensionValues the dimension values of this row.
   */
  public AdditiveRow(Dimensions dimensions, DimensionValues dimensionValues) {
    super(dimensions, dimensionValues);
  }

  /**
   * Constructs an additive row.
   *
   * @param dimensions the dimension names of this row.
   * @param dimensionValues the dimension values of this row.
   * @param baselineValue the baseline value of this additive metric.
   * @param currentValue the current value of this additive metric.
   */
  public AdditiveRow(Dimensions dimensions, DimensionValues dimensionValues, double baselineValue, double currentValue) {
    super(dimensions, dimensionValues);
    this.baselineValue = baselineValue;
    this.currentValue = currentValue;
  }

  /**
   * Returns the baseline value of this additive row.
   *
   * @return the baseline value of this additive row.
   */
  public double getBaselineValue() {
    return baselineValue;
  }

  /**
   * Sets the baseline value of this additive row.
   *
   * @param baselineValue the baseline value of this additive row.
   */
  public void setBaselineValue(double baselineValue) {
    this.baselineValue = baselineValue;
  }

  /**
   * Returns the current value of this additive row.
   *
   * @return the current value of this additive row.
   */
  public double getCurrentValue() {
    return currentValue;
  }

  /**
   * Sets the current value of this additive row.
   * @param currentValue the current value of this additive row.
   */
  public void setCurrentValue(double currentValue) {
    this.currentValue = currentValue;
  }

  @Override
  public CubeNode toNode() {
    return new AdditiveCubeNode(this);
  }

  @Override
  public CubeNode toNode(int level, int index, CubeNode parent) {
    return new AdditiveCubeNode(level, index, this, (AdditiveCubeNode) parent);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AdditiveRow row = (AdditiveRow) o;
    return Double.compare(row.getBaselineValue(), getBaselineValue()) == 0
        && Double.compare(row.getCurrentValue(), getCurrentValue()) == 0 && Objects
        .equals(getDimensions(), row.getDimensions()) && Objects.equals(getDimensionValues(), row.getDimensionValues());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDimensions(), getDimensionValues(), getBaselineValue(), getCurrentValue());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("baselineValue", baselineValue)
        .add("currentValue", currentValue)
        .add("dimensions", dimensions)
        .add("dimensionValues", dimensionValues)
        .toString();
  }
}
