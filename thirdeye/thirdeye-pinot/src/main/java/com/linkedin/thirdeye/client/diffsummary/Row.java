/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.client.diffsummary;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


class Row {
  private Dimensions dimensions;
  private DimensionValues dimensionValues;
  private double baselineValue;
  private double currentValue;

  public Row(Dimensions dimensions, DimensionValues dimensionValues) {
    this.dimensions = Preconditions.checkNotNull(dimensions);
    this.dimensionValues = Preconditions.checkNotNull(dimensionValues);
  }

  public Row(Dimensions dimensions, DimensionValues dimensionValues, double baselineValue, double currentValue) {
    this.dimensions = Preconditions.checkNotNull(dimensions);
    this.dimensionValues = Preconditions.checkNotNull(dimensionValues);
    this.baselineValue = baselineValue;
    this.currentValue = currentValue;
  }

  public Dimensions getDimensions() {
    return dimensions;
  }

  public void setDimensions(Dimensions dimensions) {
    this.dimensions = Preconditions.checkNotNull(dimensions);
  }

  public DimensionValues getDimensionValues() {
    return dimensionValues;
  }

  public void setDimensionValues(DimensionValues dimensionValues) {
    this.dimensionValues = Preconditions.checkNotNull(dimensionValues);
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

  public double ratio() {
    return currentValue / baselineValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Row row = (Row) o;
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
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
