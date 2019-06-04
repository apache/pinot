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

package org.apache.pinot.thirdeye.cube.data.dbrow;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;


public abstract class BaseRow implements Row {
  protected Dimensions dimensions;
  protected DimensionValues dimensionValues;

  public BaseRow() { }

  public BaseRow(Dimensions dimensions, DimensionValues dimensionValues) {
    this.dimensions = Preconditions.checkNotNull(dimensions);
    this.dimensionValues = Preconditions.checkNotNull(dimensionValues);
  }

  @Override
  public Dimensions getDimensions() {
    return dimensions;
  }

  @Override
  public void setDimensions(Dimensions dimensions) {
    this.dimensions = Preconditions.checkNotNull(dimensions);
  }

  @Override
  public DimensionValues getDimensionValues() {
    return dimensionValues;
  }

  @Override
  public void setDimensionValues(DimensionValues dimensionValues) {
    this.dimensionValues = Preconditions.checkNotNull(dimensionValues);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BaseRow)) {
      return false;
    }
    BaseRow baseRow = (BaseRow) o;
    return Objects.equal(dimensions, baseRow.dimensions) && Objects.equal(dimensionValues, baseRow.dimensionValues);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dimensions, dimensionValues);
  }
}
