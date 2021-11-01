/**
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
package org.apache.pinot.segment.local.segment.index.readers;

import java.math.BigDecimal;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Dictionary of a single double value.
 */
public class ConstantValueDoubleDictionary extends BaseImmutableDictionary {
  private final double _value;

  public ConstantValueDoubleDictionary(double value) {
    super(1);
    _value = value;
  }

  @Override
  public DataType getValueType() {
    return DataType.DOUBLE;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    double doubleValue = Double.parseDouble(stringValue);
    if (doubleValue < _value) {
      return -1;
    }
    if (doubleValue > _value) {
      return -2;
    }
    return 0;
  }

  @Override
  public Double getMinVal() {
    return _value;
  }

  @Override
  public Double getMaxVal() {
    return _value;
  }

  @Override
  public double[] getSortedValues() {
    return new double[]{_value};
  }

  @Override
  public Double get(int dictId) {
    return _value;
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) _value;
  }

  @Override
  public long getLongValue(int dictId) {
    return (long) _value;
  }

  @Override
  public float getFloatValue(int dictId) {
    return (float) _value;
  }

  @Override
  public double getDoubleValue(int dictId) {
    return _value;
  }

  @Override
  public String getStringValue(int dictId) {
    return Double.toString(_value);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimal.valueOf(_value);
  }
}
