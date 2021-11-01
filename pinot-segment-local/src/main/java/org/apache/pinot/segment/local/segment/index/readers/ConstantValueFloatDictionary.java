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
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Dictionary of a single float value.
 */
public class ConstantValueFloatDictionary extends BaseImmutableDictionary {
  private final float _value;

  public ConstantValueFloatDictionary(float value) {
    super(1);
    _value = value;
  }

  @Override
  public DataType getValueType() {
    return DataType.FLOAT;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    float floatValue = Float.parseFloat(stringValue);
    if (floatValue < _value) {
      return -1;
    }
    if (floatValue > _value) {
      return -2;
    }
    return 0;
  }

  @Override
  public Float getMinVal() {
    return _value;
  }

  @Override
  public Float getMaxVal() {
    return _value;
  }

  @Override
  public float[] getSortedValues() {
    return new float[]{_value};
  }

  @Override
  public Float get(int dictId) {
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
    return _value;
  }

  @Override
  public double getDoubleValue(int dictId) {
    return _value;
  }

  @Override
  public String getStringValue(int dictId) {
    return Float.toString(_value);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimalUtils.valueOf(_value);
  }
}
