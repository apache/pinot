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

import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Dictionary of a single int value.
 */
public class ConstantValueIntDictionary extends BaseImmutableDictionary {
  private final int _value;

  public ConstantValueIntDictionary(int value) {
    super(1);
    _value = value;
  }

  @Override
  public DataType getValueType() {
    return DataType.INT;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int intValue = Integer.parseInt(stringValue);
    if (intValue < _value) {
      return -1;
    }
    if (intValue > _value) {
      return -2;
    }
    return 0;
  }

  @Override
  public Integer getMinVal() {
    return _value;
  }

  @Override
  public Integer getMaxVal() {
    return _value;
  }

  @Override
  public int[] getSortedValues() {
    return new int[]{_value};
  }

  @Override
  public Integer get(int dictId) {
    return _value;
  }

  @Override
  public int getIntValue(int dictId) {
    return _value;
  }

  @Override
  public long getLongValue(int dictId) {
    return _value;
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
    return Integer.toString(_value);
  }
}
