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
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * Dictionary of a single string value.
 */
public class ConstantValueStringDictionary extends BaseImmutableDictionary {
  private final String _value;

  public ConstantValueStringDictionary(String value) {
    super(1);
    _value = value;
  }

  @Override
  public DataType getValueType() {
    return DataType.STRING;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int result = stringValue.compareTo(_value);
    if (result < 0) {
      return -1;
    }
    if (result > 0) {
      return -2;
    }
    return 0;
  }

  @Override
  public String getMinVal() {
    return _value;
  }

  @Override
  public String getMaxVal() {
    return _value;
  }

  @Override
  public String[] getSortedValues() {
    return new String[]{_value};
  }

  @Override
  public String get(int dictId) {
    return _value;
  }

  @Override
  public int getIntValue(int dictId) {
    return Integer.parseInt(_value);
  }

  @Override
  public long getLongValue(int dictId) {
    return Long.parseLong(_value);
  }

  @Override
  public float getFloatValue(int dictId) {
    return Float.parseFloat(_value);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(_value);
  }

  @Override
  public String getStringValue(int dictId) {
    return _value;
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return BytesUtils.toBytes(_value);
  }
}
