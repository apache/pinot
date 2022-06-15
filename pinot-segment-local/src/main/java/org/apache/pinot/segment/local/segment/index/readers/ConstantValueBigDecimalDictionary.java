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
 * Dictionary of a single BIG_DECIMAL value.
 */
public class ConstantValueBigDecimalDictionary extends BaseImmutableDictionary {
  private final BigDecimal _value;

  public ConstantValueBigDecimalDictionary(BigDecimal value) {
    super(1);
    _value = value;
  }

  @Override
  public DataType getValueType() {
    return DataType.BIG_DECIMAL;
  }

  @Override
  public int indexOf(String stringValue) {
    // Use compareTo instead of equals because
    return new BigDecimal(stringValue).compareTo(_value) == 0 ? 0 : NULL_VALUE_INDEX;
  }

  @Override
  public int indexOf(BigDecimal bigDecimalValue) {
    return bigDecimalValue.compareTo(_value) == 0 ? 0 : NULL_VALUE_INDEX;
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int compareResult = new BigDecimal(stringValue).compareTo(_value);
    if (compareResult < 0) {
      return -1;
    }
    if (compareResult > 0) {
      return -2;
    }
    return 0;
  }

  @Override
  public BigDecimal getMinVal() {
    return _value;
  }

  @Override
  public BigDecimal getMaxVal() {
    return _value;
  }

  @Override
  public BigDecimal[] getSortedValues() {
    return new BigDecimal[]{_value};
  }

  @Override
  public BigDecimal get(int dictId) {
    return _value;
  }

  @Override
  public int getIntValue(int dictId) {
    return _value.intValue();
  }

  @Override
  public long getLongValue(int dictId) {
    return _value.longValue();
  }

  @Override
  public float getFloatValue(int dictId) {
    return _value.floatValue();
  }

  @Override
  public double getDoubleValue(int dictId) {
    return _value.doubleValue();
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return _value;
  }

  @Override
  public String getStringValue(int dictId) {
    return _value.toPlainString();
  }
}
