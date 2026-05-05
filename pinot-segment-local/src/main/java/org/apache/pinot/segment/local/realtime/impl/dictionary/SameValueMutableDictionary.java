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
package org.apache.pinot.segment.local.realtime.impl.dictionary;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.Utf8Utils;


/**
 * SameValueMutableDictionary is used to wrap any MutableDictionary, but store the same value. This is done to
 * allow noRawDataForTextIndex config to work with mutable indexes.
 */
public class SameValueMutableDictionary implements MutableDictionary {
  private final String _actualValue;
  private final String[] _actualValues;
  private final byte[] _valueBytes;
  private final MutableDictionary _delegate;

  public SameValueMutableDictionary(Object actualValue, MutableDictionary delegate) {
    _actualValue = actualValue.toString();
    _actualValues = new String[]{_actualValue};
    _valueBytes = Utf8Utils.encode(_actualValue);
    _delegate = delegate;
  }

  public int index(Object value) {
    return _delegate.index(_actualValue);
  }

  public int[] index(Object[] values) {
    return _delegate.index(_actualValues);
  }

  @Override
  public DataType getValueType() {
    return _delegate.getValueType();
  }

  @Override
  public int length() {
    return _delegate.length();
  }

  @Override
  public int indexOf(String stringValue) {
    return _delegate.index(stringValue);
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    return _delegate.getDictIdsInRange(lower, upper, includeLower, includeUpper);
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return _delegate.compare(dictId1, dictId2);
  }

  @Override
  public String getMinVal() {
    return _actualValue;
  }

  @Override
  public String getMaxVal() {
    return _actualValue;
  }

  @Override
  public String[] getSortedValues() {
    return _actualValues;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _valueBytes.length;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _valueBytes.length;
  }

  @Override
  public boolean isAscii() {
    return _valueBytes.length == _actualValue.length();
  }

  @Override
  public String get(int dictId) {
    return _actualValue;
  }

  @Override
  public int getIntValue(int dictId) {
    return Integer.parseInt(_actualValue);
  }

  @Override
  public long getLongValue(int dictId) {
    return Long.parseLong(_actualValue);
  }

  @Override
  public float getFloatValue(int dictId) {
    return Float.parseFloat(_actualValue);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(_actualValue);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return new BigDecimal(_actualValue);
  }

  @Override
  public String getStringValue(int dictId) {
    return _actualValue;
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _valueBytes;
  }

  @Override
  public int getValueSize(int dictId) {
    return _valueBytes.length;
  }

  @Override
  public void close()
      throws IOException {
    _delegate.close();
  }
}
