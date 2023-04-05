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
package org.apache.pinot.segment.local.segment.creator.impl;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


class SameValueForwardIndexCreator implements ForwardIndexCreator {

  private final Object _actualValue;
  private final ForwardIndexCreator _delegate;

  public SameValueForwardIndexCreator(Object value, ForwardIndexCreator delegate) {
    _actualValue = value;
    _delegate = delegate;
  }

  @Override
  public void seal()
      throws IOException {
    _delegate.seal();
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return _delegate.isSingleValue();
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _delegate.getValueType();
  }

  @Override
  public void putInt(int value) {
    _delegate.putInt((int) _actualValue);
  }

  @Override
  public void putLong(long value) {
    _delegate.putLong((long) _actualValue);
  }

  @Override
  public void putFloat(float value) {
    _delegate.putFloat((float) _actualValue);
  }

  @Override
  public void putDouble(double value) {
    _delegate.putDouble((double) _actualValue);
  }

  @Override
  public void close()
      throws IOException {
    _delegate.close();
  }

  @Override
  public void putDictId(int dictId) {
    throw new UnsupportedOperationException("Cannot be used with dictionaries");
  }

  @Override
  public void putDictIdMV(int[] dictIds) {
    throw new UnsupportedOperationException("Cannot be used with dictionaries");
  }

  @Override
  public void putBigDecimal(BigDecimal value) {
    _delegate.putBigDecimal((BigDecimal) _actualValue);
  }

  @Override
  public void putString(String value) {
    _delegate.putString((String) _actualValue);
  }

  @Override
  public void putBytes(byte[] value) {
    _delegate.putBytes((byte[]) _actualValue);
  }

  @Override
  public void putIntMV(int[] values) {
    _delegate.putIntMV((int[]) _actualValue);
  }

  @Override
  public void putLongMV(long[] values) {
    _delegate.putLongMV((long[]) _actualValue);
  }

  @Override
  public void putFloatMV(float[] values) {
    _delegate.putFloatMV((float[]) _actualValue);
  }

  @Override
  public void putDoubleMV(double[] values) {
    _delegate.putDoubleMV((double[]) _actualValue);
  }

  @Override
  public void putStringMV(String[] values) {
    _delegate.putStringMV((String[]) _actualValue);
  }

  @Override
  public void putBytesMV(byte[][] values) {
    _delegate.putBytesMV((byte[][]) _actualValue);
  }
}
