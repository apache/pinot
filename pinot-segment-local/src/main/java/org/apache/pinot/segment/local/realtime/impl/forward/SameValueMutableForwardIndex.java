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
package org.apache.pinot.segment.local.realtime.impl.forward;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * SameValueMutableForwardIndex is used to wrap any MutableForwardIndex, but store the same value. This is done to
 * allow noRawDataForTextIndex config to work with mutable indexes.
 */
public class SameValueMutableForwardIndex implements MutableForwardIndex {

  private final Object _actualValue;
  private final Object[] _actualValues;
  private final MutableForwardIndex _delegate;

  public SameValueMutableForwardIndex(Object actualValue, MutableForwardIndex delegate) {
    _actualValue = actualValue;
    _actualValues = new Object[]{actualValue};
    _delegate = delegate;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _actualValue.toString().length();
  }

  @Override
  public int getLengthOfLongestElement() {
    return _actualValue.toString().length();
  }

  @Override
  public boolean isDictionaryEncoded() {
    return _delegate.isDictionaryEncoded();
  }

  @Override
  public boolean isSingleValue() {
    return _delegate.isSingleValue();
  }

  @Override
  public FieldSpec.DataType getStoredType() {
    return _delegate.getStoredType();
  }

  // Single-value methods
  @Override
  public int getDictId(int docId) {
    return _delegate.getDictId(docId);
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer) {
    _delegate.readDictIds(docIds, length, dictIdBuffer);
  }

  @Override
  public int getInt(int docId) {
    return _delegate.getInt(docId);
  }

  @Override
  public long getLong(int docId) {
    return _delegate.getLong(docId);
  }

  @Override
  public float getFloat(int docId) {
    return _delegate.getFloat(docId);
  }

  @Override
  public double getDouble(int docId) {
    return _delegate.getDouble(docId);
  }

  @Override
  public String getString(int docId) {
    return _actualValue.toString();
  }

  @Override
  public byte[] getBytes(int docId) {
    return _actualValue.toString().getBytes();
  }

  @Override
  public void setDictId(int docId, int dictId) {
    _delegate.setDictId(docId, dictId);
  }

  @Override
  public void setInt(int docId, int value) {
    _delegate.setInt(docId, value);
  }

  @Override
  public void setLong(int docId, long value) {
    _delegate.setLong(docId, value);
  }

  @Override
  public void setFloat(int docId, float value) {
    _delegate.setFloat(docId, value);
  }

  @Override
  public void setDouble(int docId, double value) {
    _delegate.setDouble(docId, value);
  }

  @Override
  public void setString(int docId, String value) {
    _delegate.setString(docId, value);
  }

  @Override
  public void setBytes(int docId, byte[] value) {
    _delegate.setBytes(docId, value);
  }

  // Multi-value methods
  @Override
  public int getDictIdMV(int docId, int[] dictIdBuffer) {
    return _delegate.getDictIdMV(docId, dictIdBuffer);
  }

  @Override
  public int[] getDictIdMV(int docId) {
    return _delegate.getDictIdMV(docId);
  }

  @Override
  public int[] getIntMV(int docId) {
    return _delegate.getIntMV(docId);
  }

  @Override
  public long[] getLongMV(int docId) {
    return _delegate.getLongMV(docId);
  }

  @Override
  public float[] getFloatMV(int docId) {
    return _delegate.getFloatMV(docId);
  }

  @Override
  public double[] getDoubleMV(int docId) {
    return _delegate.getDoubleMV(docId);
  }

  @Override
  public String[] getStringMV(int docId) {
    return _delegate.getStringMV(docId);
  }

  @Override
  public byte[][] getBytesMV(int docId) {
    return _delegate.getBytesMV(docId);
  }

  @Override
  public int getNumValuesMV(int docId) {
    return _delegate.getNumValuesMV(docId);
  }

  @Override
  public void setDictIdMV(int docId, int[] dictIds) {
    _delegate.setDictIdMV(docId, dictIds);
  }

  @Override
  public void setIntMV(int docId, int[] values) {
    _delegate.setIntMV(docId, values);
  }

  @Override
  public void setLongMV(int docId, long[] values) {
    _delegate.setLongMV(docId, values);
  }

  @Override
  public void setFloatMV(int docId, float[] values) {
    _delegate.setFloatMV(docId, values);
  }

  @Override
  public void setDoubleMV(int docId, double[] values) {
    _delegate.setDoubleMV(docId, values);
  }

  @Override
  public void setStringMV(int docId, String[] values) {
    _delegate.setStringMV(docId, values);
  }

  @Override
  public void setBytesMV(int docId, byte[][] values) {
    _delegate.setBytesMV(docId, values);
  }

  @Override
  public void close()
      throws IOException {
    _delegate.close();
  }

  @Override
  public void add(Object value, int dictId, int docId) {
    _delegate.add(_actualValue, dictId, docId);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds, int docId) {
    _delegate.add(_actualValues, dictIds, docId);
  }
}
