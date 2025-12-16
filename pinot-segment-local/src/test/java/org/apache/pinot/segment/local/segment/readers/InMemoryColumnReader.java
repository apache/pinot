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
package org.apache.pinot.segment.local.segment.readers;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.ColumnReader;


/**
 * Mock ColumnReader for testing with configurable data.
 */
public class InMemoryColumnReader implements ColumnReader {
  private final String _columnName;
  private final Object[] _values;
  private final boolean[] _isNull;
  private final boolean _isSingleValue;
  private final Class<?> _dataType;
  private int _currentIndex = 0;

  public InMemoryColumnReader(String columnName, Object[] values, boolean[] isNull, boolean isSingleValue,
      Class<?> dataType) {
    _columnName = columnName;
    _values = values;
    _isNull = isNull;
    _isSingleValue = isSingleValue;
    _dataType = dataType;
  }

  @Override
  public boolean hasNext() {
    return _currentIndex < _values.length;
  }

  @Nullable
  @Override
  public Object next() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values");
    }
    Object value = _values[_currentIndex];
    _currentIndex++;
    return value;
  }

  @Override
  public boolean isNextNull() {
    return hasNext() && _isNull[_currentIndex];
  }

  @Override
  public void skipNext() {
    if (hasNext()) {
      _currentIndex++;
    }
  }

  @Override
  public boolean isSingleValue() {
    return _isSingleValue;
  }

  @Override
  public boolean isInt() {
    return _dataType == Integer.class;
  }

  @Override
  public boolean isLong() {
    return _dataType == Long.class;
  }

  @Override
  public boolean isFloat() {
    return _dataType == Float.class;
  }

  @Override
  public boolean isDouble() {
    return _dataType == Double.class;
  }

  @Override
  public boolean isString() {
    return _dataType == String.class;
  }

  @Override
  public boolean isBytes() {
    return _dataType == byte[].class;
  }

  @Override
  public int nextInt() {
    return (Integer) next();
  }

  @Override
  public long nextLong() {
    return (Long) next();
  }

  @Override
  public float nextFloat() {
    return (Float) next();
  }

  @Override
  public double nextDouble() {
    return (Double) next();
  }

  @Override
  public String nextString() {
    return (String) next();
  }

  @Override
  public byte[] nextBytes() {
    return (byte[]) next();
  }

  @Override
  public int[] nextIntMV() {
    return (int[]) next();
  }

  @Override
  public long[] nextLongMV() {
    return (long[]) next();
  }

  @Override
  public float[] nextFloatMV() {
    return (float[]) next();
  }

  @Override
  public double[] nextDoubleMV() {
    return (double[]) next();
  }

  @Override
  public String[] nextStringMV() {
    return (String[]) next();
  }

  @Override
  public byte[][] nextBytesMV() {
    return (byte[][]) next();
  }

  @Override
  public void rewind() {
    _currentIndex = 0;
  }

  @Override
  public String getColumnName() {
    return _columnName;
  }

  @Override
  public int getTotalDocs() {
    return _values.length;
  }

  @Override
  public boolean isNull(int docId) {
    return _isNull[docId];
  }

  @Override
  public int getInt(int docId) {
    return (Integer) _values[docId];
  }

  @Override
  public long getLong(int docId) {
    return (Long) _values[docId];
  }

  @Override
  public float getFloat(int docId) {
    return (Float) _values[docId];
  }

  @Override
  public double getDouble(int docId) {
    return (Double) _values[docId];
  }

  @Override
  public String getString(int docId) {
    return (String) _values[docId];
  }

  @Override
  public byte[] getBytes(int docId) {
    return (byte[]) _values[docId];
  }

  @Override
  public Object getValue(int docId) {
    return _values[docId];
  }

  @Override
  public int[] getIntMV(int docId) {
    return (int[]) _values[docId];
  }

  @Override
  public long[] getLongMV(int docId) {
    return (long[]) _values[docId];
  }

  @Override
  public float[] getFloatMV(int docId) {
    return (float[]) _values[docId];
  }

  @Override
  public double[] getDoubleMV(int docId) {
    return (double[]) _values[docId];
  }

  @Override
  public String[] getStringMV(int docId) {
    return (String[]) _values[docId];
  }

  @Override
  public byte[][] getBytesMV(int docId) {
    return (byte[][]) _values[docId];
  }

  @Override
  public void close() {
    // No-op
  }
}
