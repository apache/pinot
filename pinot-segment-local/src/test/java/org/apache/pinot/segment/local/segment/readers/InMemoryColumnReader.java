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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.apache.pinot.spi.utils.PinotDataType;


/**
 * Mock ColumnReader for testing with configurable data.
 */
public class InMemoryColumnReader implements ColumnReader {
  private final String _columnName;
  private final Object[] _values;
  private final boolean[] _isNull;
  private final boolean _isSingleValue;
  private final Class<?> _dataType;

  public InMemoryColumnReader(String columnName, Object[] values, boolean[] isNull, boolean isSingleValue,
      Class<?> dataType) {
    _columnName = columnName;
    _values = values;
    _isNull = isNull;
    _isSingleValue = isSingleValue;
    _dataType = dataType;
  }

  @Override
  public String getColumnName() {
    return _columnName;
  }

  @Nullable
  @Override
  public PinotDataType getValueType() {
    DataType dataType;
    if (_dataType == Integer.class) {
      dataType = DataType.INT;
    } else if (_dataType == Long.class) {
      dataType = DataType.LONG;
    } else if (_dataType == Float.class) {
      dataType = DataType.FLOAT;
    } else if (_dataType == Double.class) {
      dataType = DataType.DOUBLE;
    } else if (_dataType == BigDecimal.class) {
      dataType = DataType.BIG_DECIMAL;
    } else if (_dataType == String.class) {
      dataType = DataType.STRING;
    } else if (_dataType == byte[].class) {
      dataType = DataType.BYTES;
    } else {
      return null;
    }
    return ColumnReader.toValueType(dataType, _isSingleValue);
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
  public Object getValue(int docId) {
    Object value = _values[docId];
    if (_isSingleValue || value == null || value instanceof Object[]) {
      return value;
    }
    // A multi-value column returns an Object[] per the ColumnReader contract; box primitive MV arrays (int[], long[],
    // ...) so getValue() matches the interface instead of leaking the raw primitive array.
    int length = Array.getLength(value);
    Object[] boxed = new Object[length];
    for (int i = 0; i < length; i++) {
      boxed[i] = Array.get(value, i);
    }
    return boxed;
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
  public BigDecimal getBigDecimal(int docId) {
    return (BigDecimal) _values[docId];
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
  public MultiValueResult<int[]> getIntMV(int docId) {
    return MultiValueResult.of((int[]) _values[docId], null);
  }

  @Override
  public MultiValueResult<long[]> getLongMV(int docId) {
    return MultiValueResult.of((long[]) _values[docId], null);
  }

  @Override
  public MultiValueResult<float[]> getFloatMV(int docId) {
    return MultiValueResult.of((float[]) _values[docId], null);
  }

  @Override
  public MultiValueResult<double[]> getDoubleMV(int docId) {
    return MultiValueResult.of((double[]) _values[docId], null);
  }

  @Override
  public BigDecimal[] getBigDecimalMV(int docId) {
    return (BigDecimal[]) _values[docId];
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
