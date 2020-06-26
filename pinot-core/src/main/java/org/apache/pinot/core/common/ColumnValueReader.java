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
package org.apache.pinot.core.common;

import org.apache.pinot.core.io.reader.ForwardIndexReader;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * ColumnValueReader is a wrapper class on top of the forward index reader to read values from it.
 * <p>Batch value read APIs support the following data type conversions for numeric types:
 * <ul>
 *   <li>INT -> LONG</li>
 *   <li>INT, LONG -> FLOAT</li>
 *   <li>INT, LONG, FLOAT -> DOUBLE</li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class ColumnValueReader {
  private final ForwardIndexReader _forwardIndexReader;
  private final DataType _valueType;
  private final ReaderContext _readerContext;

  public ColumnValueReader(ForwardIndexReader forwardIndexReader) {
    _forwardIndexReader = forwardIndexReader;
    _valueType = forwardIndexReader.getValueType();
    // TODO: Figure out a way to close the reader context. Currently it can cause direct memory leak.
    _readerContext = forwardIndexReader.createContext();
  }

  /**
   * Returns the data type of the values in the value reader.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  public DataType getValueType() {
    return _valueType;
  }

  /**
   * Returns {@code true} if the value reader is for a single-value column, {@code false} otherwise.
   */
  public boolean isSingleValue() {
    return _forwardIndexReader.isSingleValue();
  }

  /**
   * NOTE: The following single value read APIs do not handle the data type conversion for performance concern. Caller
   *       should always call the API that matches the value type.
   */

  /**
   * Returns the INT type single-value at the given document id.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  public int getIntValue(int docId) {
    return _forwardIndexReader.getInt(docId, _readerContext);
  }

  /**
   * Returns the LONG type single-value at the given document id.
   */
  public long getLongValue(int docId) {
    return _forwardIndexReader.getLong(docId, _readerContext);
  }

  /**
   * Returns the FLOAT type single-value at the given document id.
   */
  public float getFloatValue(int docId) {
    return _forwardIndexReader.getFloat(docId, _readerContext);
  }

  /**
   * Returns the DOUBLE type single-value at the given document id.
   */
  public double getDoubleValue(int docId) {
    return _forwardIndexReader.getDouble(docId, _readerContext);
  }

  /**
   * Returns the STRING type single-value at the given document id.
   */
  public String getStringValue(int docId) {
    return _forwardIndexReader.getString(docId, _readerContext);
  }

  /**
   * Returns the BYTES type single-value at the given document id.
   */
  public byte[] getBytesValue(int docId) {
    return _forwardIndexReader.getBytes(docId, _readerContext);
  }

  /**
   * Reads the INT type multi-value at the given document id into the value buffer and returns the number of values in
   * the multi-value entry.
   * <p>The passed in value buffer should be large enough to hold all the values of a multi-value entry.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  public int getIntValues(int docId, int[] valueBuffer) {
    return _forwardIndexReader.getIntArray(docId, valueBuffer, _readerContext);
  }

  // TODO: Support raw index for multi-value columns

  /**
   * NOTE: The following batch value read APIs support data type conversions for numeric types. Caller can call any
   *       API regardless of the value type.
   * TODO: Consider letting the caller handle the data type conversion because for different use cases, we might need to
   *       convert data type differently.
   */

  /**
   * Batch reads the INT type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  public void getIntValues(int[] docIds, int length, int[] valueBuffer) {
    if (_valueType == DataType.INT) {
      _forwardIndexReader.readValues(docIds, length, valueBuffer, _readerContext);
    } else {
      throw new IllegalStateException(String.format("Cannot read %s as INT", _valueType));
    }
  }

  /**
   * Batch reads the LONG type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  public void getLongValues(int[] docIds, int length, long[] valueBuffer) {
    switch (_valueType) {
      case INT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getInt(docIds[i], _readerContext);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getLong(docIds[i], _readerContext);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Cannot read %s as LONG", _valueType));
    }
  }

  /**
   * Batch reads the FLOAT type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  public void getFloatValues(int[] docIds, int length, float[] valueBuffer) {
    switch (_valueType) {
      case INT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getInt(docIds[i], _readerContext);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getLong(docIds[i], _readerContext);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getFloat(docIds[i], _readerContext);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Cannot read %s as FLOAT", _valueType));
    }
  }

  /**
   * Batch reads the DOUBLE type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  public void getDoubleValues(int[] docIds, int length, double[] valueBuffer) {
    switch (_valueType) {
      case INT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getInt(docIds[i], _readerContext);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getLong(docIds[i], _readerContext);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getFloat(docIds[i], _readerContext);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _forwardIndexReader.getDouble(docIds[i], _readerContext);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Cannot read %s as DOUBLE", _valueType));
    }
  }

  /**
   * Batch reads the STRING type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  public void getStringValues(int[] docIds, int length, String[] valueBuffer) {
    if (_valueType == DataType.STRING) {
      for (int i = 0; i < length; i++) {
        valueBuffer[i] = _forwardIndexReader.getString(docIds[i], _readerContext);
      }
    } else {
      throw new IllegalStateException(String.format("Cannot read %s as STRING", _valueType));
    }
  }

  /**
   * Batch reads the BYTES type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  public void getBytesValues(int[] docIds, int length, byte[][] valueBuffer) {
    if (_valueType == DataType.BYTES) {
      for (int i = 0; i < length; i++) {
        valueBuffer[i] = _forwardIndexReader.getBytes(docIds[i], _readerContext);
      }
    } else {
      throw new IllegalStateException(String.format("Cannot read %s as BYTES", _valueType));
    }
  }
}
