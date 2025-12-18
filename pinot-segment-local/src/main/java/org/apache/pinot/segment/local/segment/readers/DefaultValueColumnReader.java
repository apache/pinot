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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.ColumnReader;


/**
 * ColumnReader implementation that returns default values for new columns.
 *
 * <p>This reader is used when a column exists in the target schema but not in the source data.
 * It returns the default null value for the field spec for all document IDs.
 */
public class DefaultValueColumnReader implements ColumnReader {

  private final String _columnName;
  private final int _numDocs;
  private final Object _defaultValue;
  private final FieldSpec _fieldSpec;
  private final FieldSpec.DataType _dataType;

  // Pre-computed multi-value arrays for reuse
  private int[] _defaultIntMV;
  private long[] _defaultLongMV;
  private float[] _defaultFloatMV;
  private double[] _defaultDoubleMV;
  private String[] _defaultStringMV;
  private byte[][] _defaultBytesMV;

  private int _currentIndex;

  /**
   * Create a DefaultValueColumnReader for a new column.
   *
   * @param columnName Name of the new column
   * @param numDocs Total number of documents
   * @param fieldSpec Field specification for the new column
   */
  public DefaultValueColumnReader(String columnName, int numDocs, FieldSpec fieldSpec) {
    _columnName = columnName;
    _numDocs = numDocs;
    _currentIndex = 0;
    _fieldSpec = fieldSpec;
    _dataType = fieldSpec.getDataType();

    // For multi-value fields, wrap the default value in an array
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    if (fieldSpec.isSingleValueField()) {
      _defaultValue = defaultNullValue;
    } else {
      _defaultValue = new Object[]{defaultNullValue};
      // Pre-compute typed arrays for multi-value fields to avoid repeated allocations
      Object[] defaultArray = (Object[]) _defaultValue;
      switch (_dataType) {
        case INT:
          _defaultIntMV = new int[defaultArray.length];
          for (int i = 0; i < defaultArray.length; i++) {
            _defaultIntMV[i] = ((Number) defaultArray[i]).intValue();
          }
          break;
        case LONG:
          _defaultLongMV = new long[defaultArray.length];
          for (int i = 0; i < defaultArray.length; i++) {
            _defaultLongMV[i] = ((Number) defaultArray[i]).longValue();
          }
          break;
        case FLOAT:
          _defaultFloatMV = new float[defaultArray.length];
          for (int i = 0; i < defaultArray.length; i++) {
            _defaultFloatMV[i] = ((Number) defaultArray[i]).floatValue();
          }
          break;
        case DOUBLE:
          _defaultDoubleMV = new double[defaultArray.length];
          for (int i = 0; i < defaultArray.length; i++) {
            _defaultDoubleMV[i] = ((Number) defaultArray[i]).doubleValue();
          }
          break;
        case STRING:
          _defaultStringMV = new String[defaultArray.length];
          for (int i = 0; i < defaultArray.length; i++) {
            _defaultStringMV[i] = (String) defaultArray[i];
          }
          break;
        case BYTES:
          _defaultBytesMV = new byte[defaultArray.length][];
          for (int i = 0; i < defaultArray.length; i++) {
            _defaultBytesMV[i] = (byte[]) defaultArray[i];
          }
          break;
        default:
          break;
      }
    }
  }

  @Override
  public boolean hasNext() {
    return _currentIndex < _numDocs;
  }

  @Override
  @Nullable
  public Object next() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return _defaultValue;
  }

  @Override
  public void rewind() {
    _currentIndex = 0;
  }

  @Override
  public boolean isNextNull() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    return _defaultValue == null;
  }

  @Override
  public void skipNext() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
  }

  @Override
  public boolean isSingleValue() {
    return _fieldSpec.isSingleValueField();
  }

  @Override
  public boolean isInt() {
    return _dataType == FieldSpec.DataType.INT;
  }

  @Override
  public boolean isLong() {
    return _dataType == FieldSpec.DataType.LONG;
  }

  @Override
  public boolean isFloat() {
    return _dataType == FieldSpec.DataType.FLOAT;
  }

  @Override
  public boolean isDouble() {
    return _dataType == FieldSpec.DataType.DOUBLE;
  }

  @Override
  public boolean isString() {
    return _dataType == FieldSpec.DataType.STRING;
  }

  @Override
  public boolean isBytes() {
    return _dataType == FieldSpec.DataType.BYTES;
  }

  @Override
  public int nextInt() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return ((Number) _defaultValue).intValue();
  }

  @Override
  public long nextLong() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return ((Number) _defaultValue).longValue();
  }

  @Override
  public float nextFloat() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return ((Number) _defaultValue).floatValue();
  }

  @Override
  public double nextDouble() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return ((Number) _defaultValue).doubleValue();
  }

  @Override
  public String nextString() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return (String) _defaultValue;
  }

  @Override
  public byte[] nextBytes() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return (byte[]) _defaultValue;
  }

  @Override
  public int[] nextIntMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return _defaultIntMV;
  }

  @Override
  public long[] nextLongMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return _defaultLongMV;
  }

  @Override
  public float[] nextFloatMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return _defaultFloatMV;
  }

  @Override
  public double[] nextDoubleMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return _defaultDoubleMV;
  }

  @Override
  public String[] nextStringMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return _defaultStringMV;
  }

  @Override
  public byte[][] nextBytesMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    _currentIndex++;
    return _defaultBytesMV;
  }

  @Override
  public String getColumnName() {
    return _columnName;
  }

  @Override
  public int getTotalDocs() {
    return _numDocs;
  }

  @Override
  public boolean isNull(int docId) {
    validateDocId(docId);
    return _defaultValue == null;
  }

  // Single-value accessors

  @Override
  public int getInt(int docId) {
    validateDocId(docId);
    return ((Number) _defaultValue).intValue();
  }

  @Override
  public long getLong(int docId) {
    validateDocId(docId);
    return ((Number) _defaultValue).longValue();
  }

  @Override
  public float getFloat(int docId) {
    validateDocId(docId);
    return ((Number) _defaultValue).floatValue();
  }

  @Override
  public double getDouble(int docId) {
    validateDocId(docId);
    return ((Number) _defaultValue).doubleValue();
  }

  @Override
  public String getString(int docId) {
    validateDocId(docId);
    return (String) _defaultValue;
  }

  @Override
  public byte[] getBytes(int docId) {
    validateDocId(docId);
    return (byte[]) _defaultValue;
  }

  @Override
  public Object getValue(int docId) {
    validateDocId(docId);
    return _defaultValue;
  }

  // Multi-value accessors

  @Override
  public int[] getIntMV(int docId) {
    validateDocId(docId);
    return _defaultIntMV;
  }

  @Override
  public long[] getLongMV(int docId) {
    validateDocId(docId);
    return _defaultLongMV;
  }

  @Override
  public float[] getFloatMV(int docId) {
    validateDocId(docId);
    return _defaultFloatMV;
  }

  @Override
  public double[] getDoubleMV(int docId) {
    validateDocId(docId);
    return _defaultDoubleMV;
  }

  @Override
  public String[] getStringMV(int docId) {
    validateDocId(docId);
    return _defaultStringMV;
  }

  @Override
  public byte[][] getBytesMV(int docId) {
    validateDocId(docId);
    return _defaultBytesMV;
  }

  /**
   * Validate that the document ID is within valid range.
   *
   * @param docId Document ID to validate
   * @throws IndexOutOfBoundsException if docId is out of range
   */
  private void validateDocId(int docId) {
    if (docId < 0 || docId >= _numDocs) {
      throw new IndexOutOfBoundsException(
          "docId " + docId + " is out of range. Valid range is 0 to " + (_numDocs - 1));
    }
  }

  @Override
  public void close() {
    // No resources to close
  }
}
