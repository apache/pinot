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

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.MultiValueResult;


/**
 * Implementation of ColumnReader for Pinot segments.
 *
 * <p>This class wraps the existing PinotSegmentColumnReader and provides the ColumnReader interface
 * for columnar segment building. It handles:
 * <ul>
 *   <li>Reading column values from Pinot segments</li>
 *   <li>Resource cleanup</li>
 * </ul>
 */
public class PinotSegmentColumnReaderImpl implements ColumnReader {
  private final PinotSegmentColumnReader _segmentColumnReader;
  private final String _columnName;
  private final int _numDocs;
  private final FieldSpec.DataType _dataType;
  private final boolean _skipDefaultNullValues;

  private int _currentIndex;

  // Reusable variables to avoid garbage collection on every next() call
  private Object _reuseValue;

  /**
   * Create a PinotSegmentColumnReaderImpl for an existing column in the segment.
   *
   * @param indexSegment Source segment to read from
   * @param columnName Name of the column
   */
  public PinotSegmentColumnReaderImpl(IndexSegment indexSegment, String columnName) {
    this(indexSegment, columnName, false);
  }

  /**
   * Create a PinotSegmentColumnReaderImpl for an existing column in the segment.
   *
   * @param indexSegment Source segment to read from
   * @param columnName Name of the column
   * @param skipDefaultNullValues Whether to skip reading default null values from the record.
   *                              If true, null values return null. If false, null values return
   *                              the segment's stored value (which contains the default).
   */
  public PinotSegmentColumnReaderImpl(IndexSegment indexSegment, String columnName,
      boolean skipDefaultNullValues) {
    this(new PinotSegmentColumnReader(indexSegment, columnName), columnName,
        indexSegment.getSegmentMetadata().getTotalDocs(),
        indexSegment.getSegmentMetadata().getSchema().getFieldSpecFor(columnName).getDataType(),
        skipDefaultNullValues);
  }

  /**
   * Constructor for subclasses that need to provide their own PinotSegmentColumnReader.
   *
   * @param segmentColumnReader The segment column reader
   * @param columnName Name of the column
   * @param numDocs Total number of documents
   * @param dataType The data type of the column
   * @param skipDefaultNullValues Whether to skip reading default null values from the record.
   *                              If true, null values return null. If false, null values return
   *                              the segment's stored value (which contains the default).
   */
  public PinotSegmentColumnReaderImpl(PinotSegmentColumnReader segmentColumnReader, String columnName,
      int numDocs, FieldSpec.DataType dataType, boolean skipDefaultNullValues) {
    _segmentColumnReader = segmentColumnReader;
    _columnName = columnName;
    _numDocs = numDocs;
    _dataType = dataType;
    _currentIndex = 0;
    _skipDefaultNullValues = skipDefaultNullValues;
  }

  @Override
  public boolean hasNext() {
    return _currentIndex < _numDocs;
  }

  @Override
  @Nullable
  public Object next()
      throws IOException {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }

    // Return null if the value is null and skipDefaultNullValues is true
    if (_skipDefaultNullValues && _segmentColumnReader.isNull(_currentIndex)) {
      _currentIndex++;
      return null;
    }
    _reuseValue = _segmentColumnReader.getValue(_currentIndex);
    _currentIndex++;

    // Return null if the value is null
    if (_reuseValue == null) {
      return null;
    }

    return _reuseValue;
  }

  @Override
  public boolean isNextNull() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    return _segmentColumnReader.isNull(_currentIndex);
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
    return _segmentColumnReader.isSingleValue();
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
    int value = _segmentColumnReader.getInt(_currentIndex);
    _currentIndex++;
    return value;
  }

  @Override
  public long nextLong() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    long value = _segmentColumnReader.getLong(_currentIndex);
    _currentIndex++;
    return value;
  }

  @Override
  public float nextFloat() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    float value = _segmentColumnReader.getFloat(_currentIndex);
    _currentIndex++;
    return value;
  }

  @Override
  public double nextDouble() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    double value = _segmentColumnReader.getDouble(_currentIndex);
    _currentIndex++;
    return value;
  }

  @Override
  public String nextString() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    String value = _segmentColumnReader.getString(_currentIndex);
    _currentIndex++;
    return value;
  }

  @Override
  public byte[] nextBytes() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    byte[] value = _segmentColumnReader.getBytes(_currentIndex);
    _currentIndex++;
    return value;
  }

  // For all multi-value primitive type methods (nextIntMV, nextLongMV, nextFloatMV, nextDoubleMV,
  // getIntMV, getLongMV, getFloatMV, getDoubleMV), we pass null for the validity bitset since
  // multi-value primitive types cannot have null elements. Nulls are removed by NullValueTransformer
  @Override
  public MultiValueResult<int[]> nextIntMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    int[] value = _segmentColumnReader.getIntMV(_currentIndex);
    _currentIndex++;
    return MultiValueResult.of(value, null);
  }

  @Override
  public MultiValueResult<long[]> nextLongMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    long[] value = _segmentColumnReader.getLongMV(_currentIndex);
    _currentIndex++;
    return MultiValueResult.of(value, null);
  }

  @Override
  public MultiValueResult<float[]> nextFloatMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    float[] value = _segmentColumnReader.getFloatMV(_currentIndex);
    _currentIndex++;
    return MultiValueResult.of(value, null);
  }

  @Override
  public MultiValueResult<double[]> nextDoubleMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    double[] value = _segmentColumnReader.getDoubleMV(_currentIndex);
    _currentIndex++;
    return MultiValueResult.of(value, null);
  }

  @Override
  public String[] nextStringMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    String[] value = _segmentColumnReader.getStringMV(_currentIndex);
    _currentIndex++;
    return value;
  }

  @Override
  public byte[][] nextBytesMV() {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }
    byte[][] value = _segmentColumnReader.getBytesMV(_currentIndex);
    _currentIndex++;
    return value;
  }

  @Override
  public void rewind()
      throws IOException {
    _currentIndex = 0;
    _reuseValue = null;
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
    return _segmentColumnReader.isNull(docId);
  }

  // Single-value accessors

  @Override
  public int getInt(int docId) {
    return _segmentColumnReader.getInt(docId);
  }

  @Override
  public long getLong(int docId) {
    return _segmentColumnReader.getLong(docId);
  }

  @Override
  public float getFloat(int docId) {
    return _segmentColumnReader.getFloat(docId);
  }

  @Override
  public double getDouble(int docId) {
    return _segmentColumnReader.getDouble(docId);
  }

  @Override
  public String getString(int docId) {
    return _segmentColumnReader.getString(docId);
  }

  @Override
  public byte[] getBytes(int docId) {
    return _segmentColumnReader.getBytes(docId);
  }

  @Override
  public Object getValue(int docId)
      throws IOException {
    // Return null if the value is null and skipDefaultNullValues is true
    if (_skipDefaultNullValues && _segmentColumnReader.isNull(docId)) {
      return null;
    }
    // Return the segment value (which contains the default for null entries)
    return _segmentColumnReader.getValue(docId);
  }

  // Multi-value accessors

  @Override
  public MultiValueResult<int[]> getIntMV(int docId) {
    return MultiValueResult.of(_segmentColumnReader.getIntMV(docId), null);
  }

  @Override
  public MultiValueResult<long[]> getLongMV(int docId) {
    return MultiValueResult.of(_segmentColumnReader.getLongMV(docId), null);
  }

  @Override
  public MultiValueResult<float[]> getFloatMV(int docId) {
    return MultiValueResult.of(_segmentColumnReader.getFloatMV(docId), null);
  }

  @Override
  public MultiValueResult<double[]> getDoubleMV(int docId) {
    return MultiValueResult.of(_segmentColumnReader.getDoubleMV(docId), null);
  }

  @Override
  public String[] getStringMV(int docId) {
    return _segmentColumnReader.getStringMV(docId);
  }

  @Override
  public byte[][] getBytesMV(int docId) {
    return _segmentColumnReader.getBytesMV(docId);
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
  public void close()
      throws IOException {
    _segmentColumnReader.close();
  }
}
