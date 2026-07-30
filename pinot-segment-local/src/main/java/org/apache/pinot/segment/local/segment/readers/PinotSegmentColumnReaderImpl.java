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
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.apache.pinot.spi.utils.PinotDataType;


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
  private final DataType _dataType;
  private final boolean _skipDefaultNullValues;

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
      int numDocs, DataType dataType, boolean skipDefaultNullValues) {
    _segmentColumnReader = segmentColumnReader;
    _columnName = columnName;
    _numDocs = numDocs;
    _dataType = dataType;
    _skipDefaultNullValues = skipDefaultNullValues;
  }

  @Override
  public String getColumnName() {
    return _columnName;
  }

  @Nullable
  @Override
  public PinotDataType getValueType() {
    return ColumnReader.toValueType(_dataType, _segmentColumnReader.isSingleValue());
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

  @Override
  public Object getValue(int docId)
      throws IOException {
    // Return null if the value is null and skipDefaultNullValues is true
    if (_skipDefaultNullValues && _segmentColumnReader.isNull(docId)) {
      return null;
    }
    // The segment stores physical values (e.g. int for BOOLEAN, long for TIMESTAMP); surface the logical object per
    // the getValue() contract. Null entries return the segment's stored default, also converted to the logical type.
    return ColumnReader.toLogicalValue(_segmentColumnReader.getValue(docId), _dataType);
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
  public BigDecimal getBigDecimal(int docId) {
    return _segmentColumnReader.getBigDecimal(docId);
  }

  @Override
  public String getString(int docId) {
    return _segmentColumnReader.getString(docId);
  }

  @Override
  public byte[] getBytes(int docId) {
    return _segmentColumnReader.getBytes(docId);
  }

  // Multi-value accessors

  // For all multi-value primitive type methods (getIntMV, getLongMV, getFloatMV, getDoubleMV), we pass null for
  // the validity bitset since multi-value primitive types cannot have null elements. Nulls are removed by
  // NullValueTransformer
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
  public BigDecimal[] getBigDecimalMV(int docId) {
    return _segmentColumnReader.getBigDecimalMV(docId);
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
