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
import org.apache.pinot.spi.data.readers.ColumnReader;


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
    _segmentColumnReader = new PinotSegmentColumnReader(indexSegment, columnName);
    _columnName = columnName;
    _numDocs = indexSegment.getSegmentMetadata().getTotalDocs();
    _currentIndex = 0;
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

    // Return null if the value is null
    if (_segmentColumnReader.isNull(_currentIndex)) {
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
  public int getInt(int docId)
      throws IOException {
    return _segmentColumnReader.getInt(docId);
  }

  @Override
  public long getLong(int docId)
      throws IOException {
    return _segmentColumnReader.getLong(docId);
  }

  @Override
  public float getFloat(int docId)
      throws IOException {
    return _segmentColumnReader.getFloat(docId);
  }

  @Override
  public double getDouble(int docId)
      throws IOException {
    return _segmentColumnReader.getDouble(docId);
  }

  @Override
  public String getString(int docId)
      throws IOException {
    return _segmentColumnReader.getString(docId);
  }

  @Override
  public byte[] getBytes(int docId)
      throws IOException {
    return _segmentColumnReader.getBytes(docId);
  }

  // Multi-value accessors

  @Override
  public int[] getIntMV(int docId)
      throws IOException {
    return _segmentColumnReader.getIntMV(docId);
  }

  @Override
  public long[] getLongMV(int docId)
      throws IOException {
    return _segmentColumnReader.getLongMV(docId);
  }

  @Override
  public float[] getFloatMV(int docId)
      throws IOException {
    return _segmentColumnReader.getFloatMV(docId);
  }

  @Override
  public double[] getDoubleMV(int docId)
      throws IOException {
    return _segmentColumnReader.getDoubleMV(docId);
  }

  @Override
  public String[] getStringMV(int docId)
      throws IOException {
    return _segmentColumnReader.getStringMV(docId);
  }

  @Override
  public byte[][] getBytesMV(int docId)
      throws IOException {
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
