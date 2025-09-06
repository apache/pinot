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
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.recordtransformer.DataTypeTransformer;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of ColumnReader for Pinot segments.
 *
 * <p>This class wraps the existing PinotSegmentColumnReader and provides the ColumnReader interface
 * for columnar segment building. It handles:
 * <ul>
 *   <li>Reading column values from Pinot segments</li>
 *   <li>Data type conversions when target schema differs from source</li>
 *   <li>Null value detection</li>
 *   <li>Resource cleanup</li>
 * </ul>
 */
public class PinotSegmentColumnReaderImpl implements ColumnReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentColumnReaderImpl.class);

  private final PinotSegmentColumnReader _segmentColumnReader;
  private final String _columnName;
  private final int _numDocs;
  private final boolean _needsConversion;
  private final FieldSpec _targetFieldSpec;

  private int _currentIndex;
  private Object _currentValue;
  private boolean _currentIsNull;

  /**
   * Create a PinotSegmentColumnReaderImpl for an existing column in the segment.
   *
   * @param indexSegment Source segment to read from
   * @param columnName Name of the column
   * @param targetFieldSpec Target field specification (may differ from source for data type conversion)
   */
  public PinotSegmentColumnReaderImpl(IndexSegment indexSegment, String columnName, FieldSpec targetFieldSpec) {
    _segmentColumnReader = new PinotSegmentColumnReader(indexSegment, columnName);
    _columnName = columnName;
    _numDocs = indexSegment.getSegmentMetadata().getTotalDocs();
    _targetFieldSpec = targetFieldSpec;
    _currentIndex = 0;

    // Check if data type conversion is needed
    FieldSpec sourceFieldSpec = indexSegment.getSegmentMetadata().getSchema().getFieldSpecFor(columnName);
    _needsConversion = sourceFieldSpec != null
        && !sourceFieldSpec.getDataType().equals(targetFieldSpec.getDataType());

    if (_needsConversion) {
      LOGGER.debug("Data type conversion needed for column: {} from {} to {}",
          columnName, sourceFieldSpec.getDataType(), targetFieldSpec.getDataType());
    }
  }

  @Override
  public boolean hasNext() {
    return _currentIndex < _numDocs;
  }

  @Override
  @Nullable
  public Object next() throws IOException {
    if (!hasNext()) {
      throw new IllegalStateException("No more values available");
    }

    Object value = _segmentColumnReader.getValue(_currentIndex);
    _currentIsNull = _segmentColumnReader.isNull(_currentIndex);
    _currentIndex++;

    if (value == null) {
      _currentValue = null;
      return null;
    }

    // Apply data type conversion if needed
    if (_needsConversion) {
      _currentValue = convertValueToTargetDataType(value);
    } else {
      _currentValue = value;
    }

    return _currentValue;
  }

  @Override
  public boolean isNull() {
    return _currentIsNull;
  }

  @Override
  public void rewind() throws IOException {
    _currentIndex = 0;
    _currentValue = null;
    _currentIsNull = false;
  }

  @Override
  public String getColumnName() {
    return _columnName;
  }

  /**
   * Convert value from source segment data type to target schema data type using DataTypeTransformer's
   * conversion logic. This ensures consistency with the standard data transformation pipeline.
   */
  private Object convertValueToTargetDataType(Object value) {
    if (value == null) {
      return null;
    }
    PinotDataType targetType = PinotDataType.getPinotDataTypeForIngestion(_targetFieldSpec);
    return DataTypeTransformer.convertValue(value, targetType, _columnName);
  }

  @Override
  public void close() throws IOException {
    _segmentColumnReader.close();
  }
}
