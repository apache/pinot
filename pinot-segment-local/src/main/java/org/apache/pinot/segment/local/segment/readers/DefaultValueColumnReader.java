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

    // For multi-value fields, wrap the default value in an array
    Object defaultNullValue = fieldSpec.getDefaultNullValue();
    if (fieldSpec.isSingleValueField()) {
      _defaultValue = defaultNullValue;
    } else {
      _defaultValue = new Object[]{defaultNullValue};
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
    _currentIndex++;
    return _defaultValue;
  }


  @Override
  public void rewind() throws IOException {
    _currentIndex = 0;
  }

  @Override
  public String getColumnName() {
    return _columnName;
  }

  @Override
  public void close() throws IOException {
    // No resources to close
  }
}
