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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ColumnReaderFactory implementation for immutable Pinot segments.
 *
 * <p>This factory creates ColumnReader instances for reading data from Pinot segments
 * in a columnar fashion. It handles:
 * <ul>
 *   <li>Creating readers for existing columns in the segment</li>
 *   <li>Creating default value readers for new columns</li>
 *   <li>Resource management for all created readers</li>
 * </ul>
 */
public class PinotSegmentColumnReaderFactory implements ColumnReaderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentColumnReaderFactory.class);

  private final IndexSegment _indexSegment;
  private Schema _targetSchema;
  @Nullable private Map<String, ColumnReader> _columnReaders;

  /**
   * Create a PinotSegmentColumnReaderFactory.
   *
   * @param indexSegment Source segment to read from
   */
  public PinotSegmentColumnReaderFactory(IndexSegment indexSegment) {
    _indexSegment = indexSegment;
    _columnReaders = new HashMap<>();
  }

  @Override
  public void init(Schema targetSchema)
      throws IOException {
    _targetSchema = targetSchema;
    _columnReaders = initializeAllColumnReaders();
    LOGGER.info("Initialized PinotSegmentColumnReaderFactory with target schema containing {} columns",
        targetSchema.getPhysicalColumnNames().size());
  }

  @Override
  public Set<String> getAvailableColumns() {
    return _indexSegment.getPhysicalColumnNames();
  }

  /**
   * Get the total number of documents/rows in the data source.
   *
   * @return Total number of documents
   */
  public int getNumDocs() {
    return _indexSegment.getSegmentMetadata().getTotalDocs();
  }

  @Override
  public ColumnReader getColumnReader(String columnName)
      throws IOException {
    if (_targetSchema == null) {
      throw new IllegalStateException("Factory not initialized. Call init() first.");
    }

    ColumnReader reader = _columnReaders.get(columnName);
    if (reader == null) {
      throw new IOException("Column reader not found for column: " + columnName);
    }
    return reader;
  }

  /**
   * Internal method to create a column reader for the specified column.
   * This method is called during initialization to create all readers.
   */
  private ColumnReader createColumnReader(String columnName, FieldSpec targetFieldSpec) {
    if (targetFieldSpec.isVirtualColumn()) {
      throw new IllegalStateException("Target field spec is a virtual column.");
    }

    ColumnReader columnReader;

    if (hasColumn(columnName)) {
      // Column exists in source segment - create a segment column reader
      LOGGER.debug("Creating segment column reader for existing column: {}", columnName);
      columnReader = new PinotSegmentColumnReaderImpl(_indexSegment, columnName);
    } else {
      // New column - create a default value reader
      LOGGER.debug("Creating default value reader for new column: {}", columnName);
      columnReader = new DefaultValueColumnReader(columnName, getNumDocs(), targetFieldSpec);
    }

    return columnReader;
  }

  @Override
  public Map<String, ColumnReader> getAllColumnReaders()
      throws IOException {
    return _columnReaders;
  }

  /**
   * Internal method to initialize all column readers during factory initialization.
   */
  private Map<String, ColumnReader> initializeAllColumnReaders()
      throws IOException {
    if (_targetSchema == null) {
      throw new IllegalStateException("Factory not initialized. Call init() first.");
    }

    Map<String, ColumnReader> allReaders = new HashMap<>();

    // Create readers for all columns in the target schema
    for (FieldSpec fieldSpec : _targetSchema.getAllFieldSpecs()) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      ColumnReader reader = createColumnReader(columnName, fieldSpec);
      allReaders.put(columnName, reader);
    }

    return allReaders;
  }

  /**
   * Check if the specified column exists in the source data.
   *
   * @param columnName Column name to check
   * @return true if the column exists in source, false otherwise
   */
  public boolean hasColumn(String columnName) {
    return _indexSegment.getPhysicalColumnNames().contains(columnName);
  }

  @Override
  public void close()
      throws IOException {
    LOGGER.debug("Closing PinotSegmentColumnReaderFactory and {} column readers", _columnReaders.size());

    // Close all created column readers
    for (ColumnReader reader : _columnReaders.values()) {
      try {
        reader.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing column reader for column: {}", reader.getColumnName(), e);
      }
    }

    _columnReaders.clear();
  }
}
