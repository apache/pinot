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
package org.apache.pinot.spi.data.readers;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Factory interface for creating ColumnReader instances for different data sources.
 *
 * <p>This factory provides a generic way to create column readers for various data sources
 * such as Pinot segments, Parquet files, etc. The factory handles:
 * <ul>
 *   <li>Creating column readers for existing columns</li>
 *   <li>Creating default value readers for new columns</li>
 *   <li>Data type conversions between source and target schemas</li>
 *   <li>Resource management</li>
 * </ul>
 */
public interface ColumnReaderFactory extends Closeable, Serializable {

  /**
   * Initialize the factory with the data source and target schema.
   *
   * @param targetSchema Target schema for the output segment
   * @throws IOException If initialization fails
   */
  void init(Schema targetSchema) throws IOException;

  /**
   * Get the set of column names available in the source data.
   *
   * @return Set of available column names in the source
   */
  Set<String> getAvailableColumns();

  /**
   * Get the total number of documents/rows in the data source.
   *
   * @return Total number of documents
   */
  int getNumDocs();

  /**
   * Create a column reader for the specified column.
   *
   * @param columnName Name of the column to read
   * @param targetFieldSpec Target field specification from the output schema
   * @return ColumnReader instance for the specified column
   * @throws IOException If the column reader cannot be created
   */
  ColumnReader createColumnReader(String columnName, FieldSpec targetFieldSpec) throws IOException;

  /**
   * Get all column readers for the target schema.
   * This is a convenience method that creates readers for all columns in the target schema.
   *
   * @return Map of column name to ColumnReader
   * @throws IOException If any column reader cannot be created
   */
  Map<String, ColumnReader> getAllColumnReaders() throws IOException;

  /**
   * Check if the specified column exists in the source data.
   *
   * @param columnName Column name to check
   * @return true if the column exists in source, false otherwise
   */
  boolean hasColumn(String columnName);
}
