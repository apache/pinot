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
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * The <code>RecordReader</code> interface is used to read records from various file formats into {@link GenericRow}s.
 * Pinot segments will be generated from {@link GenericRow}s.
 * <p>NOTE: for time column, record reader should be able to read both incoming and outgoing time
 */
public interface RecordReader extends Closeable, Serializable {

  /**
   * Initializes the record reader with data file, schema and (optional) record reader config.
   *
   * @param dataFile Data file
   * @param fieldsToRead The fields to read from the data file. If null or empty, reads all fields
   * @param recordReaderConfig Config for the reader specific to the format. e.g. delimiter for csv format etc
   * @throws IOException If an I/O error occurs
   */
  void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException;

  /**
   * Return <code>true</code> if more records remain to be read.
   */
  boolean hasNext();

  /**
   * Get the next record.
   *
   * TODO: Add default implementation because all the override implementations are the same
   */
  GenericRow next()
      throws IOException;

  /**
   * Get the next record. Re-use the given row to reduce garbage.
   * <p>The passed in row should be cleared before calling this method.
   *
   * TODO: Consider clearing the row within the record reader to simplify the caller
   */
  GenericRow next(GenericRow reuse)
      throws IOException;

  /**
   * Rewind the reader to start reading from the first record again.
   */
  void rewind()
      throws IOException;
}
