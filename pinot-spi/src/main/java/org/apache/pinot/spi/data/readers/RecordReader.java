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
 * <p>
 * <h2>Exception Handling</h2>
 * <p>When implementing {@link #next(GenericRow)}, implementations should use appropriate exception types to indicate
 * the nature of the error:
 * <ul>
 *   <li><b>{@link RecordFetchException}</b>: Throw when encountering I/O or data fetch errors that may prevent
 *       the reader from advancing its pointer to the next record. These errors count toward the consecutive failure
 *       threshold to prevent infinite loops.</li>
 *   <li><b>{@link IOException}</b>: For backward compatibility, IOException is treated as a parse error and can be
 *       skipped indefinitely.</li>
 * </ul>
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
   * <p>This method should not throw exception. Caller is not responsible for handling exceptions from this method.
   */
  boolean hasNext();

  /**
   * Get the next record.
   * <p>This method should be called only if {@link #hasNext()} returns <code>true</code>. Caller is responsible for
   * handling exceptions from this method and skip the row if user wants to continue reading the remaining rows.
   * <p>
   * <b>Exception Guidelines:</b>
   * <ul>
   *   <li>Throw {@link RecordFetchException} if the reader cannot advance due to I/O or fetch errors</li>
   *   <li>Throw {@link IOException} if data is corrupt but the reader can advance to the next record</li>
   * </ul>
   * <b>Important:</b> If an exception is thrown, the implementation should ensure that either:
   * <ul>
   *   <li>The reader's pointer advances to the next record (for parse errors), OR</li>
   *   <li>The reader throws RecordFetchException to indicate it cannot advance (for fetch errors)</li>
   * </ul>
   * This prevents infinite loops when the same record is repeatedly attempted.
   */
  default GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  /**
   * Get the next record. Re-use the given row to reduce garbage.
   * <p>The passed in row should be cleared before calling this method.
   * <p>This method should be called only if {@link #hasNext()} returns <code>true</code>. Caller is responsible for
   * handling exceptions from this method and skip the row if user wants to continue reading the remaining rows.
   * <p>
   * <b>Exception Guidelines:</b>
   * <ul>
   *   <li>Throw {@link RecordFetchException} if the reader cannot advance due to I/O or fetch errors (e.g., network
   *       failure, file system error, stream corruption that prevents reading). These errors count toward the
   *       consecutive failure threshold.</li>
   *   <li>Throw {@link IOException} if data is corrupt or malformed but the reader can advance to the next
   *       record (e.g., invalid format, type conversion failure, schema mismatch).</li>
   * </ul>
   * <b>Important:</b> If an exception is thrown, the implementation should ensure that either:
   * <ul>
   *   <li>The reader's pointer advances to the next record (for parse errors), OR</li>
   *   <li>The reader throws RecordFetchException to indicate it cannot advance (for fetch errors)</li>
   * </ul>
   * This prevents infinite loops when the same record is repeatedly attempted.<br>
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
