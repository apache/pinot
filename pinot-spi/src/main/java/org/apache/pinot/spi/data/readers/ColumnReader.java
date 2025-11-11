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
import javax.annotation.Nullable;


/**
 * The <code>ColumnReader</code> interface is used to read column data from various data sources
 * for columnar segment building. Unlike RecordReader which reads row-by-row, ColumnReader provides
 * column-wise access to data, enabling efficient columnar segment creation.
 *
 * <p>This interface follows an iterator pattern similar to RecordReader:
 * <ul>
 *   <li>Sequential iteration over all values in a column using hasNext() and next()</li>
 *   <li>Rewind capability to restart iteration</li>
 *   <li>Resource cleanup</li>
 * </ul>
 *
 * <p>Implementations should handle data type conversions, default values for new columns,
 * and efficient column-wise data access patterns.
 */
public interface ColumnReader extends Closeable, Serializable {

  /**
   * Return <code>true</code> if more values remain to be read in this column.
   * <p>This method should not throw exception. Caller is not responsible for handling exceptions from this method.
   */
  boolean hasNext();

  /**
   * Get the next value in the column.
   * <p>This method should be called only if {@link #hasNext()} returns <code>true</code>. Caller is responsible for
   * handling exceptions from this method and skip the value if user wants to continue reading the remaining values.
   *
   * @return Next column value, or null if the value is null
   * @throws IOException If an I/O error occurs while reading
   */
  @Nullable
  Object next()
      throws IOException;

  /**
   * Rewind the reader to start reading from the first value again.
   *
   * @throws IOException If an I/O error occurs while rewinding
   */
  void rewind()
      throws IOException;

  /**
   * Get the name of the column.
   *
   * @return Column name
   */
  String getColumnName();

  /**
   * Get the total number of documents in this column.
   *
   * @return Total number of documents
   */
  int getTotalDocs();

  /**
   * Check if the value at the given document ID is null.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return true if the value is null, false otherwise
   * @throws IndexOutOfBoundsException If docId is out of range
   */
  boolean isNull(int docId);

  // Single-value accessors

  /**
   * Get int value at the given document ID for single-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return int value at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  int getInt(int docId)
      throws IOException;

  /**
   * Get long value at the given document ID for single-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return long value at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  long getLong(int docId)
      throws IOException;

  /**
   * Get float value at the given document ID for single-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return float value at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  float getFloat(int docId)
      throws IOException;

  /**
   * Get double value at the given document ID for single-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return double value at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  double getDouble(int docId)
      throws IOException;

  /**
   * Get String value at the given document ID for single-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return String value at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  String getString(int docId)
      throws IOException;

  /**
   * Get byte[] value at the given document ID for single-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return byte[] value at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  byte[] getBytes(int docId)
      throws IOException;

  // Multi-value accessors

  /**
   * Get int[] values at the given document ID for multi-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return int[] values at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  int[] getIntMV(int docId)
      throws IOException;

  /**
   * Get long[] values at the given document ID for multi-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return long[] values at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  long[] getLongMV(int docId)
      throws IOException;

  /**
   * Get float[] values at the given document ID for multi-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return float[] values at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  float[] getFloatMV(int docId)
      throws IOException;

  /**
   * Get double[] values at the given document ID for multi-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return double[] values at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  double[] getDoubleMV(int docId)
      throws IOException;

  /**
   * Get String[] values at the given document ID for multi-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return String[] values at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  String[] getStringMV(int docId)
      throws IOException;

  /**
   * Get byte[][] values at the given document ID for multi-value columns.
   * <p>Document ID is 0-based. Valid values are 0 to {@link #getTotalDocs()} - 1.
   *
   * @param docId Document ID (0-based)
   * @return byte[][] values at the document ID
   * @throws IndexOutOfBoundsException If docId is out of range
   * @throws IOException If an I/O error occurs while reading
   */
  byte[][] getBytesMV(int docId)
      throws IOException;
}
