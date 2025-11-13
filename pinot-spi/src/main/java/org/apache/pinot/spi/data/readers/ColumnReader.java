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
 * <p>This interface provides 3 patterns optimised for different use cases
 * (Some implementations may not support all patterns):
 * <ul>
 *   <li>Sequential iteration over all values in a column using hasNext() and next() and rewind()</li>
 *   <li>Sequential iteration with type-specific methods (nextInt(), nextLong(), etc.)
 *        and null handling (isNextNull(), skipNext())</li>
 *   <li>Random access by document ID using getInt(docId), getLong(docId), etc. and isNull(docId) for null checks</li>
 * </ul>
 *
 * <p>Implementations should handle data type conversions, default values for new columns,
 * and efficient column-wise data access patterns.
 *
 * <h2>Usage Patterns</h2>
 *
 * <p>There are three primary patterns for reading data from a ColumnReader:
 *
 * <h3>Pattern 1: Sequential Iteration with Generic next() and Null Checks</h3>
 * <p>This pattern uses the generic {@link #next()} method which returns Object and may return null.
 * Suitable when you need to handle arbitrary data types or when null handling is done on the return value.
 *
 * <pre>{@code
 * // Read all values in the column
 * while (columnReader.hasNext()) {
 *   Object value = columnReader.next();
 *   if (value != null) {
 *     // Process non-null value
 *     processValue(value);
 *   } else {
 *     // Handle null value
 *     handleNullValue();
 *   }
 * }
 *
 * // Rewind to read the column again
 * columnReader.rewind();
 *
 * // Second pass through the data
 * while (columnReader.hasNext()) {
 *   Object value = columnReader.next();
 *   if (value != null) {
 *     processValueAgain(value);
 *   }
 * }
 * }</pre>
 *
 * <h3>Pattern 2: Sequential Iteration with Type-Specific Methods and Explicit Null Checks</h3>
 * <p>This pattern uses {@link #isNextNull()} to check for nulls before calling type-specific methods
 * like {@link #nextInt()}, {@link #nextLong()}, etc. Use {@link #skipNext()} to advance past null values.
 * This is the preferred pattern when you know the column data type and want to avoid boxing overhead.
 *
 * <pre>{@code
 * // Read all int values in the column, handling nulls
 * while (columnReader.hasNext()) {
 *   if (columnReader.isNextNull()) {
 *     // Skip the null value
 *     columnReader.skipNext();
 *     handleNullValue();
 *   } else {
 *     // Read the primitive int value (no boxing)
 *     int value = columnReader.nextInt();
 *     processIntValue(value);
 *   }
 * }
 *
 * // Rewind to read the column again
 * columnReader.rewind();
 *
 * // Second pass - maybe with different logic
 * while (columnReader.hasNext()) {
 *   if (!columnReader.isNextNull()) {
 *     int value = columnReader.nextInt();
 *     processIntValueAgain(value);
 *   } else {
 *     columnReader.skipNext();
 *   }
 * }
 * }</pre>
 *
 * <h3>Pattern 3: Random Access by Document ID</h3>
 * <p>This pattern uses {@link #getTotalDocs()} to get the total number of documents, then uses
 * document ID-based accessors like {@link #getInt(int)}, {@link #getLong(int)}, etc. to read
 * specific values. Use {@link #isNull(int)} to check if a value is null before reading.
 * This pattern is useful when you need random access or want to process documents in a specific order.
 *
 * <pre>{@code
 *
 * // Random access example - read specific document IDs
 * int[] docIdsToRead = {5, 10, 15, 20};
 * for (int docId : docIdsToRead) {
 *   if (!columnReader.isNull(docId)) {
 *     int value = columnReader.getInt(docId);
 *     processSpecificDoc(docId, value);
 *   }
 * }
 *
 * // Read in reverse order
 * // Get the total number of documents
 * int totalDocs = columnReader.getTotalDocs();
 * for (int docId = totalDocs - 1; docId >= 0; docId--) {
 *   if (!columnReader.isNull(docId)) {
 *     int value = columnReader.getInt(docId);
 *     processReverseOrder(docId, value);
 *   }
 * }
 * }</pre>
 *
 * <h3>Choosing the Right Pattern</h3>
 * <ul>
 *   <li><b>Pattern 1</b>: Use when dealing with generic Object types or when you don't know
 *       the column type at compile time. Less efficient due to boxing.</li>
 *   <li><b>Pattern 2</b>: Use when you know the column type and want efficient sequential iteration
 *       with primitive types. Preferred for most columnar segment building scenarios.</li>
 *   <li><b>Pattern 3</b>: Use when you need random access, want to process documents in a specific
 *       order, or need to access the same document multiple times.</li>
 * </ul>
 *
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
   * Check if the next value to be read is null.
   */
  boolean isNextNull() throws IOException;

  /**
   * Move the reader to skip the next value in the column.
   * This is typically called if isNextNull() returns true to skip the null value before calling nextInt(), etc
   *  which can't handle null values.
   *
   * @throws IOException If an I/O error occurs while reading
   */
  void skipNext() throws IOException;

  /**
   * Get the next int value in the column.
   * Should be called only if the column data type is INT and isNextNull() returns false.
   *
   * @return Next int value
   * @throws IOException If an I/O error occurs while reading
   */
  int nextInt() throws IOException;

  /**
   * Get the next long value in the column.
   * Should be called only if the column data type is LONG and isNextNull() returns false.
   *
   * @return Next long value
   * @throws IOException If an I/O error occurs while reading
   */
  long nextLong() throws IOException;

  /**
   * Get the next float value in the column.
   * Should be called only if the column data type is FLOAT and isNextNull() returns false.
   *
   * @return Next float value
   * @throws IOException If an I/O error occurs while reading
   */
  float nextFloat() throws IOException;

  /**
   * Get the next double value in the column.
   * Should be called only if the column data type is DOUBLE and isNextNull() returns false.
   *
   * @return Next double value
   * @throws IOException If an I/O error occurs while reading
   */
  double nextDouble() throws IOException;

  /**
   * Get the next int[] values in the column for multi-value columns.
   * Should be called only if the column is multi-value INT and isNextNull() returns false.
   *
   * @return Next int[] values
   * @throws IOException If an I/O error occurs while reading
   */
  int[] nextIntMV() throws IOException;

  /**
   * Get the next long[] values in the column for multi-value columns.
   * Should be called only if the column is multi-value LONG and isNextNull() returns false.
   *
   * @return Next long[] values
   * @throws IOException If an I/O error occurs while reading
   */
  long[] nextLongMV() throws IOException;

  /**
   * Get the next float[] values in the column for multi-value columns.
   * Should be called only if the column is multi-value FLOAT and isNextNull() returns false.
   *
   * @return Next float[] values
   * @throws IOException If an I/O error occurs while reading
   */
  float[] nextFloatMV() throws IOException;

  /**
   * Get the next double[] values in the column for multi-value columns.
   * Should be called only if the column is multi-value DOUBLE and isNextNull() returns false.
   *
   * @return Next double[] values
   * @throws IOException If an I/O error occurs while reading
   */
  double[] nextDoubleMV() throws IOException;

  /**
   * Get the next String[] values in the column for multi-value columns.
   * Should be called only if the column is multi-value STRING and isNextNull() returns false.
   *
   * @return Next String[] values
   * @throws IOException If an I/O error occurs while reading
   */
  String[] nextStringMV() throws IOException;

  /**
   * Get the next byte[][] values in the column for multi-value columns.
   * Should be called only if the column is multi-value BYTES and isNextNull() returns false.
   *
   * @return Next byte[][] values
   * @throws IOException If an I/O error occurs while reading
   */
  byte[][] nextBytesMV() throws IOException;

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
