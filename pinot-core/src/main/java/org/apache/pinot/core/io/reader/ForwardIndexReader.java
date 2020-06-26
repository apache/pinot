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
package org.apache.pinot.core.io.reader;

import java.io.Closeable;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for forward index reader.
 *
 * @param <T> Type of the ReaderContext
 */
public interface ForwardIndexReader<T extends ReaderContext> extends Closeable {

  /**
   * Returns the data type of the values in the forward index.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  DataType getValueType();

  /**
   * Returns {@code true} if the forward index is for a single-value column, {@code false} otherwise.
   */
  boolean isSingleValue();

  /**
   * Creates a new {@link ReaderContext} of the reader which can be used to accelerate the reads.
   * <p>TODO: Figure out a way to close the reader context. Currently it can cause direct memory leak.
   */
  @Nullable
  default T createContext() {
    return null;
  }

  /**
   * SINGLE-VALUE COLUMN APIs
   */

  /**
   * Reads the INT value at the given document id.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docId Document id
   * @return INT type single-value at the given document id
   */
  default int getInt(int docId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the INT value at the given document id. The passed in reader context can be used to accelerate the reads.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docId Document id
   * @param context Reader context
   * @return INT type single-value at the given document id
   */
  default int getInt(int docId, T context) {
    return getInt(docId);
  }

  /**
   * Reads the LONG type single-value at the given document id.
   *
   * @param docId Document id
   * @return LONG type single-value at the given document id
   */
  default long getLong(int docId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the LONG type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @param context Reader context
   * @return LONG type single-value at the given document id
   */
  default long getLong(int docId, T context) {
    return getLong(docId);
  }

  /**
   * Reads the FLOAT type single-value at the given document id.
   *
   * @param docId Document id
   * @return FLOAT type single-value at the given document id
   */
  default float getFloat(int docId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the FLOAT type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @param context Reader context
   * @return FLOAT type single-value at the given document id
   */
  default float getFloat(int docId, T context) {
    return getFloat(docId);
  }

  /**
   * Reads the DOUBLE type single-value at the given document id.
   *
   * @param docId Document id
   * @return DOUBLE type single-value at the given document id
   */
  default double getDouble(int docId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the DOUBLE type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @param context Reader context
   * @return DOUBLE type single-value at the given document id
   */
  default double getDouble(int docId, T context) {
    return getDouble(docId);
  }

  /**
   * Reads the STRING type single-value at the given document id.
   *
   * @param docId Document id
   * @return STRING type single-value at the given document id
   */
  default String getString(int docId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the STRING type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @param context Reader context
   * @return STRING type single-value at the given document id
   */
  default String getString(int docId, T context) {
    return getString(docId);
  }

  /**
   * Reads the BYTES type single-value at the given document id.
   *
   * @param docId Document id
   * @return BYTES type single-value at the given document id
   */
  default byte[] getBytes(int docId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the BYTES type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @param context Reader context
   * @return BYTES type single-value at the given document id
   */
  default byte[] getBytes(int docId, T context) {
    return getBytes(docId);
  }

  /**
   * Batch reads multiple INT type single-values at the given document ids into the passed in value buffer (buffer size
   * must be larger than or equal to the length).
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param values Value buffer
   */
  default void readValues(int[] docIds, int length, int[] values) {
    for (int i = 0; i < length; i++) {
      values[i] = getInt(docIds[i]);
    }
  }

  /**
   * Batch reads multiple INT type single-values at the given document ids into the passed in value buffer (buffer size
   * must be larger than or equal to the length). The passed in reader context can be used to accelerate the reads.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param values Value buffer
   * @param context Reader context
   */
  default void readValues(int[] docIds, int length, int[] values, T context) {
    readValues(docIds, length, values);
  }

  /**
   * MULTI-VALUE COLUMN APIs
   */

  /**
   * Reads the INT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docId Document id
   * @param intArray Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getIntArray(int docId, int[] intArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the INT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry. The passed in reader context can be used to accelerate the reads.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docId Document id
   * @param intArray Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getIntArray(int docId, int[] intArray, T context) {
    return getIntArray(docId, intArray);
  }

  /**
   * Reads the LONG type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param longArray Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getLongArray(int docId, long[] longArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the FLOAT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param floatArray Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getFloatArray(int docId, float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the DOUBLE type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param doubleArray Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getDoubleArray(int docId, double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the STRING type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param stringArray Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getStringArray(int docId, String[] stringArray) {
    throw new UnsupportedOperationException();
  }
}
