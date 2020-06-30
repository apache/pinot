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
package org.apache.pinot.core.io.readerwriter;

import org.apache.pinot.core.io.reader.ForwardIndexReader;
import org.apache.pinot.core.io.reader.ReaderContext;


/**
 * Interface for forward index reader-writer (for CONSUMING segment).
 */
public interface ForwardIndexReaderWriter extends ForwardIndexReader<ReaderContext> {

  /**
   * Returns the length (size in bytes) of the shortest elements inside the forward index.
   *
   * @return The length (size in bytes) of the shortest elements inside the forward index.
   */
  int getLengthOfShortestElement();

  /**
   * Returns the length (size in bytes) of the longest elements inside the forward index.
   *
   * @return The length (size in bytes) of the longest elements inside the forward index.
   */
  int getLengthOfLongestElement();

  /**
   * SINGLE-VALUE COLUMN APIs
   */

  /**
   * Writes the INT type single-value into the given document id.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docId Document id
   * @param value Value to write
   */
  default void setInt(int docId, int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the LONG type single-value into the given document id.
   *
   * @param docId Document id
   * @param value Value to write
   */
  default void setLong(int docId, long value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the FLOAT type single-value into the given document id.
   *
   * @param docId Document id
   * @param value Value to write
   */
  default void setFloat(int docId, float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the DOUBLE type single-value into the given document id.
   *
   * @param docId Document id
   * @param value Value to write
   */
  default void setDouble(int docId, double value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the STRING type single-value into the given document id.
   *
   * @param docId Document id
   * @param value Value to write
   */
  default void setString(int docId, String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the BYTES type single-value into the given document id.
   *
   * @param docId Document id
   * @param value Value to write
   */
  default void setBytes(int docId, byte[] value) {
    throw new UnsupportedOperationException();
  }

  /**
   * MULTI-VALUE COLUMN APIs
   */

  /**
   * Writes the INT type multi-value from the given int array into the given document id.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docId Document id
   * @param intArray Array containing the values to write
   */
  default void setIntArray(int docId, int[] intArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the LONG type multi-value from the given long array into the given document id.
   *
   * @param docId Document id
   * @param longArray Array containing the values to write
   */
  default void setLongArray(int docId, long[] longArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the FLOAT type multi-value from the given float array into the given document id.
   *
   * @param docId Document id
   * @param floatArray Array containing the values to write
   */
  default void setFloatArray(int docId, float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the DOUBLE type multi-value from the given double array into the given document id.
   *
   * @param docId Document id
   * @param doubleArray Array containing the values to write
   */
  default void setDoubleArray(int docId, double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the STRING type multi-value from the given String array into the given document id.
   *
   * @param docId Document id
   * @param stringArray Array containing the values to write
   */
  default void setStringArray(int docId, String[] stringArray) {
    throw new UnsupportedOperationException();
  }
}
