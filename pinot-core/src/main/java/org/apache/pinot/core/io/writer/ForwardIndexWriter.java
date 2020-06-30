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
package org.apache.pinot.core.io.writer;

import java.io.Closeable;


/**
 * Interface for forward index writer.
 * <p>TODO: Remove this interface and merge the writer implementations into the ForwardIndexCreator implementations.
 */
public interface ForwardIndexWriter extends Closeable {

  /**
   * SINGLE-VALUE COLUMN APIs
   */

  /**
   * Writes the next INT type single-value into the forward index.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param value Value to write
   */
  default void putInt(int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next LONG type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putLong(long value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next FLOAT type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putFloat(float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next DOUBLE type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putDouble(double value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next STRING type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putString(String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next BYTES type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putBytes(byte[] value) {
    throw new UnsupportedOperationException();
  }

  /**
   * MULTI-VALUE COLUMN APIs
   */

  /**
   * Writes the next INT type multi-value from the given int array into the forward index.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param intArray Array containing the values to write
   */
  default void putIntArray(int[] intArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next LONG type multi-value from the given long array into the forward index.
   *
   * @param longArray Array containing the values to write
   */
  default void putLongArray(long[] longArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next FLOAT type multi-value from the given float array into the forward index.
   *
   * @param floatArray Array containing the values to write
   */
  default void putFloatArray(float[] floatArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next DOUBLE type multi-value from the given double array into the forward index.
   *
   * @param doubleArray Array containing the values to write
   */
  default void putDoubleArray(double[] doubleArray) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next STRING type multi-value from the given String array into the forward index.
   *
   * @param stringArray Array containing the values to write
   */
  default void putStringArray(String[] stringArray) {
    throw new UnsupportedOperationException();
  }
}
