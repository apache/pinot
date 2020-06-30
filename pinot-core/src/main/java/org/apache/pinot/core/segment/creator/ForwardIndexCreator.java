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
package org.apache.pinot.core.segment.creator;

import java.io.Closeable;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Interface for forward index creator.
 */
public interface ForwardIndexCreator extends Closeable {

  /**
   * Returns the data type of the values in the forward index.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  FieldSpec.DataType getValueType();

  /**
   * Returns {@code true} if the forward index is for a single-value column, {@code false} otherwise.
   */
  boolean isSingleValue();

  /**
   * SINGLE-VALUE COLUMN APIs
   */

  /**
   * Indexes the next INT type single-value into the forward index.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param value Value to index
   */
  default void index(int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Indexes the next LONG type single-value into the forward index.
   *
   * @param value Value to index
   */
  default void index(long value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Indexes the next FLOAT type single-value into the forward index.
   *
   * @param value Value to index
   */
  default void index(float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Indexes the next DOUBLE type single-value into the forward index.
   *
   * @param value Value to index
   */
  default void index(double value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Indexes the next STRING type single-value into the forward index.
   *
   * @param value Value to index
   */
  default void index(String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Indexes the next BYTES type single-value into the forward index.
   *
   * @param value Value to index
   */
  default void index(byte[] value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Indexes the next raw single-value (not dictionary id) into the forward index. The given value should be of the
   * forward index value type.
   *
   * @param value Value to index
   */
  default void index(Object value) {
    throw new UnsupportedOperationException();
  }

  /**
   * MULTI-VALUE COLUMN APIs
   */

  /**
   * Indexes the next INT type multi-value from the given int array into the forward index.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param intArray Array containing the values to index
   */
  default void index(int[] intArray) {
    throw new UnsupportedOperationException();
  }
}
