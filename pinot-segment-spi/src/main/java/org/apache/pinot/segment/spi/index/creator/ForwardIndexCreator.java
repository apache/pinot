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
package org.apache.pinot.segment.spi.index.creator;

import java.io.Closeable;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for forward index creator.
 */
public interface ForwardIndexCreator extends Closeable {

  /**
   * Returns {@code true} if the forward index is dictionary-encoded, {@code false} if it is raw.
   */
  boolean isDictionaryEncoded();

  /**
   * Returns {@code true} if the forward index is for a single-value column, {@code false} if it is for a multi-value
   * column.
   */
  boolean isSingleValue();

  /**
   * Returns the data type of the values in the forward index. Returns {@link DataType#INT} for dictionary-encoded
   * forward index.
   */
  DataType getValueType();

  /**
   * DICTIONARY-ENCODED INDEX APIs
   */

  /**
   * Writes the dictionary id for the next single-value into the forward index.
   *
   * @param dictId Document id to write
   */
  default void putDictId(int dictId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the dictionary ids for the next multi-value into the forward index.
   *
   * @param dictIds Document ids to write
   */
  default void putDictIdMV(int[] dictIds) {
    throw new UnsupportedOperationException();
  }

  /**
   * SINGLE-VALUE COLUMN RAW INDEX APIs
   */

  /**
   * Writes the next INT type single-value into the forward index.
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
   * MULTI-VALUE COLUMN RAW INDEX APIs
   * TODO: Not supported yet
   */

  /**
   * Writes the next INT type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putIntMV(int[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next LONG type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putLongMV(long[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next FLOAT type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putFloatMV(float[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next DOUBLE type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putDoubleMV(double[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next STRING type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putStringMV(String[] values) {
    throw new UnsupportedOperationException();
  }
}
