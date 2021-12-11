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
package org.apache.pinot.segment.spi.index.reader;

import java.io.Closeable;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for forward index reader.
 *
 * @param <T> Type of the ReaderContext
 */
public interface ForwardIndexReader<T extends ForwardIndexReaderContext> extends Closeable {

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
   * Creates a new {@link ForwardIndexReaderContext} of the reader which can be used to accelerate the reads.
   * NOTE: Caller is responsible for closing the returned reader context.
   */
  @Nullable
  default T createContext() {
    return null;
  }

  /**
   * DICTIONARY-ENCODED INDEX APIs
   */

  /**
   * Reads the dictionary id for a single-value column at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return Dictionary id at the given document id
   */
  default int getDictId(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Batch reads multiple dictionary ids for a single-value column at the given document ids into the passed in buffer
   * (the buffer size must be larger than or equal to the length).
   *
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param dictIdBuffer Dictionary id buffer
   * @param context Reader context
   */
  default void readDictIds(int[] docIds, int length, int[] dictIdBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the dictionary ids for a multi-value column at the given document id into the passed in buffer (the buffer
   * size must be enough to hold all the values for the multi-value entry) and returns the number of values within the
   * multi-value entry.
   *
   * @param docId Document id
   * @param dictIdBuffer Dictionary id buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getDictIdMV(int docId, int[] dictIdBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * SINGLE-VALUE COLUMN RAW INDEX APIs
   */

  /**
   * Reads the INT value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return INT type single-value at the given document id
   */
  default int getInt(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the LONG type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return LONG type single-value at the given document id
   */
  default long getLong(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the FLOAT type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return FLOAT type single-value at the given document id
   */
  default float getFloat(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the DOUBLE type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return DOUBLE type single-value at the given document id
   */
  default double getDouble(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the STRING type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return STRING type single-value at the given document id
   */
  default String getString(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the BYTES type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return BYTES type single-value at the given document id
   */
  default byte[] getBytes(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * MULTI-VALUE COLUMN RAW INDEX APIs
   * TODO: Not supported yet
   */

  /**
   * Reads the INT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getIntMV(int docId, int[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the LONG type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getLongMV(int docId, long[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the FLOAT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getFloatMV(int docId, float[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the DOUBLE type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getDoubleMV(int docId, double[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the STRING type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getStringMV(int docId, String[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the bytes type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getBytesMV(int docId, byte[][] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

}
