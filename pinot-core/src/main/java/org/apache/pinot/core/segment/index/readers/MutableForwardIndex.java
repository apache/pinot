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
package org.apache.pinot.core.segment.index.readers;

/**
 * Interface for mutable forward index (for CONSUMING segment).
 * NOTE: Mutable forward index does not use reader context to accelerate the reads.
 */
public interface MutableForwardIndex extends ForwardIndexReader<ForwardIndexReaderContext> {

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
   * DICTIONARY-ENCODED INDEX APIs
   */

  /**
   * Reads the dictionary id for a single-value column at the given document id.
   *
   * @param docId Document id
   * @return Dictionary id at the given document id
   */
  default int getDictId(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getDictId(int docId, ForwardIndexReaderContext context) {
    return getDictId(docId);
  }

  /**
   * Batch reads multiple dictionary ids for a single-value column at the given document ids into the passed in buffer
   * (the buffer size must be larger than or equal to the length).
   *
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param dictIdBuffer Dictionary id buffer
   */
  default void readDictIds(int[] docIds, int length, int[] dictIdBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    readDictIds(docIds, length, dictIdBuffer);
  }

  /**
   * Reads the dictionary ids for a multi-value column at the given document id into the passed in buffer (the buffer
   * size must be enough to hold all the values for the multi-value entry) and returns the number of values within the
   * multi-value entry.
   *
   * @param docId Document id
   * @param dictIdBuffer Dictionary id buffer
   * @return Number of values within the multi-value entry
   */
  default int getDictIdMV(int docId, int[] dictIdBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getDictIdMV(int docId, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    return getDictIdMV(docId, dictIdBuffer);
  }

  /**
   * Writes the dictionary id for a single-value column into the given document id.
   *
   * @param docId Document id
   * @param dictId Dictionary id to write
   */
  default void setDictId(int docId, int dictId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the dictionary ids for a multi-value column into the given document id.
   *
   * @param docId Document id
   * @param dictIds Dictionary ids to write
   */
  default void setDictIdMV(int docId, int[] dictIds) {
    throw new UnsupportedOperationException();
  }

  /**
   * SINGLE-VALUE COLUMN RAW INDEX APIs
   */

  /**
   * Reads the INT value at the given document id. The passed in reader context can be used to accelerate the reads.
   * <p>NOTE: Dictionary id is handled as INT type.
   *
   * @param docId Document id
   * @return INT type single-value at the given document id
   */
  default int getInt(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getInt(int docId, ForwardIndexReaderContext context) {
    return getInt(docId);
  }

  /**
   * Reads the LONG type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @return LONG type single-value at the given document id
   */
  default long getLong(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  default long getLong(int docId, ForwardIndexReaderContext context) {
    return getLong(docId);
  }

  /**
   * Reads the FLOAT type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @return FLOAT type single-value at the given document id
   */
  default float getFloat(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  default float getFloat(int docId, ForwardIndexReaderContext context) {
    return getFloat(docId);
  }

  /**
   * Reads the DOUBLE type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @return DOUBLE type single-value at the given document id
   */
  default double getDouble(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  default double getDouble(int docId, ForwardIndexReaderContext context) {
    return getDouble(docId);
  }

  /**
   * Reads the STRING type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @return STRING type single-value at the given document id
   */
  default String getString(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  default String getString(int docId, ForwardIndexReaderContext context) {
    return getString(docId);
  }

  /**
   * Reads the BYTES type single-value at the given document id. The passed in reader context can be used to accelerate
   * the reads.
   *
   * @param docId Document id
   * @return BYTES type single-value at the given document id
   */
  default byte[] getBytes(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  default byte[] getBytes(int docId, ForwardIndexReaderContext context) {
    return getBytes(docId);
  }

  /**
   * Writes the INT type single-value into the given document id.
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
   * @return Number of values within the multi-value entry
   */
  default int getIntMV(int docId, int[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getIntMV(int docId, int[] valueBuffer, ForwardIndexReaderContext context) {
    return getIntMV(docId, valueBuffer);
  }

  /**
   * Reads the LONG type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getLongMV(int docId, long[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getLongMV(int docId, long[] valueBuffer, ForwardIndexReaderContext context) {
    return getLongMV(docId, valueBuffer);
  }

  /**
   * Reads the FLOAT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getFloatMV(int docId, float[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getFloatMV(int docId, float[] valueBuffer, ForwardIndexReaderContext context) {
    return getFloatMV(docId, valueBuffer);
  }

  /**
   * Reads the DOUBLE type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getDoubleMV(int docId, double[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getDoubleMV(int docId, double[] valueBuffer, ForwardIndexReaderContext context) {
    return getDoubleMV(docId, valueBuffer);
  }

  /**
   * Reads the STRING type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @return Number of values within the multi-value entry
   */
  default int getStringMV(int docId, String[] valueBuffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  default int getStringMV(int docId, String[] valueBuffer, ForwardIndexReaderContext context) {
    return getStringMV(docId, valueBuffer);
  }

  /**
   * Writes the INT type multi-value into the given document id.
   *
   * @param docId Document id
   * @param values Values to write
   */
  default void setIntMV(int docId, int[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the LONG type multi-value into the given document id.
   *
   * @param docId Document id
   * @param values Values to write
   */
  default void setLongMV(int docId, long[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the FLOAT type multi-value into the given document id.
   *
   * @param docId Document id
   * @param values Values to write
   */
  default void setFloatMV(int docId, float[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the DOUBLE type multi-value into the given document id.
   *
   * @param docId Document id
   * @param values Values to write
   */
  default void setDoubleMV(int docId, double[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the STRING type multi-value into the given document id.
   *
   * @param docId Document id
   * @param values Values to write
   */
  default void setStringMV(int docId, String[] values) {
    throw new UnsupportedOperationException();
  }
}
