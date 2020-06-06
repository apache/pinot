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
package org.apache.pinot.core.common;

import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * TODO: Split BlockValSet into multiple interfaces. The docId based APIs do not apply to Block concept.
 */
public interface BlockValSet {

  DataType getValueType();

  boolean isSingleValue();

  /**
   * DOCUMENT ID BASED APIs
   */

  /**
   * NOTE: The following single value read APIs do not handle the data type conversion for performance concern. Caller
   *       should always call the API that matches the data type of the {@code BlockValSet}.
   */

  /**
   * Returns the INT type single-value at the given document id.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  int getIntValue(int docId);

  /**
   * Returns the LONG type single-value at the given document id.
   */
  long getLongValue(int docId);

  /**
   * Returns the FLOAT type single-value at the given document id.
   */
  float getFloatValue(int docId);

  /**
   * Returns the DOUBLE type single-value at the given document id.
   */
  double getDoubleValue(int docId);

  /**
   * Returns the STRING type single-value at the given document id.
   */
  String getStringValue(int docId);

  /**
   * Returns the BYTES type single-value at the given document id.
   */
  byte[] getBytesValue(int docId);

  /**
   * Reads the INT type multi-value at the given document id into the value buffer and returns the number of values in
   * the multi-value entry.
   * <p>The passed in value buffer should be large enough to hold all the values of a multi-value entry.
   * <p>NOTE: Dictionary id is handled as INT type.
   */
  int getIntValues(int docId, int[] valueBuffer);

  /**
   * Reads the LONG type multi-value at the given document id into the value buffer and returns the number of values in
   * the multi-value entry.
   * <p>The passed in value buffer should be large enough to hold all the values of a multi-value entry.
   */
  int getLongValues(int docId, long[] valueBuffer);

  /**
   * Reads the FLOAT type multi-value at the given document id into the value buffer and returns the number of values
   * in the multi-value entry.
   * <p>The passed in value buffer should be large enough to hold all the values of a multi-value entry.
   */
  int getFloatValues(int docId, float[] valueBuffer);

  /**
   * Reads the DOUBLE type multi-value at the given document id into the value buffer and returns the number of values
   * in the multi-value entry.
   * <p>The passed in value buffer should be large enough to hold all the values of a multi-value entry.
   */
  int getDoubleValues(int docId, double[] valueBuffer);

  /**
   * Reads the STRING type multi-value at the given document id into the value buffer and returns the number of values
   * in the multi-value entry.
   * <p>The passed in value buffer should be large enough to hold all the values of a multi-value entry.
   */
  int getStringValues(int docId, String[] valueBuffer);

  /**
   * NOTE: The following batch value read APIs should be able to handle the data type conversion. Caller can call any
   *       API regardless of the data type of the {@code BlockValSet}.
   * TODO: Consider letting the caller handle the data type conversion because for different use cases, we might need to
   *       convert data type differently.
   */

  /**
   * Batch reads the dictionary ids at the given document ids of the given length into the dictionary id buffer.
   * <p>The passed in dictionary id buffer size should be larger than or equal to the length.
   */
  void getDictionaryIds(int[] docIds, int length, int[] dictIdBuffer);

  /**
   * Batch reads the INT type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  void getIntValues(int[] docIds, int length, int[] valueBuffer);

  /**
   * Batch reads the LONG type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  void getLongValues(int[] docIds, int length, long[] valueBuffer);

  /**
   * Batch reads the FLOAT type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  void getFloatValues(int[] docIds, int length, float[] valueBuffer);

  /**
   * Batch reads the DOUBLE type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  void getDoubleValues(int[] docIds, int length, double[] valueBuffer);

  /**
   * Batch reads the STRING type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  void getStringValues(int[] docIds, int length, String[] valueBuffer);

  /**
   * Batch reads the BYTES type single-values at the given document ids of the given length into the value buffer.
   * <p>The passed in value buffer size should be larger than or equal to the length.
   */
  void getBytesValues(int[] docIds, int length, byte[][] valueBuffer);

  /**
   * SINGLE-VALUED COLUMN APIs
   */

  /**
   * Returns the dictionary Ids for a single-valued column.
   *
   * @return Array of dictionary Ids
   */
  int[] getDictionaryIdsSV();

  /**
   * Returns the int values for a single-valued column.
   *
   * @return Array of int values
   */
  int[] getIntValuesSV();

  /**
   * Returns the long values for a single-valued column.
   *
   * @return Array of long values
   */
  long[] getLongValuesSV();

  /**
   * Returns the float values for a single-valued column.
   *
   * @return Array of float values
   */
  float[] getFloatValuesSV();

  /**
   * Returns the double values for a single-valued column.
   *
   * @return Array of double values
   */
  double[] getDoubleValuesSV();

  /**
   * Returns the string values for a single-valued column.
   *
   * @return Array of string values
   */
  String[] getStringValuesSV();

  /**
   * Returns the byte[] values for a single-valued column.
   *
   * @return Array of string values
   */
  byte[][] getBytesValuesSV();

  /**
   * MULTI-VALUED COLUMN APIs
   */

  /**
   * Returns the dictionary Ids for a multi-valued column.
   *
   * @return Array of dictionary Ids
   */
  int[][] getDictionaryIdsMV();

  /**
   * Returns the int values for a multi-valued column.
   *
   * @return Array of int values
   */
  int[][] getIntValuesMV();

  /**
   * Returns the long values for a multi-valued column.
   *
   * @return Array of long values
   */
  long[][] getLongValuesMV();

  /**
   * Returns the float values for a multi-valued column.
   *
   * @return Array of float values
   */
  float[][] getFloatValuesMV();

  /**
   * Returns the double values for a multi-valued column.
   *
   * @return Array of double values
   */
  double[][] getDoubleValuesMV();

  /**
   * Returns the string values for a multi-valued column.
   *
   * @return Array of string values
   */
  String[][] getStringValuesMV();

  /**
   * Returns the number of MV entries for a multi-valued column.
   *
   * @return Array of number of MV entries
   */
  int[] getNumMVEntries();
}
