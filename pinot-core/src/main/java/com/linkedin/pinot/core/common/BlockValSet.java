/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec.DataType;


public interface BlockValSet {

  BlockValIterator iterator();

  DataType getValueType();

  /**
   * DOCUMENT ID BASED APIs
   */

  /**
   * Get dictionary Ids for the given docIds.
   *
   * @param inDocIds Input docIds
   * @param inStartPos Start index in inDocIds
   * @param inDocIdsSize Number of input doc ids
   * @param outDictionaryIds Output array
   * @param outStartPos Start position in outDictionaryIds
   */
  void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds, int outStartPos);

  /**
   * Get Integer values for the given docIds.
   *
   * @param inDocIds Input docIds
   * @param inStartPos Start index in inDocIds
   * @param inDocIdsSize Number of input doc ids
   * @param outValues Output array
   * @param outStartPos Start position in outValues
   */
  void getIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outValues, int outStartPos);

  /**
   * Get long values for the given docIds.
   *
   * @param inDocIds Input docIds
   * @param inStartPos Start index in inDocIds
   * @param inDocIdsSize Number of input doc ids
   * @param outValues Output array
   * @param outStartPos Start position in outValues
   */
  void getLongValues(int[] inDocIds, int inStartPos, int inDocIdsSize, long[] outValues, int outStartPos);

  /**
   * Get float values for the given docIds.
   *
   * @param inDocIds Input docIds
   * @param inStartPos Start index in inDocIds
   * @param inDocIdsSize Number of input doc ids
   * @param outValues Output array
   * @param outStartPos Start position in outValues
   */
  void getFloatValues(int[] inDocIds, int inStartPos, int inDocIdsSize, float[] outValues, int outStartPos);

  /**
   *
   * @param inDocIds Input docIds
   * @param inStartPos Start index in inDocIds
   * @param inDocIdsSize Number of input doc ids
   * @param outValues Output array
   * @param outStartPos Start position in outValues
   */
  void getDoubleValues(int[] inDocIds, int inStartPos, int inDocIdsSize, double[] outValues, int outStartPos);

  /**
   * Get string values for the given docIds.
   *
   * @param inDocIds Input docIds
   * @param inStartPos Start index in inDocIds
   * @param inDocIdsSize Number of input doc ids
   * @param outValues Output array
   * @param outStartPos Start position in outValues
   */
  void getStringValues(int[] inDocIds, int inStartPos, int inDocIdsSize, String[] outValues, int outStartPos);

  /**
   * Get byte[] values for the given docIds.
   *
   * @param inDocIds Input docIds
   * @param inStartPos Start index in inDocIds
   * @param inDocIdsSize Number of input doc ids
   * @param outValues Output array
   * @param outStartPos Start position in outValues
   */
  void getBytesValues(int[] inDocIds, int inStartPos, int inDocIdsSize, byte[][] outValues, int outStartPos);

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
