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

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.Vector;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code BlockValSet} contains a block of values for a column (Projection layer) or a transform expression
 * (Transform layer).
 */
public interface BlockValSet {

  /**
   * Returns the null value bitmap in the value set.
   */
  @Nullable
  RoaringBitmap getNullBitmap();

  /**
   * Returns the data type of the values in the value set.
   */
  DataType getValueType();

  /**
   * Returns {@code true} if the value set is for a single-value column, {@code false} otherwise.
   */
  boolean isSingleValue();

  /**
   * Returns the dictionary for the column, or {@code null} if the column is not dictionary-encoded.
   */
  @Nullable
  Dictionary getDictionary();

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
   * Returns the BigDecimal values for a single-valued column.
   *
   * @return Array of BigDecimal values
   */
  BigDecimal[] getBigDecimalValuesSV();

  /**
   * Returns the Vector values for a single-valued column.
   *
   * @return Array of vector values
   */
  Vector[] getVectorValuesSV();

  /**
   * Returns the string values for a single-valued column.
   *
   * @return Array of string values
   */
  String[] getStringValuesSV();

  /**
   * Returns the byte[] values for a single-valued column.
   *
   * @return Array of byte[] values
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
   * Returns the byte[] values for a multi-valued column.
   *
   * @return Array of byte[] values
   */
  byte[][][] getBytesValuesMV();

  /**
   * Returns the number of MV entries for a multi-valued column.
   *
   * @return Array of number of MV entries
   */
  int[] getNumMVEntries();
}
