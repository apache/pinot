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
   * Returns the dictionary for the column if one exists, or {@code null} otherwise. The dictionary may live on disk
   * (segment-backed columns) or be built on the fly (transform functions). It may be present even when
   * {@link #isDictionaryEncoded()} returns {@code false} — a column declared as {@code EncodingType.RAW} with an
   * explicit {@code dictionaryIndex} carries a dictionary on disk but a RAW forward index, and a column with a
   * disabled forward index has no way to read dict IDs at all. Callers that select between a dictionary-id read
   * path ({@link #getDictionaryIdsSV()} / {@link #getDictionaryIdsMV()}) and a value read path MUST gate on
   * {@link #isDictionaryEncoded()}, not {@code getDictionary() != null}.
   */
  @Nullable
  Dictionary getDictionary();

  /**
   * Returns {@code true} if the dict-id read path ({@link #getDictionaryIdsSV()} / {@link #getDictionaryIdsMV()})
   * is callable on this value set.
   *
   * <p>The default implementation falls back to {@code getDictionary() != null}, which is correct for value sets
   * where dictionary presence and dict-id readability are coupled. Implementers MUST override this whenever the
   * two can diverge — most notably the segment projection layer, where a column can declare
   * {@code EncodingType.RAW} alongside an explicit {@code dictionaryIndex} (dictionary present, but
   * {@code readDictIds} throws), or where the forward index is disabled outright (no forward index to read).
   */
  default boolean isDictionaryEncoded() {
    return getDictionary() != null;
  }

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

  default int[] get32BitsMurmur3HashValuesSV() {
    throw new UnsupportedOperationException();
  }

  default long[] get64BitsMurmur3HashValuesSV() {
    throw new UnsupportedOperationException();
  }

  default long[][] get128BitsMurmur3HashValuesSV() {
    throw new UnsupportedOperationException();
  }

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
   * Returns the BigDecimal values for a multi-valued column.
   *
   * @return Array of BigDecimal values
   */
  BigDecimal[][] getBigDecimalValuesMV();

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
