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

import it.unimi.dsi.fastutil.ints.IntSet;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Interface for the dictionary. For the read APIs, type conversion among INT, LONG, FLOAT, DOUBLE, STRING should be
 * supported. Type conversion between STRING and BYTES via Hex encoding/decoding should be supported.
 */
@SuppressWarnings("rawtypes")
public interface Dictionary extends IndexReader {
  int NULL_VALUE_INDEX = -1;

  /**
   * Returns {@code true} if the values in the dictionary are sorted, {@code false} otherwise.
   */
  boolean isSorted();

  /**
   * Returns the data type of the values in the dictionary.
   */
  DataType getValueType();

  /**
   * Returns the number of values in the dictionary.
   */
  int length();

  /**
   * Returns the index of the string representation of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if
   * the value does not exist. This method is for the cross-type predicate evaluation.
   */
  int indexOf(String stringValue);

  /**
   * Returns the index of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if the value does not exist.
   * Must be implemented for INT dictionaries.
   */
  default int indexOf(int intValue) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the index of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if the value does not exist.
   * Must be implemented for LONG dictionaries.
   */
  default int indexOf(long longValue) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the index of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if the value does not exist.
   * Must be implemented for FLOAT dictionaries.
   */
  default int indexOf(float floatValue) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the index of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if the value does not exist.
   * Must be implemented for DOUBLE dictionaries.
   */
  default int indexOf(double doubleValue) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the index of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if the value does not exist.
   * Must be implemented for BIG_DECIMAL dictionaries.
   */
  default int indexOf(BigDecimal bigDecimalValue) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the index of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if the value does not exist.
   * Must be implemented for BYTE_ARRAY dictionaries.
   */
  default int indexOf(ByteArray bytesValue) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the insertion index of the string representation of the value in the dictionary. This method follows the
   * same behavior as in {@link Arrays#binarySearch(Object[], Object)}. All sorted dictionaries should support this
   * method. This method is for the range predicate evaluation.
   */
  int insertionIndexOf(String stringValue);

  /**
   * Returns a set of dictIds in the given value range, where lower/upper bound can be "*" which indicates unbounded
   * range. All unsorted dictionaries should support this method. This method is for the range predicate evaluation.
   */
  IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper);

  /**
   * Returns the comparison result of the values (actual value instead of string representation of the value) for the
   * given dictionary ids, i.e. {@code value1.compareTo(value2)}.
   */
  int compare(int dictId1, int dictId2);

  /**
   * Returns the minimum value in the dictionary. For type BYTES, {@code ByteArray} will be returned. Undefined if the
   * dictionary is empty.
   */
  Comparable getMinVal();

  /**
   * Returns the maximum value in the dictionary. For type BYTES, {@code ByteArray} will be returned. Undefined if the
   * dictionary is empty.
   */
  Comparable getMaxVal();

  /**
   * Returns a sorted array of all values in the dictionary. For type INT/LONG/FLOAT/DOUBLE, primitive type array will
   * be returned; for type BIG_DECIMAL, {@code BigDecimal[]} will be returned; for type STRING, {@code String[]} will be
   * returned; for type BYTES, {@code ByteArray[]} will be returned.
   * This method is for the stats collection phase when sealing the consuming segment.
   */
  Object getSortedValues();

  // Single-value read APIs

  /**
   * Returns the value at the given dictId in the dictionary.
   * <p>The Object type returned for each value type:
   * <ul>
   *   <li>INT -> Integer</li>
   *   <li>LONG -> Long</li>
   *   <li>FLOAT -> Float</li>
   *   <li>DOUBLE -> Double</li>
   *   <li>BIG_DECIMAL -> BigDecimal</li>
   *   <li>STRING -> String</li>
   *   <li>BYTES -> byte[]</li>
   * </ul>
   */
  Object get(int dictId);

  /**
   * Returns the value at the given dictId in the dictionary.
   * <p>The Object type returned for each value type:
   * <ul>
   *   <li>INT -> Integer</li>
   *   <li>LONG -> Long</li>
   *   <li>FLOAT -> Float</li>
   *   <li>DOUBLE -> Double</li>
   *   <li>BIG_DECIMAL -> BigDecimal</li>
   *   <li>STRING -> String</li>
   *   <li>BYTES -> ByteArray</li>
   * </ul>
   */
  default Object getInternal(int dictId) {
    return get(dictId);
  }

  int getIntValue(int dictId);

  long getLongValue(int dictId);

  float getFloatValue(int dictId);

  double getDoubleValue(int dictId);

  BigDecimal getBigDecimalValue(int dictId);

  String getStringValue(int dictId);

  /**
   * NOTE: Should be overridden for STRING, BIG_DECIMAL and BYTES dictionary.
   */
  default byte[] getBytesValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  default ByteArray getByteArrayValue(int dictId) {
    return new ByteArray(getBytesValue(dictId));
  }

  // Batch read APIs

  default void readIntValues(int[] dictIds, int length, int[] outValues) {
    for (int i = 0; i < length; i++) {
      outValues[i] = getIntValue(dictIds[i]);
    }
  }

  default void readLongValues(int[] dictIds, int length, long[] outValues) {
    for (int i = 0; i < length; i++) {
      outValues[i] = getLongValue(dictIds[i]);
    }
  }

  default void readFloatValues(int[] dictIds, int length, float[] outValues) {
    for (int i = 0; i < length; i++) {
      outValues[i] = getFloatValue(dictIds[i]);
    }
  }

  default void readDoubleValues(int[] dictIds, int length, double[] outValues) {
    for (int i = 0; i < length; i++) {
      outValues[i] = getDoubleValue(dictIds[i]);
    }
  }

  default void readBigDecimalValues(int[] dictIds, int length, BigDecimal[] outValues) {
    for (int i = 0; i < length; i++) {
      outValues[i] = getBigDecimalValue(dictIds[i]);
    }
  }

  default void readStringValues(int[] dictIds, int length, String[] outValues) {
    for (int i = 0; i < length; i++) {
      outValues[i] = getStringValue(dictIds[i]);
    }
  }

  default void readBytesValues(int[] dictIds, int length, byte[][] outValues) {
    for (int i = 0; i < length; i++) {
      outValues[i] = getBytesValue(dictIds[i]);
    }
  }

  /**
   * Returns the dictIds for the given sorted values. This method is for the IN/NOT IN predicate evaluation.
   * @param sortedValues
   * @param dictIds
   */
  default void getDictIds(List<String> sortedValues, IntSet dictIds) {
    for (String value : sortedValues) {
      int dictId = indexOf(value);
      if (dictId >= 0) {
        dictIds.add(dictId);
      }
    }
  }
}
