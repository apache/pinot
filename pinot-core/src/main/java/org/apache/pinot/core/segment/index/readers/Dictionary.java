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

import java.io.Closeable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for the dictionary. For the read APIs, type conversion among INT, LONG, FLOAT, DOUBLE, STRING should be
 * supported. Type conversion between STRING and BYTES via Hex encoding/decoding should be supported.
 */
public interface Dictionary extends Closeable {
  int NULL_VALUE_INDEX = -1;

  /**
   * NOTE: Immutable dictionary is always sorted; mutable dictionary is always unsorted.
   */
  boolean isSorted();

  /**
   * Returns the data type of the values in the dictionary.
   */
  DataType getValueType();

  int length();

  /**
   * Returns the index of the string representation of the value in the dictionary, or {@link #NULL_VALUE_INDEX} (-1) if
   * the value does not exist. This API is for cross-type predicate evaluation.
   */
  int indexOf(String stringValue);

  // Single-value read APIs

  /**
   * Returns the value at the given dictId in the dictionary.
   * <p>The Object type returned for each value type:
   * <ul>
   *   <li>INT -> Integer</li>
   *   <li>LONG -> Long</li>
   *   <li>FLOAT -> Float</li>
   *   <li>DOUBLE -> Double</li>
   *   <li>STRING -> String</li>
   *   <li>BYTES -> byte[]</li>
   * </ul>
   */
  Object get(int dictId);

  int getIntValue(int dictId);

  long getLongValue(int dictId);

  float getFloatValue(int dictId);

  double getDoubleValue(int dictId);

  String getStringValue(int dictId);

  byte[] getBytesValue(int dictId);

  // Batch read APIs

  void readIntValues(int[] dictIds, int length, int[] outValues);

  void readLongValues(int[] dictIds, int length, long[] outValues);

  void readFloatValues(int[] dictIds, int length, float[] outValues);

  void readDoubleValues(int[] dictIds, int length, double[] outValues);

  void readStringValues(int[] dictIds, int length, String[] outValues);

  void readBytesValues(int[] dictIds, int length, byte[][] outValues);
}
