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

public interface RawValueBasedInvertedIndexCreator extends InvertedIndexCreator {

  /**
   * For single-value column, adds the int value for the next document.
   */
  void add(int value);

  /**
   * For multi-value column, adds the int values for the next document.
   */
  void add(int[] values, int length);

  /**
   * For single-value column, adds the long value for the next document.
   */
  void add(long value);

  /**
   * For multi-value column, adds the long values for the next document.
   */
  void add(long[] values, int length);

  /**
   * For single-value column, adds the float value for the next document.
   */
  void add(float value);

  /**
   * For multi-value column, adds the float values for the next document.
   */
  void add(float[] values, int length);

  /**
   * For single-value column, adds the double value for the next document.
   */
  void add(double value);

  /**
   * For multi-value column, adds the double values for the next document.
   */
  void add(double[] values, int length);

  /**
   * For single-value column, adds the String value for the next document.
   *
   * @param value String value to add for the next document
   */
  default void add(String value) { }

  /**
   * For multi-value column, adds the String values for the next document.
   *
   * @param value Array of String values to add for the next document
   * @param length Number of values in the array
   */
  default void add(String[] value, int length) { }

  /**
   * For single-value column, adds the byte array value for the next document.
   *
   * @param value Byte array value to add for the next document
   */
  default void add(byte[] value) { }

  /**
   * For multi-value column, adds the byte array values for the next document.
   *
   * @param value Array of byte array values to add for the next document
   * @param length Number of values in the array
   */
  default void add(byte[][] value, int length) { }
}
