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
package org.apache.pinot.core.io.reader;

import java.util.Map;

/**
 * Interface for reader for Single Valued Map
 */
public interface MapSingleValueReader<T extends ReaderContext> extends DataFileReader<T> {

  /**
   * @param rowId rowNumber
   * @param keys array that will contain the keys after running this method
   * @param values array that will contain the values after running this method
   * @return number of entries in the map, use this to iterate over keys and values array
   */
  int getIntIntMap(int rowId, int[] keys, int[] values);

  /**
   * @param rowId rowNumber
   * @param keys array that will contain the keys after running this method
   * @param values array that will contain the values after running this method
   * @return number of entries in the map, use this to iterate over keys and values array
   */
  int getStringIntMap(int rowId, String[] keys, int[] values);

  /**
   * @param rowId rowNumber
   * @param keys array that will contain the keys after running this method
   * @param values array that will contain the values after running this method
   * @return number of entries in the map, use this to iterate over keys and values array
   */
  int getStringLongMap(int rowId, String[] keys, long[] values);

  /**
   * @param rowId rowNumber
   * @param keys array that will contain the keys after running this method
   * @param values array that will contain the values after running this method
   * @return number of entries in the map, use this to iterate over keys and values array
   */
  int getStringFloatMap(int rowId, String[] keys, float[] values);

  /**
   * @param rowId rowNumber
   * @param keys array that will contain the keys after running this method
   * @param values array that will contain the values after running this method
   * @return number of entries in the map, use this to iterate over keys and values array
   */
  int getStringDoubleMap(int rowId, String[] keys, double[] values);

  /**
   * @param rowId rowNumber
   * @param keys array that will contain the keys after running this method
   * @param values array that will contain the values after running this method
   * @return number of entries in the map, use this to iterate over keys and values array
   */
  int getStringStringMap(int rowId, String[] keys, String[] values);

  /**
   * Get keys for the rowId
   */
  int getIntKeySet(int rowId, int[] keys);

  /**
   * Get keys for the rowId
   */
  int getStringKeySet(int rowId, String[] keys);

  /**
   * Get the int value for a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return int value for row[keyDictId]
   */

  int getIntValue(int row, int key);

  /**
   * Get the int value for a given (row, key).
   *
   * @param row row id
   * @param key key
   * @param context Reader context
   * @return float value at (row, key)
   */
  int getIntValue(int row, int key, T context);

  /**
   * Get the char value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return Char value at (row, key)
   */
  char getCharValue(int row, String key);

  /**
   * Get the short value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return Short value at (row, key)
   */
  short getShortValue(int row, String key);

  /**
   * Get the int value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return int value at (row, key)
   */
  int getIntValue(int row, String key);

  /**
   * Get the int value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @param context Reader context
   * @return int value at (row, key)
   */
  int getIntValue(int row, String key, T context);

  /**
   * Get the long value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return long value at (row, key)
   */
  long getLongValue(int row, String key);

  /**
   * Get the long value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @param context Reader context
   * @return long value at (row, key)
   */
  long getLongValue(int row, String key, T context);

  /**
   * Get the float value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return float value at (row, key)
   */
  float getFloatValue(int row, String key);

  /**
   * Get the float value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @param context Reader context
   * @return float value at (row, key)
   */
  float getFloatValue(int row, String key, T context);

  /**
   * Get the double value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return double value at (row, key)
   */
  double getDoubleValue(int row, String key);

  /**
   * Get the double value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @param context Reader context
   * @return double value at (row, key)
   */
  double getDoubleValue(int row, String key, T context);

  /**
   * Get the String value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return String value at (row, key)
   */
  String getStringValue(int row, String key);

  /**
   * Get the String value at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @param context Reader context
   * @return String value at (row, key)
   */
  String getStringValue(int row, String key, T context);

  /**
   * Get the byte at a given (row, key).
   *
   * @param row row id
   * @param key key
   * @return byte value at (row, key)
   */
  byte[] getBytesValue(int row, String key);
}
