/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.io.reader;

/**
 * Interface for reader for Single Value and Multiple Columns.
 *
 * @param <T>
 */
public interface SingleValueMultiColumnReader<T extends ReaderContext> extends DataFileReader<T> {

  /**
   * Get the char at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @return Char value at (row, column)
   */
  char getChar(int row, int column);

  /**
   * Get the short at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @return Short value at (row, column)
   */
  short getShort(int row, int column);

  /**
   * Get the int at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @return int value at (row, column)
   */
  int getInt(int row, int column);

  /**
   * Get the int at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @param context Reader context
   * @return int value at (row, column)
   */
  int getInt(int row, int column, T context);

  /**
   * Get the long at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @return long value at (row, column)
   */
  long getLong(int row, int column);

  /**
   * Get the long at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @param context Reader context
   *
   * @return long value at (row, column)
   */
  long getLong(int row, int column, T context);

  /**
   * Get the float at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @return float value at (row, column)
   */
  float getFloat(int row, int column);

  /**
   * Get the float at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @param context Reader context
   *
   * @return float value at (row, column)
   */
  float getFloat(int row, int column, T context);

  /**
   * Get the double at a given (row, column).
   *
   * @param row row id
   * @param column column id
   *
   * @return double value at (row, column)
   */
  double getDouble(int row, int column);

  /**
   * Get the double at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @param context Reader context
   *
   * @return double value at (row, column)
   */
  double getDouble(int row, int column, T context);

  /**
   * Get the String at a given (row, column).
   *
   * @param row row id
   * @param column column id
   * @return String value at (row, column)
   */
  String getString(int row, int column);

  /**
   * Get the String at a given (row, column).
   *
   * @param row row id
   * @param column column id
   *
   * @param context Reader context
   *
   * @return String value at (row, column)
   */
  String getString(int row, int column, T context);

  /**
   * Get the byte at a given (row, column).
   *
   * @param row row id
   * @param column column id
   *
   * @return byte value at (row, column)
   */
  byte[] getBytes(int row, int column);
}
