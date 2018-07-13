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
package com.linkedin.pinot.core.io.reader;

public interface SingleColumnSingleValueReader<T extends ReaderContext> extends DataFileReader<T> {

  /**
   * Fetch the char at a row.
   *
   * @param row Row for which to fetch the value.
   * @return Char value at row.
   */
  char getChar(int row);

  /**
   * Fetch short value at the specified row.
   *
   * @param row  Row for which to fetch the value.
   * @return Short value at row.
   */
  short getShort(int row);

  /**
   * Fetch the int value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @return int value at row.
   */
  int getInt(int row);

  /**
   * Fetch the int value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @param context Reader context.
   * @return int value at row.
   */
  int getInt(int row, T context);

  /**
   * Fetch the long value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @return value value at row.
   */
  long getLong(int row);

  /**
   * Fetch the long value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @param context Reader context.
   * @return long value at row.
   */
  long getLong(int row, T context);

  /**
   * Fetch the float value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @return float value at row.
   */
  float getFloat(int row);

  /**
   * Fetch the float value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @param context Reader context.
   * @return float value at row.
   */
  float getFloat(int row, T context);

  /**
   * Fetch the double value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @return double value at row.
   */
  double getDouble(int row);

  /**
   * Fetch the double value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @param context Reader context.
   * @return double value at row.
   */
  double getDouble(int row, T context);

  /**
   * Fetch the String value at the specified row.
   *
   * @param row Row for which to fetch the value.
   * @return String value at row.
   */
  String getString(int row);

  /**
   * Fetch String value for the given row.
   *
   * @param row Row for which to get the string.
   * @param context Reader context.
   * @return String at row.
   */
  String getString(int row, T context);

  /**
   * Fetch the byte[] value for the given row.
   *
   * @param row Row for which to get the byte[].
   * @return byte[] value at row.
   */
  byte[] getBytes(int row);

  /**
   * Fetch the byte[] value for the given row.
   *
   * @param row Row for which to get the byte[].
   * @param context Reader context.
   * @return byte[] byte[] at the given row
   */
  byte[] getBytes(int row, T context);

  /**
   * Read a specified number of values from a given start offset.
   *
   * @param rows Array containing rows id's to read.
   * @param rowStartPos Start offset in 'rows' array.
   * @param rowSize Number of elements to read from 'rows'.
   * @param values Output array
   * @param valuesStartPos Start offset of 'values' array to write the values.
   */
  void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos);
}
