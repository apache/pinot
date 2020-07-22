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
package org.apache.pinot.client;

/**
 * A Pinot result group, representing an aggregation function in the original query.
 */
public interface ResultSet {
  /**
   * Returns the number of rows in this result set.
   *
   * @return The number of rows in this result set
   */
  int getRowCount();

  /**
   * Returns the number of columns in this result set.
   *
   * @return The number of columns in this result set
   */
  int getColumnCount();

  /**
   * Returns the column name at a given index.
   *
   * @param columnIndex The index of the column for which to retrieve the name
   * @return The name of the column at the given column index
   */
  String getColumnName(int columnIndex);

  /**
   * Returns the column type at a given index.
   *
   * @param columnIndex The index of the column for which to retrieve the name
   * @return The data type of the column at the given column index. null if data type is not supported
   */
  String getColumnDataType(int columnIndex);

  /**
   * Obtains the integer value for the given row.
   *
   * @param rowIndex The index of the row
   * @return The integer value for the given row
   */
  int getInt(int rowIndex);

  /**
   * Obtains the long value for the given row.
   *
   * @param rowIndex The index of the row
   * @return The long value for the given row
   */
  long getLong(int rowIndex);

  /**
   * Obtains the float value for the given row.
   *
   * @param rowIndex The index of the row
   * @return The float value for the given row
   */
  float getFloat(int rowIndex);

  /**
   * Obtains the double value for the given row.
   *
   * @param rowIndex The index of the row
   * @return The double value for the given row
   */
  double getDouble(int rowIndex);

  /**
   * Obtains the String value for the given row.
   *
   * @param rowIndex The index of the row
   * @return The String value for the given row
   */
  String getString(int rowIndex);

  /**
   * Obtains the integer value for the given row and column.
   *
   * @param rowIndex The index of the row
   * @param columnIndex The index of the column for which to fetch the value
   * @return The integer value for the given row and column
   */
  int getInt(int rowIndex, int columnIndex);

  /**
   * Obtains the long value for the given row and column.
   *
   * @param rowIndex The index of the row
   * @param columnIndex The index of the column for which to fetch the value
   * @return The long value for the given row and column
   */
  long getLong(int rowIndex, int columnIndex);

  /**
   * Obtains the float value for the given row and column.
   *
   * @param rowIndex The index of the row
   * @param columnIndex The index of the column for which to fetch the value
   * @return The float value for the given row and column
   */
  float getFloat(int rowIndex, int columnIndex);

  /**
   * Obtains the double value for the given row and column.
   *
   * @param rowIndex The index of the row
   * @param columnIndex The index of the column for which to fetch the value
   * @return The double value for the given row and column
   */
  double getDouble(int rowIndex, int columnIndex);

  /**
   * Obtains the String value for the given row and column.
   *
   * @param rowIndex The index of the row
   * @param columnIndex The index of the column for which to fetch the value
   * @return The String value for the given row and column
   */
  String getString(int rowIndex, int columnIndex);

  /**
   * Obtains the length of the group key, or 0 if there is no grouping key.
   *
   * @return The length of the group key, or 0 if there is no grouping key.
   */
  int getGroupKeyLength();

  /**
   * Get the group key name for the given key column index. use getGroupKeyLength() to know the number of groupKey
   * @param groupKeyColumnIndex
   * @return group key column name
   */
  String getGroupKeyColumnName(int groupKeyColumnIndex);

  /**
   * Obtains the group key value for the given row and key column index.
   *
   * @param rowIndex The index of the row
   * @param groupKeyColumnIndex The group key column's index
   * @return The group key value, as an integer
   */
  int getGroupKeyInt(int rowIndex, int groupKeyColumnIndex);

  /**
   * Obtains the group key value for the given row and key column index.
   *
   * @param rowIndex The index of the row
   * @param groupKeyColumnIndex The group key column's index
   * @return The group key value, as a long
   */
  long getGroupKeyLong(int rowIndex, int groupKeyColumnIndex);

  /**
   * Obtains the group key value for the given row and key column index.
   *
   * @param rowIndex The index of the row
   * @param groupKeyColumnIndex The group key column's index
   * @return The group key value, as a float
   */
  float getGroupKeyFloat(int rowIndex, int groupKeyColumnIndex);

  /**
   * Obtains the group key value for the given row and key column index.
   *
   * @param rowIndex The index of the row
   * @param groupKeyColumnIndex The group key column's index
   * @return The group key value, as a double
   */
  double getGroupKeyDouble(int rowIndex, int groupKeyColumnIndex);

  /**
   * Obtains the group key value for the given row and key column index.
   *
   * @param rowIndex The index of the row
   * @param groupKeyColumnIndex The group key column's index
   * @return The group key value, as a String
   */
  String getGroupKeyString(int rowIndex, int groupKeyColumnIndex);
}
