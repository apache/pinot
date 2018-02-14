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
package com.linkedin.pinot.core.io.writer;

/**
 * Interface for writing multiple columns with single values.
 */
public interface SingleValueMultiColumnWriter extends DataFileWriter {

  /**
   * Set the given int value at the specified (row, column).
   *
   * @param row Row id.
   * @param column Column id.
   * @param value Value to be set.
   */
  void setInt(int row, int column, int value);

  /**
   * Set the given long value at the specified (row, column).
   *
   * @param row Row id.
   * @param column Column id.
   * @param value Value to be set.
   */
  void setLong(int row, int column, long value);

  /**
   * Set the given float value at the specified (row, column).
   *
   * @param row Row id.
   * @param column Column id.
   * @param value Value to be set.
   */
  void setFloat(int row, int column, float value);

  /**
   * Set the given double value at the specified (row, column).
   *
   * @param row Row id.
   * @param column Column id.
   * @param value Value to be set.
   */
  void setDouble(int row, int column, double value);

  /**
   * Set the given String value at the specified (row, column).
   *
   * @param row Row id.
   * @param column Column id.
   * @param value Value to be set.
   */
  void setString(int row, int column, String value);
}
