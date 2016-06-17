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

public interface MultiColumnSingleValueReader extends DataFileReader{
  /**
   * fetch the char at a row,col
   *
   * @param row
   * @param col
   * @return
   */
  char getChar(int row, int col);

  /**
   * fetch short value at a specific row, col
   *
   * @param row
   * @param col
   * @return
   */
  short getShort(int row, int col);

  /**
   *
   * @param row
   * @param col
   * @return
   */
  int getInt(int row, int col);

  /**
   *
   * @param row
   * @param col
   * @return
   */
  long getLong(int row, int col);

  /**
   *
   * @param row
   * @param col
   * @return
   */
  float getFloat(int row, int col);

  /**
   *
   * @param row
   * @param col
   * @return
   */
  double getDouble(int row, int col);

  /**
   *
   * @param row
   * @param col
   * @return
   */
  String getString(int row, int col);

  /**
   *
   * @param row
   * @param col
   * @return
   */
  byte[] getBytes(int row, int col);

}
