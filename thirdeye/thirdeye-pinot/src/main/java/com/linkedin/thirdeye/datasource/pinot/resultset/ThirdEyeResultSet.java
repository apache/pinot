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

package com.linkedin.thirdeye.datasource.pinot.resultset;

/**
 * An interface to mimic {@link com.linkedin.pinot.client.ResultSet}. Note that this class is used to decouple
 * ThirdEye's data structure from Pinot's. During the initial migration, this class provides a similar interface for
 * backward compatibility.
 *
 * TODO: Refine this class and promote it to a standard container that store the data from any database.
 */
public interface ThirdEyeResultSet {
  int getRowCount();

  int getColumnCount();

  String getColumnName(int columnIdx);

  long getLong(int rowIdx);

  double getDouble(int rowIdx);

  String getString(int rowIdx);

  long getLong(int rowIdx, int columnIdx);

  double getDouble(int rowIdx, int columnIdx);

  String getString(int rowIdx, int columnIdx);

  int getGroupKeyLength();

  String getGroupKeyColumnName(int columnIdx);

  String getGroupKeyColumnValue(int rowIdx, int columnIdx);
}
