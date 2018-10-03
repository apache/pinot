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

public abstract class AbstractThirdEyeResultSet implements ThirdEyeResultSet {

  public long getLong(int rowIndex) {
    return this.getLong(rowIndex, 0);
  }

  public double getDouble(int rowIndex) {
    return this.getDouble(rowIndex, 0);
  }

  public String getString(int rowIndex) {
    return this.getString(rowIndex, 0);
  }

  public long getLong(int rowIndex, int columnIndex) {
    return Long.parseLong(this.getString(rowIndex, columnIndex));
  }

  public double getDouble(int rowIndex, int columnIndex) {
    return Double.parseDouble(this.getString(rowIndex, columnIndex));
  }
}
