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
 * Shared implementation between the different ResultSets.
 */
abstract class AbstractResultSet implements ResultSet {

  @Override
  public String getColumnDataType(int columnIndex) {
    return null;
  }

  @Override
  public int getInt(int rowIndex) {
    return getInt(rowIndex, 0);
  }

  @Override
  public long getLong(int rowIndex) {
    return getLong(rowIndex, 0);
  }

  @Override
  public float getFloat(int rowIndex) {
    return getFloat(rowIndex, 0);
  }

  @Override
  public double getDouble(int rowIndex) {
    return getDouble(rowIndex, 0);
  }

  @Override
  public String getString(int rowIndex) {
    return getString(rowIndex, 0);
  }

  @Override
  public int getInt(int rowIndex, int columnIndex) {
    return Integer.parseInt(getString(rowIndex, columnIndex));
  }

  @Override
  public long getLong(int rowIndex, int columnIndex) {
    return Long.parseLong(getString(rowIndex, columnIndex));
  }

  @Override
  public float getFloat(int rowIndex, int columnIndex) {
    return Float.parseFloat(getString(rowIndex, columnIndex));
  }

  @Override
  public double getDouble(int rowIndex, int columnIndex) {
    return Double.parseDouble(getString(rowIndex, columnIndex));
  }

  @Override
  public int getGroupKeyInt(int rowIndex, int groupKeyColumnIndex) {
    return Integer.parseInt(getGroupKeyString(rowIndex, groupKeyColumnIndex));
  }

  @Override
  public long getGroupKeyLong(int rowIndex, int groupKeyColumnIndex) {
    return Long.parseLong(getGroupKeyString(rowIndex, groupKeyColumnIndex));
  }

  @Override
  public float getGroupKeyFloat(int rowIndex, int groupKeyColumnIndex) {
    return Float.parseFloat(getGroupKeyString(rowIndex, groupKeyColumnIndex));
  }

  @Override
  public double getGroupKeyDouble(int rowIndex, int groupKeyColumnIndex) {
    return Double.parseDouble(getGroupKeyString(rowIndex, groupKeyColumnIndex));
  }
}
