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
package com.linkedin.pinot.client;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A Pinot query result set for group by results, of which there is one of per aggregation function
 * in the query.
 */
class GroupByResultSet extends AbstractResultSet {
  private final JSONArray _groupByResults;
  private final JSONArray _groupByColumns;
  private final String _functionName;

  public GroupByResultSet(JSONObject jsonObject) {
    try {
      _groupByResults = jsonObject.getJSONArray("groupByResult");
      _groupByColumns = jsonObject.getJSONArray("groupByColumns");
      _functionName = jsonObject.getString("function");
    } catch (JSONException e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Returns the number of rows in this result group.
   * @return The number of rows in this result group
   */
  @Override
  public int getRowCount() {
    return _groupByResults.length();
  }

  @Override
  public int getColumnCount() {
    return 1;
  }

  @Override
  public String getColumnName(int columnIndex) {
    return _functionName;
  }

  @Override
  public String getString(int rowIndex, int columnIndex) {
    if (columnIndex != 0) {
      throw new IllegalArgumentException(
          "Column index must always be 0 for aggregation result sets");
    }

    try {
      return _groupByResults.getJSONObject(rowIndex).getString("value");
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public int getGroupKeyLength() {
    return _groupByColumns.length();
  }

  @Override
  public String getGroupKeyString(int rowIndex, int groupKeyColumnIndex) {
    try {
      return _groupByResults.getJSONObject(rowIndex).getJSONArray("group").getString(groupKeyColumnIndex);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public String getGroupKeyColumnName(int groupKeyColumnIndex) {
    try {
      return _groupByColumns.getString(groupKeyColumnIndex);
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public String toString() {
    int groupKeyLength = getGroupKeyLength();
    int numColumns = groupKeyLength + getColumnCount();
    TextTable table = new TextTable();
    String[] columnNames = new String[numColumns];
    for (int c = 0; c < groupKeyLength; c++) {
      try {
        columnNames[c] = getGroupKeyColumnName(c);
      } catch (Exception e) {
        columnNames[c] = "ERROR";
      }
    }
    for (int c = 0; c < getColumnCount(); c++) {
      columnNames[groupKeyLength + c] = getColumnName(c);
    }
    table.addHeader(columnNames);

    int numRows = getRowCount();
    for (int r = 0; r < numRows; r++) {
      String[] columnValues = new String[numColumns];
      for (int c = 0; c < groupKeyLength; c++) {
        try {
          columnValues[c] = getGroupKeyString(r, c);
        } catch (Exception e) {
          columnValues[c] = "ERROR";
        }
      }
      for (int c = 0; c < getColumnCount(); c++) {
        columnValues[groupKeyLength + c] = getString(r, c);
      }
      table.addRow(columnValues);
    }
    return table.toString();
  }
}
