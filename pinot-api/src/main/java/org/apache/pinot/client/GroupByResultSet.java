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

import com.fasterxml.jackson.databind.JsonNode;


/**
 * A Pinot query result set for group by results, of which there is one of per aggregation function
 * in the query.
 */
class GroupByResultSet extends AbstractResultSet {
  private final JsonNode _groupByResults;
  private final JsonNode _groupByColumns;
  private final String _functionName;

  public GroupByResultSet(JsonNode jsonObject) {
    _groupByResults = jsonObject.get("groupByResult");
    _groupByColumns = jsonObject.get("groupByColumns");
    _functionName = jsonObject.get("function").asText();
  }

  /**
   * Returns the number of rows in this result group.
   * @return The number of rows in this result group
   */
  @Override
  public int getRowCount() {
    return _groupByResults.size();
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
      throw new IllegalArgumentException("Column index must always be 0 for aggregation result sets");
    }
    return _groupByResults.get(rowIndex).get("value").asText();
  }

  @Override
  public int getGroupKeyLength() {
    return _groupByColumns.size();
  }

  @Override
  public String getGroupKeyString(int rowIndex, int groupKeyColumnIndex) {
    return _groupByResults.get(rowIndex).get("group").get(groupKeyColumnIndex).asText();
  }

  @Override
  public String getGroupKeyColumnName(int groupKeyColumnIndex) {
    return _groupByColumns.get(groupKeyColumnIndex).asText();
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
