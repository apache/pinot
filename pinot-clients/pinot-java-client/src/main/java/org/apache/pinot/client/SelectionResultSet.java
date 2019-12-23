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
 * Selection result set, which contains the results of a selection query.
 */
class SelectionResultSet extends AbstractResultSet {
  private JsonNode _resultsArray;
  private JsonNode _columnsArray;

  public SelectionResultSet(JsonNode selectionResults) {
    _resultsArray = selectionResults.get("results");
    _columnsArray = selectionResults.get("columns");
  }

  @Override
  public int getRowCount() {
    return _resultsArray.size();
  }

  @Override
  public int getColumnCount() {
    return _columnsArray.size();
  }

  @Override
  public String getColumnName(int columnIndex) {
    return _columnsArray.get(columnIndex).asText();
  }

  @Override
  public String getString(int rowIndex, int columnIndex) {
    JsonNode jsonValue = _resultsArray.get(rowIndex).get(columnIndex);
    if (jsonValue.isTextual()) {
      return jsonValue.textValue();
    } else {
      return jsonValue.toString();
    }
  }

  @Override
  public int getGroupKeyLength() {
    return 0;
  }

  @Override
  public String getGroupKeyString(int rowIndex, int groupKeyColumnIndex) {
    throw new AssertionError("No group key string for selection results");
  }

  @Override
  public String getGroupKeyColumnName(int groupKeyColumnIndex) {
    throw new AssertionError("No group key column name for selection results");
  }

  @Override
  public String toString() {
    int numColumns = getColumnCount();
    TextTable table = new TextTable();
    String[] columnNames = new String[numColumns];
    for (int c = 0; c < numColumns; c++) {
      columnNames[c] = _columnsArray.get(c).asText();
    }
    table.addHeader(columnNames);

    int numRows = getRowCount();
    for (int r = 0; r < numRows; r++) {
      String[] columnValues = new String[numColumns];
      for (int c = 0; c < numColumns; c++) {
        try {
          columnValues[c] = getString(r, c);
        } catch (Exception e) {
          columnNames[c] = "ERROR";
        }
      }
      table.addRow(columnValues);
    }
    return table.toString();
  }
}
