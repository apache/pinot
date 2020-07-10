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
import java.util.ArrayList;
import java.util.List;


/**
 * ResultSet which contains the ResultTable from the broker response of a sql query.
 */
class ResultTableResultSet extends AbstractResultSet {
  private final JsonNode _rowsArray;
  private final JsonNode _columnNamesArray;
  private final JsonNode _columnDataTypesArray;

  public ResultTableResultSet(JsonNode resultTable) {
    _rowsArray = resultTable.get("rows");
    JsonNode dataSchema = resultTable.get("dataSchema");
    _columnNamesArray = dataSchema.get("columnNames");
    _columnDataTypesArray = dataSchema.get("columnDataTypes");
  }

  @Override
  public int getRowCount() {
    return _rowsArray.size();
  }

  @Override
  public int getColumnCount() {
    return _columnNamesArray.size();
  }

  @Override
  public String getColumnName(int columnIndex) {
    return _columnNamesArray.get(columnIndex).asText();
  }

  @Override
  public String getColumnDataType(int columnIndex) {
    return _columnDataTypesArray.get(columnIndex).asText();
  }

  @Override
  public String getString(int rowIndex, int columnIndex) {
    JsonNode jsonValue = _rowsArray.get(rowIndex).get(columnIndex);
    if (jsonValue.isTextual()) {
      return jsonValue.textValue();
    } else {
      return jsonValue.toString();
    }
  }

  public List<String> getAllColumns() {
    List<String> columns = new ArrayList<>();
    if (_columnNamesArray == null) {
      return columns;
    }

    for (JsonNode column : _columnNamesArray) {
      columns.add(column.textValue());
    }

    return columns;
  }


  public List<String> getAllColumnsDataTypes() {
    List<String> columnDataTypes = new ArrayList<>();
    if (_columnDataTypesArray == null) {
      return columnDataTypes;
    }

    for (JsonNode columnDataType : _columnDataTypesArray) {
      columnDataTypes.add(columnDataType.textValue());
    }

    return columnDataTypes;
  }

  @Override
  public int getGroupKeyLength() {
    return 0;
  }

  @Override
  public String getGroupKeyString(int rowIndex, int groupKeyColumnIndex) {
    throw new AssertionError("No group key string for result table");
  }

  @Override
  public String getGroupKeyColumnName(int groupKeyColumnIndex) {
    throw new AssertionError("No group key column name for result table");
  }

  @Override
  public String toString() {
    int numColumns = getColumnCount();
    TextTable table = new TextTable();
    String[] columnNames = new String[numColumns];
    String[] columnDataTypes = new String[numColumns];
    for (int c = 0; c < numColumns; c++) {
      columnNames[c] = _columnNamesArray.get(c).asText();
      columnDataTypes[c] = _columnDataTypesArray.get(c).asText();
    }
    table.addHeader(columnNames);
    table.addHeader(columnDataTypes);

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
